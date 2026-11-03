"""Slowly-changing-dimension (Type 2) transforms.

Pure ``DataFrame -> DataFrame`` functions; no session, no I/O. SCD2 keeps the
full history of a dimension: instead of overwriting an attribute, a changed
record **closes** the old version (stamping ``__effective_to``) and **opens** a
new current one. Facts then join on the business key *and the date range*, so a
ticket sold in April resolves to the seat's April attributes even after the seat
changed in May.

Two consumers in this layer:

* ``scd2_merge`` — the general one-batch merge used for ``dim_account`` (CRM
  nightly snapshots). Idempotent: re-applying an unchanged snapshot is a no-op.
* ``build_seat_dimension`` — ``dim_seat`` specifically, where the mid-season
  section renumbering (118-124 -> 218-224) is the SCD2 driver. Earlier orders
  reference the pre-renumber section; the closed version is how they still
  resolve.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EFFECTIVE_FROM = "__effective_from"
EFFECTIVE_TO = "__effective_to"
IS_CURRENT = "__is_current"
HIGH_DATE = "9999-12-31"

# Sentinel for null-safe signature comparison: a genuine NULL and the literal
# string "NULL" must not hash the same, or a real change could be missed.
_NULL_SENTINEL = "␀"


def _signature(cols: tuple[str, ...]):
    """A null-safe hash over the tracked columns, used to detect real changes."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(_NULL_SENTINEL)) for c in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def scd2_merge(
    current: DataFrame | None,
    incoming: DataFrame,
    key: str | tuple[str, ...],
    tracked: tuple[str, ...],
    effective_from: str,
    high_date: str = HIGH_DATE,
) -> DataFrame:
    """Apply one batch of ``incoming`` records to an SCD2 ``current`` dimension.

    Parameters
    ----------
    current
        The existing dimension in SCD2 shape (business columns plus
        ``__effective_from`` / ``__effective_to`` / ``__is_current``). ``None``
        or empty means first load.
    incoming
        A new snapshot/batch of business records. Must carry an
        ``effective_from`` column giving the date each version becomes true;
        every other column is treated as a business column.
    key
        Business key column name(s).
    tracked
        Columns whose change opens a new version. A difference in an untracked
        column is ignored (it updates in place on the next genuine change).
    effective_from
        Name of the column in ``incoming`` holding the version-start date.

    Returns
    -------
    The new dimension state. Four row populations are unioned:

    * already-closed versions — pass through untouched;
    * open versions with no incoming match, or an *identical* match — stay open;
    * open versions whose tracked signature changed — **closed** at the incoming
      effective date;
    * incoming records that are new keys or genuine changes — **opened** current.
    """
    keys = (key,) if isinstance(key, str) else tuple(key)
    high = F.to_date(F.lit(high_date))
    business_cols = [c for c in incoming.columns if c != effective_from]
    dim_cols = [*business_cols, EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT]

    incoming_dim = (
        incoming.withColumn(EFFECTIVE_FROM, F.to_date(F.col(effective_from)))
        .withColumn(EFFECTIVE_TO, high)
        .withColumn(IS_CURRENT, F.lit(True))
        .select(*dim_cols)
    )

    if current is None or len(current.take(1)) == 0:
        return incoming_dim

    open_rows = current.filter(F.col(IS_CURRENT))
    closed_rows = current.filter(~F.col(IS_CURRENT))

    open_sig = open_rows.withColumn("_sig", _signature(tracked))
    incoming_sig = incoming_dim.withColumn("_sig", _signature(tracked))

    # Classify existing open rows against the incoming batch.
    incoming_probe = incoming_sig.select(
        *keys,
        F.col("_sig").alias("_in_sig"),
        F.col(EFFECTIVE_FROM).alias("_in_from"),
    )
    open_tagged = open_sig.join(incoming_probe, list(keys), "left")

    unchanged_open = open_tagged.filter(
        F.col("_in_sig").isNull() | (F.col("_in_sig") == F.col("_sig"))
    ).select(*dim_cols)

    closed_now = (
        open_tagged.filter(F.col("_in_sig").isNotNull() & (F.col("_in_sig") != F.col("_sig")))
        .withColumn(EFFECTIVE_TO, F.col("_in_from"))
        .withColumn(IS_CURRENT, F.lit(False))
        .select(*dim_cols)
    )

    # Classify incoming rows against existing open state.
    open_probe = open_sig.select(*keys, F.col("_sig").alias("_cur_sig"))
    incoming_tagged = incoming_sig.join(open_probe, list(keys), "left")
    new_current = incoming_tagged.filter(
        F.col("_cur_sig").isNull() | (F.col("_cur_sig") != F.col("_sig"))
    ).select(*dim_cols)

    return closed_rows.unionByName(unchanged_open).unionByName(closed_now).unionByName(new_current)


def build_seat_dimension(
    seats: DataFrame,
    season_start: str = "2027-01-01",
    high_date: str = HIGH_DATE,
) -> DataFrame:
    """Build ``dim_seat`` as SCD2 from the seat manifest.

    The manifest encodes the renumbering inline: ``renumbered_to_section`` and
    ``renumber_effective_date`` are set on the affected seats (sections
    118-124). Each such seat becomes **two** versions:

    * the original section, effective from ``season_start`` and **closed** on the
      renumber date (``__is_current = false``);
    * the new section (``renumbered_to_section``), effective from the renumber
      date, **current**.

    Every other seat is a single current version from ``season_start``. The
    ``section`` column carries the value in force for that version, so a fact
    joined on ``seat_id`` *and* purchase date lands on the right section.
    """
    start = F.to_date(F.lit(season_start))
    high = F.to_date(F.lit(high_date))
    renumber_date = F.to_date(F.col("renumber_effective_date"))
    is_renumbered = F.col("renumbered_to_section").isNotNull()

    renumber_meta = ("renumbered_to_section", "renumber_effective_date")
    carried = [c for c in seats.columns if c not in renumber_meta]
    dim_cols = [*carried, EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT]

    # Version 1: original section. Current unless this seat was renumbered. The
    # renumber-metadata columns are still in scope here; the final ``select``
    # projects them away.
    v1 = (
        seats.withColumn(EFFECTIVE_FROM, start)
        .withColumn(EFFECTIVE_TO, F.when(is_renumbered, renumber_date).otherwise(high))
        .withColumn(IS_CURRENT, ~is_renumbered)
        .select(*dim_cols)
    )

    # Version 2: post-renumber section — only for renumbered seats, always current.
    v2 = (
        seats.filter(is_renumbered)
        .withColumn("section", F.col("renumbered_to_section"))
        .withColumn(EFFECTIVE_FROM, renumber_date)
        .withColumn(EFFECTIVE_TO, high)
        .withColumn(IS_CURRENT, F.lit(True))
        .select(*dim_cols)
    )

    return v1.unionByName(v2)


def current_version(dim: DataFrame) -> DataFrame:
    """The current slice of an SCD2 dimension — one row per business key."""
    return dim.filter(F.col(IS_CURRENT))
