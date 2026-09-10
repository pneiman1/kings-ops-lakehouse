"""
Data contracts for the bronze boundary.

A data contract is a written, versioned, enforced statement of what a source
system promises to deliver. It answers three questions that are otherwise
answered by argument during an incident:

  1. Which columns must be present, and of what type?
  2. What identifies a row uniquely?
  3. What happens when the source sends something we did not agree to?

Why contracts live in code rather than in a wiki
------------------------------------------------
A schema written in Confluence is a description. A schema written here is a
control: the pipeline reads it, enforces it, and fails or quarantines against
it. When ticketing adds a column without telling anyone, the contract is what
turns "silently corrupted silver" into "pipeline stopped, here is why."

Why this module imports no PySpark
----------------------------------
Contracts are data about data. Keeping them as plain Python means the entire
contract layer is unit-testable in CI with no cluster, in under a second. The
Spark-facing conversion (``schema_ddl``) emits a DDL *string*, which Spark
accepts anywhere a StructType would go — so we get enforcement without the
import.

Versioning policy
-----------------
``contract_version`` is semantic:

  PATCH  documentation or comment change only
  MINOR  additive, backward-compatible (a new nullable column)
  MAJOR  breaking (column removed, renamed, retyped, or nullability tightened)

A MAJOR bump requires a migration note in docs/adr/ and an explicit review. The
version is stamped onto every bronze row, so any downstream failure can be
traced to the exact contract in force when the row landed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class EvolutionPolicy(str, Enum):
    """What to do when the source sends columns the contract does not know about.

    The choice is not stylistic. It encodes whether an unexpected column is a
    non-event or an incident, and different sources genuinely differ:

    RESCUE
        Capture unknown fields into a rescued-data column and keep going. Correct
        for sources whose shape legitimately drifts — scanner firmware ships a
        new field and nobody tells the data team. Failing here would be an
        outage caused by a non-event.

    ADD_NEW_COLUMNS
        Add the column to the target and continue. Correct for reference data
        where a new attribute is useful and harmless. Note that in Auto Loader
        this restarts the stream once so the new schema takes effect.

    FAIL_ON_NEW_COLUMNS
        Stop. Correct for CDC from an operational database, where a new column
        means the upstream schema changed underneath us. Continuing would write
        subtly wrong data into silver, and subtly wrong is worse than stopped.

    NONE
        Ignore unknown columns entirely. Rarely correct — it discards data
        silently, which is the failure mode contracts exist to prevent.
    """

    RESCUE = "rescue"
    ADD_NEW_COLUMNS = "addNewColumns"
    FAIL_ON_NEW_COLUMNS = "failOnNewColumns"
    NONE = "none"


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    nullable: bool = True
    description: str = ""
    is_pii: bool = False

    # Spark SQL types accepted in a DDL schema string. Validated at construction
    # so a typo fails at import time in CI rather than at runtime in a pipeline.
    VALID_TYPES = (
        "STRING", "INT", "BIGINT", "DOUBLE", "FLOAT", "BOOLEAN",
        "DATE", "TIMESTAMP", "DECIMAL", "BINARY",
    )

    def __post_init__(self) -> None:
        base = self.data_type.split("(")[0].upper()
        if base not in self.VALID_TYPES and not base.startswith(("ARRAY", "STRUCT", "MAP")):
            raise ValueError(f"column '{self.name}': unsupported type '{self.data_type}'")
        if not self.name.replace("_", "").isalnum():
            raise ValueError(f"column '{self.name}': name must be alphanumeric plus underscores")

    def to_ddl(self) -> str:
        suffix = "" if self.nullable else " NOT NULL"
        return f"{self.name} {self.data_type}{suffix}"


@dataclass(frozen=True)
class DataContract:
    """The agreement with one source system."""

    source_name: str
    version: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    evolution_policy: EvolutionPolicy
    # Event-time column, required for any streaming source: without one there is
    # nothing to watermark against and late data cannot be reasoned about.
    event_time_column: str | None = None
    # Columns that must be non-null for a row to be publishable. A row failing
    # this goes to quarantine rather than silently poisoning a join.
    required_columns: tuple[str, ...] = field(default_factory=tuple)
    description: str = ""

    def __post_init__(self) -> None:
        names = [c.name for c in self.columns]
        if len(names) != len(set(names)):
            dupes = sorted({n for n in names if names.count(n) > 1})
            raise ValueError(f"contract '{self.source_name}': duplicate columns {dupes}")
        if not self.primary_key:
            raise ValueError(f"contract '{self.source_name}': primary_key cannot be empty")
        for key in self.primary_key:
            if key not in names:
                raise ValueError(
                    f"contract '{self.source_name}': primary key '{key}' is not a declared column"
                )
        for req in self.required_columns:
            if req not in names:
                raise ValueError(
                    f"contract '{self.source_name}': required column '{req}' is not declared"
                )
        if self.event_time_column and self.event_time_column not in names:
            raise ValueError(
                f"contract '{self.source_name}': event_time_column "
                f"'{self.event_time_column}' is not declared"
            )
        if len(self.version.split(".")) != 3:
            raise ValueError(f"contract '{self.source_name}': version must be MAJOR.MINOR.PATCH")

    # -- derived views ------------------------------------------------------- #

    def schema_ddl(self) -> str:
        """DDL string Spark accepts wherever a StructType is expected.

        Supplying an explicit schema is what makes ``failOnNewColumns`` and
        rescue meaningful. Without it Auto Loader infers, and an inferred schema
        cannot be violated — there is nothing to violate.
        """
        return ", ".join(c.to_ddl() for c in self.columns)

    def pii_columns(self) -> tuple[str, ...]:
        """Columns to tag for masking in M7. Declaring PII at the contract
        boundary rather than discovering it during a governance review is the
        difference between policy and archaeology."""
        return tuple(c.name for c in self.columns if c.is_pii)

    def primary_key_not_null_expectation(self) -> str:
        """Expectation predicate asserting the key is complete.

        A null primary key is not a data quality nuance — it is a row that
        cannot be merged, deduplicated, or joined. It always quarantines.
        """
        return " AND ".join(f"{k} IS NOT NULL" for k in self.primary_key)

    def required_columns_expectation(self) -> str:
        cols = self.required_columns or self.primary_key
        return " AND ".join(f"{c} IS NOT NULL" for c in cols)


# --------------------------------------------------------------------------- #
# Contracts
# --------------------------------------------------------------------------- #

GATE_SCANS = DataContract(
    source_name="gate_scans",
    version="1.1.0",  # 1.1.0 added scan_device_os mid-season (additive, non-breaking)
    description="Turnstile and handheld admission scans, streamed per gate.",
    evolution_policy=EvolutionPolicy.RESCUE,
    primary_key=("scan_id",),
    event_time_column="scan_ts",
    required_columns=("scan_id", "game_id", "gate", "scan_ts"),
    columns=(
        Column("scan_id", "STRING", nullable=False, description="Unique scan event identifier"),
        Column("game_id", "STRING", nullable=False, description="Game this scan belongs to"),
        Column("order_id", "STRING", description="Ticket order; null or unmatched for invalid scans"),
        Column("seat_id", "STRING", description="Seat scanned into"),
        Column("gate", "STRING", nullable=False, description="Physical gate"),
        Column("scan_ts", "TIMESTAMP", nullable=False, description="Event time, not arrival time"),
        Column("device_id", "STRING", description="Scanner hardware identifier"),
        Column("device_type", "STRING", description="TURNSTILE or HANDHELD"),
        Column("scan_result", "STRING", description="ACCEPTED, DUPLICATE, INVALID, UNKNOWN_CREDENTIAL"),
        Column("ticket_medium", "STRING", description="MOBILE, PRINT_AT_HOME, CARD"),
        # Added in 1.1.0. Nullable by necessity: rows from before the firmware
        # rollout will never have it, and declaring it NOT NULL would quarantine
        # half the season for a field that did not exist yet.
        Column("scan_device_os", "STRING", description="Scanner OS; absent before mid-season rollout"),
    ),
)

TICKETING_CDC = DataContract(
    source_name="ticketing_cdc",
    version="1.0.0",
    description="Change data capture from the ticketing operational store.",
    # Opposite policy to gate_scans, deliberately. A new column here means the
    # operational schema changed underneath us, which is an incident.
    evolution_policy=EvolutionPolicy.FAIL_ON_NEW_COLUMNS,
    primary_key=("order_id", "__seq"),
    event_time_column="__commit_ts",
    required_columns=("order_id", "__seq", "__op", "event_type"),
    columns=(
        Column("order_id", "STRING", nullable=False, description="Ticket order identifier"),
        Column("game_id", "STRING", nullable=False),
        Column("seat_id", "STRING", nullable=False),
        Column("account_id", "STRING", nullable=False, description="Purchasing account"),
        Column("seat_class", "STRING"),
        Column("channel", "STRING", description="Sales channel"),
        Column("face_price_usd", "DOUBLE"),
        Column("paid_price_usd", "DOUBLE"),
        Column("purchased_ts", "TIMESTAMP"),
        Column("transferred_ts", "TIMESTAMP"),
        Column("resold_ts", "TIMESTAMP"),
        Column("refunded_ts", "TIMESTAMP"),
        Column("is_comp", "BOOLEAN"),
        Column("event_type", "STRING", nullable=False, description="SALE, TRANSFER, RESALE, REFUND"),
        Column("__op", "STRING", nullable=False, description="I, U, or D"),
        # __seq is the ONLY reliable ordering key. __commit_ts is backdated by the
        # source when corrections are issued, so ordering by it resurrects
        # refunded orders. This is the single most important comment in the file.
        Column("__seq", "BIGINT", nullable=False, description="Monotonic source sequence — order by THIS"),
        Column("__commit_ts", "TIMESTAMP", nullable=False, description="Source commit time; may be backdated"),
    ),
)

POS = DataContract(
    source_name="pos",
    version="1.0.0",
    description="Concession and merchandise point-of-sale line items, per terminal.",
    evolution_policy=EvolutionPolicy.RESCUE,
    primary_key=("txn_line_id",),
    event_time_column="txn_ts",
    required_columns=("txn_line_id", "txn_id", "game_id", "sku", "txn_ts"),
    columns=(
        Column("txn_line_id", "STRING", nullable=False),
        Column("txn_id", "STRING", nullable=False, description="Basket this line belongs to"),
        Column("game_id", "STRING", nullable=False),
        Column("terminal_id", "STRING", nullable=False),
        Column("stand_location", "STRING"),
        Column("sku", "STRING", nullable=False),
        Column("item_name", "STRING"),
        Column("category", "STRING"),
        Column("unit_price_usd", "DOUBLE"),
        Column("quantity", "INT"),
        Column("gross_amount_usd", "DOUBLE"),
        Column("tax_rate", "DOUBLE", description="Zero on miscommissioned terminals — see M2 lab"),
        Column("tax_amount_usd", "DOUBLE"),
        Column("tender_type", "STRING"),
        Column("txn_ts", "TIMESTAMP", nullable=False),
    ),
)

CRM_ACCOUNTS = DataContract(
    source_name="crm_accounts",
    version="1.0.0",
    description="Ticket account master, nightly full snapshot. PII-bearing.",
    evolution_policy=EvolutionPolicy.ADD_NEW_COLUMNS,
    primary_key=("account_id",),
    required_columns=("account_id",),
    columns=(
        Column("account_id", "STRING", nullable=False),
        Column("first_name", "STRING", is_pii=True),
        Column("last_name", "STRING", is_pii=True),
        Column("email", "STRING", is_pii=True),
        Column("phone", "STRING", is_pii=True),
        Column("street_address", "STRING", is_pii=True),
        Column("postal_code", "STRING", is_pii=True),
        Column("origin_state", "STRING", description="Visitor origin; NV for locals"),
        # Not masked but access-controlled: birth_date determines minor status,
        # which drives a ROW filter in M7. Masking it would defeat the filter.
        Column("birth_date", "DATE", is_pii=True, description="Drives minor-status row filter"),
        Column("cohort", "STRING", description="LOCAL_PLAN, TOURIST, GROUP_BLOCK"),
        Column("payment_token_last4", "STRING", is_pii=True),
        Column("acquisition_channel", "STRING"),
        Column("created_ts", "TIMESTAMP"),
        Column("is_active", "BOOLEAN"),
    ),
)

SEAT_MANIFEST = DataContract(
    source_name="seat_manifest",
    version="1.0.0",
    description="Physical seat inventory. Small, stable reference data — loaded with COPY INTO.",
    evolution_policy=EvolutionPolicy.ADD_NEW_COLUMNS,
    primary_key=("seat_id",),
    required_columns=("seat_id", "section", "seat_class"),
    columns=(
        Column("seat_id", "STRING", nullable=False),
        Column("section", "INT", nullable=False),
        Column("row_label", "STRING"),
        Column("seat_number", "INT"),
        Column("seat_class", "STRING", nullable=False),
        Column("face_price_usd", "DOUBLE"),
        Column("per_cap_multiplier", "DOUBLE"),
        Column("is_ada", "BOOLEAN"),
        Column("is_shaded_day", "BOOLEAN"),
        Column("renumbered_to_section", "INT", description="Set for sections renumbered mid-season"),
        Column("renumber_effective_date", "DATE"),
    ),
)

CONTRACTS: dict[str, DataContract] = {
    c.source_name: c
    for c in (GATE_SCANS, TICKETING_CDC, POS, CRM_ACCOUNTS, SEAT_MANIFEST)
}


def get_contract(source_name: str) -> DataContract:
    try:
        return CONTRACTS[source_name]
    except KeyError:
        known = ", ".join(sorted(CONTRACTS))
        raise KeyError(f"no contract for source '{source_name}'. Known: {known}") from None
