-- Bronze reference data via COPY INTO.
--
-- WHY COPY INTO HERE AND AUTO LOADER ELSEWHERE
-- --------------------------------------------
-- Both load incrementally from object storage and both track what they have
-- already seen. They are not interchangeable, and the exam tests the choice:
--
--   Auto Loader          streaming, unbounded, file-notification or directory
--                        listing, scales to millions of files, needs a schema
--                        location and a checkpoint. Correct for gate scans and
--                        POS, where files arrive continuously and forever.
--
--   COPY INTO            idempotent batch SQL, tracks loaded files in the target
--                        table's metadata, no checkpoint to manage, retryable by
--                        simply re-running. Correct for small, stable reference
--                        data that arrives occasionally and by hand.
--
-- Seat manifest, pricing, deposits, and schedule are ~a dozen files each,
-- refreshed rarely. Running a streaming pipeline for them would mean managing
-- checkpoints and a schema location for data that changes twice a season. The
-- operational cost of Auto Loader is not justified by the volume.
--
-- The decisive question is NOT "batch or streaming." It is: does this source
-- produce files continuously and unboundedly? If yes, Auto Loader. If it is a
-- bounded, occasional drop, COPY INTO is simpler and simpler wins.
--
-- IDEMPOTENCY: COPY INTO records which files it has already ingested in the
-- target table metadata. Re-running this script does not duplicate rows. That
-- property is what makes it safe to put in a job with retries.

-- --------------------------------------------------------------------------- --
-- Seat manifest
-- --------------------------------------------------------------------------- --

CREATE TABLE IF NOT EXISTS bronze.br_seat_manifest (
  seat_id                  STRING  NOT NULL COMMENT 'Unique seat identifier',
  section                  INT     NOT NULL,
  row_label                STRING,
  seat_number              INT,
  seat_class               STRING  NOT NULL,
  face_price_usd           DOUBLE,
  per_cap_multiplier       DOUBLE,
  is_ada                   BOOLEAN,
  is_shaded_day            BOOLEAN,
  renumbered_to_section    INT     COMMENT 'Populated for sections renumbered mid-season',
  renumber_effective_date  DATE,
  _ingest_ts               TIMESTAMP,
  _source_file             STRING,
  _contract_version        STRING
)
COMMENT 'Bronze seat inventory. Loaded with COPY INTO — small, stable reference data.'
TBLPROPERTIES ('quality' = 'bronze');

COPY INTO bronze.br_seat_manifest
FROM (
  SELECT
    seat_id,
    CAST(section AS INT)                 AS section,
    row_label,
    CAST(seat_number AS INT)             AS seat_number,
    seat_class,
    CAST(face_price_usd AS DOUBLE)       AS face_price_usd,
    CAST(per_cap_multiplier AS DOUBLE)   AS per_cap_multiplier,
    CAST(is_ada AS BOOLEAN)              AS is_ada,
    CAST(is_shaded_day AS BOOLEAN)       AS is_shaded_day,
    CAST(renumbered_to_section AS INT)   AS renumbered_to_section,
    CAST(renumber_effective_date AS DATE) AS renumber_effective_date,
    current_timestamp()                  AS _ingest_ts,
    _metadata.file_path                  AS _source_file,
    '1.0.0'                              AS _contract_version
  FROM '/Volumes/${catalog}/landing/files/seat_manifest'
)
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  -- Explicit schema inference off: we CAST every column above instead. Letting
  -- CSV inference decide types is how a seat_number becomes a double and a
  -- downstream join silently returns nothing.
  'inferSchema' = 'false'
)
COPY_OPTIONS (
  -- mergeSchema false: this is reference data under an ADD_NEW_COLUMNS contract,
  -- but new columns should arrive through a reviewed contract bump, not by
  -- surprise on a Tuesday.
  'mergeSchema' = 'false'
);

-- --------------------------------------------------------------------------- --
-- Pricing feed
-- --------------------------------------------------------------------------- --

CREATE TABLE IF NOT EXISTS bronze.br_price_point (
  price_point_id     STRING NOT NULL,
  game_id            STRING NOT NULL,
  seat_class         STRING NOT NULL,
  face_price_usd     DOUBLE,
  listed_price_usd   DOUBLE,
  valid_from_ts      TIMESTAMP,
  valid_to_ts        TIMESTAMP,
  pricing_rule       STRING,
  _ingest_ts         TIMESTAMP,
  _source_file       STRING,
  _contract_version  STRING
)
COMMENT 'Bronze dynamic pricing. Overlapping validity windows are PRESERVED here and resolved in silver.'
TBLPROPERTIES ('quality' = 'bronze');

COPY INTO bronze.br_price_point
FROM (
  SELECT
    price_point_id,
    game_id,
    seat_class,
    CAST(face_price_usd AS DOUBLE)    AS face_price_usd,
    CAST(listed_price_usd AS DOUBLE)  AS listed_price_usd,
    CAST(valid_from_ts AS TIMESTAMP)  AS valid_from_ts,
    CAST(valid_to_ts AS TIMESTAMP)    AS valid_to_ts,
    pricing_rule,
    current_timestamp()               AS _ingest_ts,
    _metadata.file_path               AS _source_file,
    '1.0.0'                           AS _contract_version
  FROM '/Volumes/${catalog}/landing/files/pricing'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false')
COPY_OPTIONS ('mergeSchema' = 'false');

-- --------------------------------------------------------------------------- --
-- Reconciliation
-- --------------------------------------------------------------------------- --
-- The generator writes _manifest.json with per-dataset row counts. Comparing
-- against it is what converts "the load succeeded" from a statement about the
-- absence of errors into a statement about completeness. A job that loads zero
-- rows also produces no errors.

SELECT
  'br_seat_manifest' AS table_name,
  COUNT(*)           AS bronze_rows,
  COUNT(DISTINCT seat_id) AS distinct_keys,
  CASE WHEN COUNT(*) = COUNT(DISTINCT seat_id)
       THEN 'OK'
       ELSE RAISE_ERROR('Duplicate seat_id in bronze — COPY INTO ran against overlapping files')
  END AS key_uniqueness
FROM bronze.br_seat_manifest;
