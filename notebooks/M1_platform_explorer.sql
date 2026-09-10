-- Databricks notebook source
-- MAGIC %md
-- MAGIC # M1 — Platform Explorer
-- MAGIC
-- MAGIC Run this cell by cell. The point is to make the abstractions in
-- MAGIC `docs/modules/M1_platform.md` physically visible: you will look at an actual
-- MAGIC transaction log, watch versions get created, and time travel to an earlier state.
-- MAGIC
-- MAGIC **Prerequisite:** the bootstrap job has run, so `kings_dev` exists.

-- COMMAND ----------
-- MAGIC %md ## Part 1 — Where am I? The three-level namespace

-- COMMAND ----------

SELECT current_catalog() AS catalog, current_schema() AS schema, current_user() AS me;

-- COMMAND ----------

USE CATALOG kings_dev;
USE SCHEMA ops;

-- Every table reference is catalog.schema.table. Two-level names only work
-- because a default catalog is set — they are not actually two-level.
SHOW CATALOGS;

-- COMMAND ----------

-- information_schema is the queryable view of Unity Catalog metadata. Learn it:
-- it answers governance questions that clicking through the UI cannot.
SELECT catalog_name, schema_name
FROM system.information_schema.schemata
WHERE catalog_name = 'kings_dev'
ORDER BY schema_name;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Part 2 — A Delta table is Parquet plus a log
-- MAGIC
-- MAGIC Create a table, then look at what actually landed on disk.

-- COMMAND ----------

CREATE OR REPLACE TABLE ops.m1_demo (
  id     INT,
  label  STRING,
  loaded TIMESTAMP
) COMMENT 'M1 teaching table. Safe to drop.';

INSERT INTO ops.m1_demo VALUES (1, 'first',  current_timestamp());

-- COMMAND ----------

-- DESCRIBE DETAIL shows the physical reality: format, location, file count, size.
DESCRIBE DETAIL ops.m1_demo;

-- COMMAND ----------

-- Copy the `location` value from above into the next cell.
-- You are about to look at the actual files.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC # Replace with the location from DESCRIBE DETAIL
-- MAGIC location = spark.sql("DESCRIBE DETAIL ops.m1_demo").collect()[0]["location"]
-- MAGIC print("Table location:", location, "\n")
-- MAGIC
-- MAGIC for f in dbutils.fs.ls(location):
-- MAGIC     print(f"{f.size:>10}  {f.name}")
-- MAGIC
-- MAGIC print("\n_delta_log contents:")
-- MAGIC for f in dbutils.fs.ls(location + "/_delta_log"):
-- MAGIC     print(f"{f.size:>10}  {f.name}")

-- COMMAND ----------
-- MAGIC %md
-- MAGIC You should see ordinary `.snappy.parquet` files and a `_delta_log/` directory
-- MAGIC containing `00000000000000000000.json`.
-- MAGIC
-- MAGIC **That JSON file IS the table definition at version 0.** Read it:

-- COMMAND ----------
-- MAGIC %python
-- MAGIC log_file = location + "/_delta_log/00000000000000000000.json"
-- MAGIC for line in dbutils.fs.head(log_file, 4000).split("\n"):
-- MAGIC     if line.strip():
-- MAGIC         print(line[:300])
-- MAGIC
-- MAGIC # Look for: "metaData" (the schema), "add" (a file added to the table),
-- MAGIC # and inside "add", a "stats" field with min/max per column.
-- MAGIC # Those stats are what makes file skipping possible.

-- COMMAND ----------
-- MAGIC %md ## Part 3 — Versions and time travel

-- COMMAND ----------

INSERT INTO ops.m1_demo VALUES (2, 'second', current_timestamp());
INSERT INTO ops.m1_demo VALUES (3, 'third',  current_timestamp());
UPDATE ops.m1_demo SET label = 'CHANGED' WHERE id = 1;

-- COMMAND ----------

-- Every operation is a version. Note the operation column: WRITE, UPDATE, etc.
DESCRIBE HISTORY ops.m1_demo;

-- COMMAND ----------

-- Current state
SELECT * FROM ops.m1_demo ORDER BY id;

-- COMMAND ----------

-- State at version 1: before the second insert, before the update.
-- Nothing was restored. The old files were never deleted; the log just points
-- somewhere else now.
SELECT * FROM ops.m1_demo VERSION AS OF 1 ORDER BY id;

-- COMMAND ----------

-- Time travel by timestamp works the same way.
-- SELECT * FROM ops.m1_demo TIMESTAMP AS OF '2027-01-01T00:00:00';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Part 4 — Managed vs. external (Break-it Lab 4.2)
-- MAGIC
-- MAGIC The destructive difference. Do this once and you will never forget it.

-- COMMAND ----------

-- Managed: Unity Catalog owns the storage location and the lifecycle.
CREATE OR REPLACE TABLE ops.m1_managed (id INT, note STRING);
INSERT INTO ops.m1_managed VALUES (1, 'managed table row');

-- External: we specify LOCATION, so we own the files.
CREATE OR REPLACE TABLE ops.m1_external (id INT, note STRING)
LOCATION '/Volumes/kings_dev/ops/checkpoints/_m1_external_demo';
INSERT INTO ops.m1_external VALUES (1, 'external table row');

-- COMMAND ----------

-- Confirm the difference is visible in metadata BEFORE dropping anything.
SELECT table_name, table_type
FROM system.information_schema.tables
WHERE table_schema = 'ops' AND table_name LIKE 'm1_%';

-- COMMAND ----------
-- MAGIC %python
-- MAGIC ext_location = "/Volumes/kings_dev/ops/checkpoints/_m1_external_demo"
-- MAGIC print("External files BEFORE drop:", len(dbutils.fs.ls(ext_location)))

-- COMMAND ----------

DROP TABLE ops.m1_managed;
DROP TABLE ops.m1_external;

-- COMMAND ----------
-- MAGIC %python
-- MAGIC # The external table's files survive the DROP. The managed table's do not.
-- MAGIC try:
-- MAGIC     files = dbutils.fs.ls(ext_location)
-- MAGIC     print(f"External files AFTER drop: {len(files)} — DATA SURVIVED")
-- MAGIC except Exception as e:
-- MAGIC     print("External location gone:", e)
-- MAGIC
-- MAGIC # LESSON: DROP TABLE means something different depending on table type.
-- MAGIC # Managed is the right default. External is for data shared with outside
-- MAGIC # tools or data you are not permitted to move.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Part 5 — Break time travel on purpose (Break-it Lab 4.3)
-- MAGIC
-- MAGIC VACUUM deletes files no longer referenced by recent versions. Time travel
-- MAGIC is a *consequence* of those files still existing — not a backup service.

-- COMMAND ----------
-- MAGIC %python
-- MAGIC # The retention check exists to stop you doing this by accident.
-- MAGIC # Disabling it in production is almost always wrong.
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

-- COMMAND ----------

VACUUM ops.m1_demo RETAIN 0 HOURS;

-- COMMAND ----------

-- Now try to travel back. Expect a failure: the files that version pointed to
-- are gone.
SELECT * FROM ops.m1_demo VERSION AS OF 1 ORDER BY id;

-- COMMAND ----------
-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
-- MAGIC print("Retention check re-enabled.")
-- MAGIC print("LESSON: time travel is not a backup. It works because files have not")
-- MAGIC print("been deleted yet. Default retention is 7 days.")

-- COMMAND ----------
-- MAGIC %md ## Part 6 — Compute: what am I actually running on?

-- COMMAND ----------
-- MAGIC %python
-- MAGIC # On Free Edition this is serverless — there is no cluster spec to inspect,
-- MAGIC # which is itself the lesson. Note what you CANNOT set.
-- MAGIC print("Spark version:", spark.version)
-- MAGIC for key in ["spark.databricks.clusterUsageTags.clusterName",
-- MAGIC             "spark.databricks.clusterUsageTags.clusterNodeType",
-- MAGIC             "spark.sql.shuffle.partitions"]:
-- MAGIC     try:
-- MAGIC         print(f"{key} = {spark.conf.get(key)}")
-- MAGIC     except Exception:
-- MAGIC         print(f"{key} = <not available on serverless>")

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Compute selection — memorize this table
-- MAGIC
-- MAGIC | Type | Use for | Lifecycle | Relative cost |
-- MAGIC |---|---|---|---|
-- MAGIC | All-purpose | Interactive notebooks, shared | Manual start/stop, idles | Highest DBU rate |
-- MAGIC | Job compute | Scheduled jobs | Created per run, terminates | Lower DBU rate |
-- MAGIC | SQL warehouse | SQL and BI | Auto start/stop | Varies by class |
-- MAGIC | Serverless | Any, managed | Instant | All-in rate |
-- MAGIC
-- MAGIC **The classic cost mistake:** running scheduled production jobs on
-- MAGIC all-purpose compute. Higher DBU rate *and* it idles between runs.
-- MAGIC
-- MAGIC **On DBU comparisons:** classic bills DBUs *plus* your cloud VM spend;
-- MAGIC serverless is one all-in rate. Comparing DBU rates directly is a mistake.

-- COMMAND ----------
-- MAGIC %md ## Cleanup

-- COMMAND ----------

DROP TABLE IF EXISTS ops.m1_demo;
