# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — Retention Archiver (Guardrail 3)
# MAGIC
# MAGIC **Problem**: without retention, quarantine grows unbounded. Records that nobody
# MAGIC ever resolves sit forever, the table becomes a graveyard, and the cost of
# MAGIC scanning quarantine in dashboards and alerts grows linearly. The graveyard is
# MAGIC also the worst possible signal for compliance: "we caught these records but
# MAGIC nothing happened to them."
# MAGIC
# MAGIC **Solution**: a 90-day retention policy, enforced as a scheduled archive
# MAGIC operation. Records older than `retention_days` are added to a new
# MAGIC `silver.dq_archive_register` table and logged to `quarantine_resolutions`
# MAGIC as `permanent_reject / RETENTION_EXPIRED`. The original quarantine row stays
# MAGIC in place for audit (flag-only mode, the default).
# MAGIC
# MAGIC **Why flag-only**: the team has decided that real DELETEs against quarantine
# MAGIC tables wait for Phase 2 with explicit stakeholder approval. Flag-only mode
# MAGIC keeps Phase 1.5 fully reversible — if anything goes wrong, you reset a flag,
# MAGIC nothing is lost. The register entries are append-only.
# MAGIC
# MAGIC **Two operational modes**:
# MAGIC
# MAGIC | Mode | What it does | Default |
# MAGIC |------|--------------|---------|
# MAGIC | `flag_only` | Insert into register + log resolution. NEVER touches quarantine tables. | yes |
# MAGIC | `delete`    | Same as flag_only PLUS deletes the matching rows from the quarantine table. **Refuses to run** without an explicit confirmation token. | no |
# MAGIC
# MAGIC **Inputs**:  `silver.quarantine_*` (read-only), `silver.quarantine_resolutions` (via helper)
# MAGIC **Outputs**: `silver.dq_archive_register` (new), `silver.quarantine_resolutions` (append)
# MAGIC
# MAGIC **Idempotency**: re-running on the same day is cheap and safe. The notebook
# MAGIC checks the register before inserting, so already-archived records are skipped.
# MAGIC
# MAGIC **Zero impact on Phase 1**: in the default `flag_only` mode this notebook is
# MAGIC entirely additive. No existing tables are modified.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 1 — IMPORTS & CONFIGURATION ==========

# COMMAND ----------

# MAGIC %run ./36_dq_resolution_logger

# COMMAND ----------

# After %run, log_resolution / bulk_log_resolution / CATALOG / RESOLUTIONS_TABLE are
# available from notebook 36.

dbutils.widgets.text("retention_days", "90", "Retention threshold in days")
dbutils.widgets.dropdown("mode", "flag_only", ["flag_only", "delete"], "Archive mode")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run? (preview only)")
dbutils.widgets.text(
    "confirm_delete",
    "",
    "DELETE confirmation (must be 'I_UNDERSTAND_THIS_DELETES_RECORDS' for mode=delete)"
)

RETENTION_DAYS  = int(dbutils.widgets.get("retention_days"))
MODE            = dbutils.widgets.get("mode")
DRY_RUN         = dbutils.widgets.get("dry_run").lower() == "true"
CONFIRM_DELETE  = dbutils.widgets.get("confirm_delete").strip()

REGISTER_TABLE = f"{CATALOG}.silver.dq_archive_register"

# Hard-coded list of (quarantine_table, primary_key_column) — must match notebook 02
QUARANTINE_TABLES = [
    ("quarantine_customers", "customer_id"),
    ("quarantine_claims",    "claim_id"),
    ("quarantine_policy",    "policy_number"),
    ("quarantine_sales",     "sales_id"),
    ("quarantine_cars",      "car_id"),
]

print(f"Catalog:           {CATALOG}")
print(f"Retention days:    {RETENTION_DAYS}")
print(f"Mode:              {MODE}")
print(f"Dry run:           {DRY_RUN}")
print(f"Register table:    {REGISTER_TABLE}")
print(f"Resolutions table: {RESOLUTIONS_TABLE}")
print()

# Hard refusal of delete mode without explicit confirmation
if MODE == "delete":
    if DRY_RUN:
        print("Mode is 'delete' but dry_run is true — will preview only, nothing will be deleted.")
    elif CONFIRM_DELETE != "I_UNDERSTAND_THIS_DELETES_RECORDS":
        raise RuntimeError(
            "REFUSED: mode='delete' with dry_run='false' requires "
            "confirm_delete='I_UNDERSTAND_THIS_DELETES_RECORDS'. "
            "Phase 1.5 is flag-only by team agreement; real deletes need explicit approval."
        )
    else:
        print("WARNING: mode=delete + dry_run=false + confirm_delete passed. "
              "Real DELETEs will run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 2 — REGISTER TABLE DDL ==========

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REGISTER_TABLE} (
    archive_id        BIGINT GENERATED ALWAYS AS IDENTITY,
    source_table      STRING NOT NULL  COMMENT 'e.g. quarantine_customers',
    record_key        STRING NOT NULL  COMMENT 'natural key (customer_id, claim_id, etc.)',
    rejection_reason  STRING,
    load_timestamp    TIMESTAMP        COMMENT 'when the record originally arrived in Bronze',
    age_days          INT              COMMENT 'age in days at the time of archival',
    archived_at       TIMESTAMP NOT NULL,
    archived_by       STRING NOT NULL  COMMENT 'system or user',
    archive_status    STRING NOT NULL  COMMENT 'flagged | deleted',
    retention_days    INT              COMMENT 'the threshold that triggered the archive'
)
USING DELTA
COMMENT 'Append-only register of records archived by the retention policy. Phase 1.5 flag-only.'
""")

print(f"Created/verified: {REGISTER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 3 — FIND EXPIRED RECORDS PER TABLE ==========
# MAGIC
# MAGIC For each quarantine table: find rows where `_load_timestamp < (now - retention_days)`
# MAGIC AND not already in the register. Build a per-table preview.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, TimestampType,
)
from datetime import datetime

now_ts = datetime.utcnow()
print(f"Cutoff: rows with _load_timestamp older than now - {RETENTION_DAYS} days")
print(f"        (now = {now_ts})\n")

# Pull existing register entries once for fast in-memory exclusion
try:
    existing = (
        spark.table(REGISTER_TABLE)
             .select("source_table", "record_key")
             .distinct()
    )
    existing_count = existing.count()
except Exception:
    existing = spark.createDataFrame([], "source_table STRING, record_key STRING")
    existing_count = 0

print(f"Already-archived records in register: {existing_count}\n")

per_table_expired = {}   # source_table -> dataframe of expired-but-not-yet-archived rows
preview_summary = []

for q_table, pk_col in QUARANTINE_TABLES:
    fqn = f"{CATALOG}.silver.{q_table}"
    try:
        df = spark.table(fqn)
    except Exception as e:
        print(f"  (skipping {fqn}: {e.__class__.__name__})")
        continue

    # Identify expired rows. Note: _load_timestamp comes from Bronze Auto Loader.
    expired = (
        df.filter(F.col("_load_timestamp") < F.expr(f"current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS"))
          .select(
              F.lit(q_table).alias("source_table"),
              F.col(pk_col).cast("string").alias("record_key"),
              F.col("_rejection_reason").alias("rejection_reason"),
              F.col("_load_timestamp").alias("load_timestamp"),
              F.datediff(F.current_timestamp(), F.col("_load_timestamp")).alias("age_days"),
          )
    )

    # Anti-join against existing register entries to skip already-archived
    new_to_archive = (
        expired.alias("e")
               .join(
                   existing.alias("x"),
                   on=[F.col("e.source_table") == F.col("x.source_table"),
                       F.col("e.record_key")   == F.col("x.record_key")],
                   how="left_anti",
               )
    )

    n = new_to_archive.count()
    per_table_expired[q_table] = new_to_archive
    preview_summary.append((q_table, n))
    print(f"  {q_table:25s}  expired_and_new = {n}")

print(f"\nTotal new expired rows to archive: {sum(n for _, n in preview_summary)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 4 — DRY RUN PREVIEW ==========
# MAGIC
# MAGIC Show a sample of what would be archived. If `dry_run = true`, the notebook stops
# MAGIC after this cell — nothing is written.

# COMMAND ----------

print("=== Sample of expired records (top 20 across all tables) ===\n")

preview_dfs = [df for df in per_table_expired.values() if df.limit(1).count() > 0]
if preview_dfs:
    combined = preview_dfs[0]
    for d in preview_dfs[1:]:
        combined = combined.unionByName(d)
    combined.orderBy(F.col("age_days").desc()).show(20, truncate=False)
else:
    print("Nothing expired. Either the data is fresher than the retention threshold, "
          "or all expired records have already been archived.")

if DRY_RUN:
    print("\nDRY RUN — stopping here. Set dry_run=false to actually archive.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 5 — WRITE TO REGISTER (flag-only) ==========
# MAGIC
# MAGIC Append new expired rows to `dq_archive_register` with `archive_status = 'flagged'`.
# MAGIC Always runs in either mode (delete mode does this PLUS the actual delete in cell 6).
# MAGIC Skipped entirely when `dry_run = true`.

# COMMAND ----------

archived_count = 0
archive_status = "deleted" if (MODE == "delete" and not DRY_RUN) else "flagged"

if not DRY_RUN:
    register_schema = StructType([
        StructField("source_table",     StringType(),    False),
        StructField("record_key",       StringType(),    False),
        StructField("rejection_reason", StringType(),    True),
        StructField("load_timestamp",   TimestampType(), True),
        StructField("age_days",         IntegerType(),   True),
        StructField("archived_at",      TimestampType(), False),
        StructField("archived_by",      StringType(),    False),
        StructField("archive_status",   StringType(),    False),
        StructField("retention_days",   IntegerType(),   True),
    ])

    for q_table, df in per_table_expired.items():
        if df.limit(1).count() == 0:
            continue

        rows_to_insert = (
            df.withColumn("archived_at",    F.lit(now_ts))
              .withColumn("archived_by",    F.lit("system_retention_archiver"))
              .withColumn("archive_status", F.lit(archive_status))
              .withColumn("retention_days", F.lit(RETENTION_DAYS).cast("int"))
              .select(
                  "source_table",
                  "record_key",
                  "rejection_reason",
                  "load_timestamp",
                  F.col("age_days").cast("int"),
                  "archived_at",
                  "archived_by",
                  "archive_status",
                  "retention_days",
              )
        )

        n = rows_to_insert.count()
        rows_to_insert.write.format("delta").mode("append").saveAsTable(REGISTER_TABLE)
        archived_count += n
        print(f"  Archived {n} rows from {q_table} (status={archive_status})")

    print(f"\nTotal rows archived this run: {archived_count}")
else:
    print("DRY RUN — register write skipped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 6 — DELETE FROM QUARANTINE (delete mode only) ==========
# MAGIC
# MAGIC Only runs when ALL of:
# MAGIC - `mode = delete`
# MAGIC - `dry_run = false`
# MAGIC - `confirm_delete = I_UNDERSTAND_THIS_DELETES_RECORDS`
# MAGIC
# MAGIC In Phase 1.5 the team decision is flag-only, so this cell will always be a no-op
# MAGIC unless someone explicitly opts in. The hard refusal is in cell 1.

# COMMAND ----------

deleted_total = 0

if MODE == "delete" and not DRY_RUN and CONFIRM_DELETE == "I_UNDERSTAND_THIS_DELETES_RECORDS":
    print("DELETE MODE: removing archived rows from quarantine tables.\n")

    # Delete using IDs already inserted into the register in this run.
    # We use the just-archived register entries for this run by joining on archived_at = now_ts.
    for q_table, pk_col in QUARANTINE_TABLES:
        delete_keys_df = (
            spark.table(REGISTER_TABLE)
                 .filter(
                     (F.col("source_table") == q_table) &
                     (F.col("archived_at") == F.lit(now_ts))
                 )
                 .select("record_key")
                 .distinct()
        )
        keys = [r["record_key"] for r in delete_keys_df.collect()]
        if not keys:
            continue

        # Build a key list literal carefully — sales_id etc may be ints
        key_list_sql = ", ".join(f"'{k}'" for k in keys)
        sql = f"""
            DELETE FROM {CATALOG}.silver.{q_table}
            WHERE CAST({pk_col} AS STRING) IN ({key_list_sql})
        """
        spark.sql(sql)
        deleted_total += len(keys)
        print(f"  Deleted {len(keys)} rows from {CATALOG}.silver.{q_table}")

    print(f"\nTotal rows deleted: {deleted_total}")
else:
    print("Delete cell is a no-op (mode != delete, or dry_run, or no confirmation).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 7 — LOG RESOLUTIONS ==========
# MAGIC
# MAGIC For every newly-archived record, log a row to `quarantine_resolutions` so the
# MAGIC retention action shows up in the metrics rollup as `permanent_reject` /
# MAGIC `RETENTION_EXPIRED`. We log one resolution per `(source_table, rule)` group
# MAGIC rather than one per record — that's a deliberate choice to keep the resolutions
# MAGIC table compact while still capturing the audit signal.

# COMMAND ----------

if not DRY_RUN and archived_count > 0:
    # Read the just-archived rows back from the register and group by (source_table, rejection_reason)
    grouped = (
        spark.table(REGISTER_TABLE)
             .filter(F.col("archived_at") == F.lit(now_ts))
             .groupBy("source_table", "rejection_reason")
             .agg(F.count("*").cast("int").alias("affected"))
             .collect()
    )

    resolution_rows = []
    for r in grouped:
        resolution_rows.append(dict(
            source_table=r["source_table"],
            record_key=None,
            rule_name=None,
            path_taken="permanent_reject",
            final_status="rejected",
            reason_code="RETENTION_EXPIRED",
            affected_records=r["affected"],
            resolved_by="system_retention_archiver",
            notes=(
                f"Retention policy {RETENTION_DAYS} days. "
                f"Archive mode: {archive_status}. "
                f"Rejection reason: {r['rejection_reason']}"
            ),
        ))

    n_logged = bulk_log_resolution(resolution_rows)
    print(f"Logged {n_logged} resolution entries to {RESOLUTIONS_TABLE}")
else:
    print("Nothing to log (dry run or zero archives).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 8 — RUN SUMMARY ==========

# COMMAND ----------

print("=== Retention archiver run summary ===\n")
print(f"Cutoff:               now - {RETENTION_DAYS} days")
print(f"Mode:                 {MODE}")
print(f"Dry run:              {DRY_RUN}")
print(f"New rows archived:    {archived_count}")
print(f"Rows deleted:         {deleted_total}")
print()

print(f"Total entries in {REGISTER_TABLE}:")
spark.sql(f"""
    SELECT archive_status, COUNT(*) AS rows
    FROM {REGISTER_TABLE}
    GROUP BY archive_status
""").show()

print(f"\nMost recent 10 archive entries:")
spark.sql(f"""
    SELECT source_table, record_key, age_days, archive_status, archived_at, retention_days
    FROM {REGISTER_TABLE}
    ORDER BY archived_at DESC, age_days DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use
# MAGIC
# MAGIC ### Default (flag-only)
# MAGIC
# MAGIC ```
# MAGIC retention_days = 90
# MAGIC mode           = flag_only
# MAGIC dry_run        = true     # first run, just see the numbers
# MAGIC ```
# MAGIC Run all cells. Inspect cell 4's preview. If happy, set `dry_run = false` and re-run.
# MAGIC
# MAGIC ### Phase 1.5 production
# MAGIC
# MAGIC Schedule daily with:
# MAGIC ```
# MAGIC retention_days = 90
# MAGIC mode           = flag_only
# MAGIC dry_run        = false
# MAGIC ```
# MAGIC Records get registered, resolutions get logged, but quarantine tables stay
# MAGIC untouched. Fully reversible.
# MAGIC
# MAGIC ### Phase 2 (real deletes — explicit opt-in only)
# MAGIC
# MAGIC When the team is ready to actually delete:
# MAGIC ```
# MAGIC retention_days = 90
# MAGIC mode           = delete
# MAGIC dry_run        = false
# MAGIC confirm_delete = I_UNDERSTAND_THIS_DELETES_RECORDS
# MAGIC ```
# MAGIC Without the confirmation token the notebook refuses to run in delete mode.
# MAGIC
# MAGIC ### Lowering the threshold for testing
# MAGIC
# MAGIC The data in this POC is recent — nothing is 90 days old. To test the archiver
# MAGIC end-to-end without waiting:
# MAGIC ```
# MAGIC retention_days = 0     # archives everything
# MAGIC mode           = flag_only
# MAGIC dry_run        = true  # then false on the second run
# MAGIC ```
# MAGIC This will flag every quarantined record. Don't forget to clean the register
# MAGIC afterward if you want a clean slate:
# MAGIC ```sql
# MAGIC TRUNCATE TABLE primeins.silver.dq_archive_register;
# MAGIC DELETE FROM primeins.silver.quarantine_resolutions
# MAGIC WHERE resolved_by = 'system_retention_archiver';
# MAGIC ```
