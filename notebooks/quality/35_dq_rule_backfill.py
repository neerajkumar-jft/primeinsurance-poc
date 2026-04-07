# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — Rule Fix Backfill (Path 2 Template)
# MAGIC
# MAGIC **Problem**: when data engineering discovers that a Silver rule was too strict,
# MAGIC they fix the rule in `02_silver_dlt_pipeline.py` and redeploy. But the records
# MAGIC that were already quarantined under the old rule don't automatically come back.
# MAGIC Without a backfill mechanism, those records sit in quarantine forever even
# MAGIC though they would now pass.
# MAGIC
# MAGIC **Solution**: a parameterized template notebook. The caller provides:
# MAGIC - `quarantine_table` — which quarantine table to scan
# MAGIC - `passing_filter_sql` — SQL expression that defines "now passes the new rule"
# MAGIC - `reason_code` — short tag for the audit log
# MAGIC - `notes` — context for the resolution
# MAGIC
# MAGIC The notebook:
# MAGIC 1. Filters the quarantine table to records matching `passing_filter_sql`
# MAGIC 2. Anti-joins against `silver.steward_repaired_records` to skip already-promoted
# MAGIC 3. Promotes the new matches by writing them to the same sidecar table that
# MAGIC    notebook 34 uses, with `action = 'rule_backfill_promote'`
# MAGIC 4. Logs a single bulk resolution entry per backfill run via the helper from
# MAGIC    notebook 36 (path_taken=rule_fix, status=resolved)
# MAGIC
# MAGIC **Why a sidecar instead of writing back to silver.<table>**: same reason as
# MAGIC notebook 34. The Silver tables are managed by DLT. The sidecar approach is
# MAGIC honest and reversible — downstream consumers can UNION the sidecar in via a
# MAGIC view if they want, but the main Silver tables are never touched.
# MAGIC
# MAGIC **Critical safety**: this notebook NEVER deletes from the quarantine table.
# MAGIC The original quarantine row stays in place for audit.
# MAGIC
# MAGIC **Idempotency**: re-running with the same parameters skips records that were
# MAGIC promoted in a previous run. The anti-join is on `(source_table, record_key)`.
# MAGIC
# MAGIC **Workflow for a real rule fix**:
# MAGIC 1. Discover problem: an over-strict rule in `02_silver_dlt_pipeline.py`
# MAGIC 2. Update the rule and redeploy (this fixes future ingestion)
# MAGIC 3. Open this notebook
# MAGIC 4. Set widgets to reflect the new rule (e.g. `passing_filter_sql = "region IN (...)"` with the expanded list)
# MAGIC 5. Run with `dry_run = true` to preview matching records
# MAGIC 6. If preview looks right, set `dry_run = false` and re-run
# MAGIC 7. Check the resolution log; the metrics rollup will pick up the change
# MAGIC
# MAGIC **Inputs**:
# MAGIC - `silver.quarantine_*` (read-only)
# MAGIC - `silver.steward_repaired_records` (read for anti-join, write to append promoted rows)
# MAGIC - `silver.quarantine_resolutions` (write via helper from notebook 36)
# MAGIC
# MAGIC **Zero impact on Phase 1**: read-only on Silver. Writes only to two existing
# MAGIC sidecar/log tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 1 — IMPORTS & WIDGETS ==========

# COMMAND ----------

# MAGIC %run ./36_dq_resolution_logger

# COMMAND ----------

# After %run: log_resolution / bulk_log_resolution / CATALOG / RESOLUTIONS_TABLE
# are available.

dbutils.widgets.dropdown(
    "quarantine_table",
    "quarantine_customers",
    [
        "quarantine_customers",
        "quarantine_claims",
        "quarantine_policy",
        "quarantine_sales",
        "quarantine_cars",
    ],
    "Quarantine table to backfill"
)
dbutils.widgets.text(
    "passing_filter_sql",
    "region IN ('East', 'West', 'Central', 'South', 'North')",
    "SQL expression: records that NOW PASS the rule"
)
dbutils.widgets.text(
    "reason_code",
    "RULE_LOOSENED",
    "Short reason code for the audit log"
)
dbutils.widgets.text(
    "notes",
    "Rule was relaxed — see ticket #__",
    "Notes (free text, stored on every promoted record)"
)
dbutils.widgets.text(
    "rule_name",
    "",
    "Optional: name of the rule that was fixed (for resolutions log)"
)
dbutils.widgets.text(
    "performed_by",
    "",
    "Your email (logged on every action)"
)
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run? (preview only)")

QUARANTINE_TABLE  = dbutils.widgets.get("quarantine_table")
PASSING_FILTER    = dbutils.widgets.get("passing_filter_sql").strip()
REASON_CODE       = dbutils.widgets.get("reason_code").strip() or "RULE_LOOSENED"
NOTES             = dbutils.widgets.get("notes").strip() or None
RULE_NAME         = dbutils.widgets.get("rule_name").strip() or None
PERFORMED_BY      = dbutils.widgets.get("performed_by").strip() or "unknown_engineer"
DRY_RUN           = dbutils.widgets.get("dry_run").lower() == "true"

# Match notebook 34's PK map
PRIMARY_KEY_MAP = {
    "quarantine_customers": "customer_id",
    "quarantine_claims":    "claim_id",
    "quarantine_policy":    "policy_number",
    "quarantine_sales":     "sales_id",
    "quarantine_cars":      "car_id",
}
PK_COL = PRIMARY_KEY_MAP[QUARANTINE_TABLE]

QUARANTINE_FQN  = f"{CATALOG}.silver.{QUARANTINE_TABLE}"
SIDECAR_TABLE   = f"{CATALOG}.silver.steward_repaired_records"

print(f"Catalog:           {CATALOG}")
print(f"Quarantine table:  {QUARANTINE_FQN}")
print(f"Primary key:       {PK_COL}")
print(f"Passing filter:    {PASSING_FILTER}")
print(f"Reason code:       {REASON_CODE}")
print(f"Rule name:         {RULE_NAME}")
print(f"Performed by:      {PERFORMED_BY}")
print(f"Notes:             {NOTES}")
print(f"Dry run:           {DRY_RUN}")
print(f"Sidecar table:     {SIDECAR_TABLE}")
print(f"Resolutions table: {RESOLUTIONS_TABLE}")

if not PASSING_FILTER:
    raise ValueError("passing_filter_sql is required.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 2 — VERIFY SIDECAR EXISTS ==========
# MAGIC
# MAGIC The sidecar is created by notebook 34 (steward review). If you're running this
# MAGIC notebook before notebook 34, we create it here with the same DDL so the
# MAGIC backfill writes don't fail. The two notebooks share the same sidecar so
# MAGIC steward and rule-fix promotions live side by side.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SIDECAR_TABLE} (
    repair_id          BIGINT GENERATED ALWAYS AS IDENTITY,
    source_table       STRING NOT NULL,
    record_key         STRING NOT NULL,
    action             STRING NOT NULL,
    original_payload   STRING,
    repaired_payload   STRING,
    rejection_reason   STRING,
    notes              STRING,
    repaired_by        STRING NOT NULL,
    repaired_at        TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Steward and rule-backfill promoted records. Sidecar to silver.<table>.'
""")

print(f"Verified: {SIDECAR_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 3 — IDENTIFY MATCHING RECORDS ==========
# MAGIC
# MAGIC Apply the passing filter to the quarantine table. Anti-join against the sidecar
# MAGIC to exclude records that were already promoted by a previous backfill or by a
# MAGIC steward action.

# COMMAND ----------

from pyspark.sql import functions as F

# Read the quarantine table
try:
    quarantine_df = spark.table(QUARANTINE_FQN)
    quarantine_total = quarantine_df.count()
    print(f"Total rows in {QUARANTINE_FQN}: {quarantine_total}")
except Exception as e:
    raise RuntimeError(f"Could not read {QUARANTINE_FQN}: {e}")

# Apply the passing filter
try:
    matching = quarantine_df.filter(PASSING_FILTER)
except Exception as e:
    raise RuntimeError(
        f"Could not apply passing_filter_sql='{PASSING_FILTER}'. "
        f"Check the syntax and column names. Error: {e}"
    )

matching_count = matching.count()
print(f"Records matching the new rule:  {matching_count}")

# Anti-join against already-promoted records in the sidecar (for this source_table)
already_promoted = (
    spark.table(SIDECAR_TABLE)
         .filter(F.col("source_table") == F.lit(QUARANTINE_TABLE))
         .select("record_key").distinct()
)
already_promoted_count = already_promoted.count()
print(f"Already-promoted in sidecar:    {already_promoted_count}")

new_matches = (
    matching.alias("q")
            .join(
                already_promoted.alias("s"),
                on=F.col("q." + PK_COL).cast("string") == F.col("s.record_key"),
                how="left_anti",
            )
)
new_matches_count = new_matches.count()
print(f"New records to promote:         {new_matches_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 4 — PREVIEW ==========

# COMMAND ----------

print(f"=== Preview: top 20 records to be promoted ===\n")
new_matches.show(20, truncate=False)

if DRY_RUN:
    print("\nDRY RUN — stopping here. Set dry_run=false to actually promote.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 5 — PROMOTE RECORDS TO SIDECAR ==========
# MAGIC
# MAGIC For each matching record:
# MAGIC - Build a JSON snapshot of the original quarantine row
# MAGIC - Insert a row into the sidecar with `action = 'rule_backfill_promote'`
# MAGIC - The original quarantine row stays put

# COMMAND ----------

import json
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
)
from datetime import datetime

promoted_count = 0

if not DRY_RUN and new_matches_count > 0:
    now = datetime.utcnow()

    # Collect the matching records to driver. For a POC this is fine — production
    # would do this as a Spark transformation, but the volume is small here and the
    # JSON serialization is easier in Python.
    rows = new_matches.collect()

    sidecar_schema = StructType([
        StructField("source_table",     StringType(),    False),
        StructField("record_key",       StringType(),    False),
        StructField("action",           StringType(),    False),
        StructField("original_payload", StringType(),    True),
        StructField("repaired_payload", StringType(),    True),
        StructField("rejection_reason", StringType(),    True),
        StructField("notes",            StringType(),    True),
        StructField("repaired_by",      StringType(),    False),
        StructField("repaired_at",      TimestampType(), False),
    ])

    sidecar_tuples = []
    for r in rows:
        d = r.asDict()
        record_key = str(d.get(PK_COL))
        rejection_reason = d.get("_rejection_reason")
        original_payload = json.dumps(d, default=str)
        sidecar_tuples.append((
            QUARANTINE_TABLE,
            record_key,
            "rule_backfill_promote",
            original_payload,
            None,                   # no repair payload — the record is unchanged, only the rule changed
            rejection_reason,
            NOTES,
            PERFORMED_BY,
            now,
        ))

    if sidecar_tuples:
        spark.createDataFrame(sidecar_tuples, sidecar_schema) \
             .write.format("delta").mode("append").saveAsTable(SIDECAR_TABLE)
        promoted_count = len(sidecar_tuples)
        print(f"Promoted {promoted_count} records to {SIDECAR_TABLE}")
elif DRY_RUN:
    print("DRY RUN — sidecar write skipped.")
else:
    print("Nothing to promote.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 6 — LOG BULK RESOLUTION ==========
# MAGIC
# MAGIC One resolution row per backfill run, summarizing how many records moved through
# MAGIC the rule_fix path. Distinct from the per-record sidecar entries — the resolution
# MAGIC log captures the *decision* (rule was fixed, N records benefited), the sidecar
# MAGIC captures the *records*.

# COMMAND ----------

if not DRY_RUN and promoted_count > 0:
    log_resolution(
        source_table=QUARANTINE_TABLE,
        record_key=None,                 # rule-level, not record-level
        rule_name=RULE_NAME,
        path_taken="rule_fix",
        final_status="resolved",
        reason_code=REASON_CODE,
        affected_records=promoted_count,
        resolved_by=PERFORMED_BY,
        notes=(
            f"Rule backfill: {promoted_count} records promoted via filter "
            f"'{PASSING_FILTER}'. {NOTES or ''}"
        ).strip(),
    )
    print(f"Logged 1 bulk resolution to {RESOLUTIONS_TABLE} (affected_records={promoted_count})")
else:
    print("Nothing to log.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 7 — RUN SUMMARY ==========

# COMMAND ----------

print("=== Rule backfill run summary ===\n")
print(f"Quarantine table:  {QUARANTINE_FQN}")
print(f"Passing filter:    {PASSING_FILTER}")
print(f"Total quarantine:  {quarantine_total}")
print(f"Matched filter:    {matching_count}")
print(f"Already promoted:  {already_promoted_count}")
print(f"New promotions:    {promoted_count}")
print(f"Mode:              {'DRY RUN' if DRY_RUN else 'EXECUTED'}")
print()

print(f"Most recent 10 entries in {SIDECAR_TABLE} (action=rule_backfill_promote):")
spark.sql(f"""
    SELECT source_table, record_key, repaired_by, repaired_at, notes
    FROM {SIDECAR_TABLE}
    WHERE action = 'rule_backfill_promote'
    ORDER BY repaired_at DESC
    LIMIT 10
""").show(truncate=False)

print(f"\nMost recent 5 rule_fix resolutions in {RESOLUTIONS_TABLE}:")
spark.sql(f"""
    SELECT source_table, rule_name, reason_code, affected_records,
           resolved_by, resolved_at, notes
    FROM {RESOLUTIONS_TABLE}
    WHERE path_taken = 'rule_fix'
    ORDER BY created_at DESC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use — worked example
# MAGIC
# MAGIC ### Scenario
# MAGIC The business adds a new region "Northwest" to the customers schema. The Silver
# MAGIC rule `valid_region` was previously hard-coded to:
# MAGIC ```sql
# MAGIC region IN ('East', 'West', 'Central', 'South', 'North')
# MAGIC ```
# MAGIC As a result, every customer with `region = 'Northwest'` is in
# MAGIC `quarantine_customers` with `_rejection_reason = 'invalid region value'`.
# MAGIC
# MAGIC ### Step 1 — fix the rule in the Silver pipeline
# MAGIC In `notebooks/silver/02_silver_dlt_pipeline.py`, update the
# MAGIC `customers_clean` decorator and the `quarantine_customers` filter:
# MAGIC ```python
# MAGIC @dlt.expect_or_drop(
# MAGIC     "valid_region",
# MAGIC     "region IN ('East', 'West', 'Central', 'South', 'North', 'Northwest')"
# MAGIC )
# MAGIC ```
# MAGIC Redeploy the bundle. Future ingestion is now correct.
# MAGIC
# MAGIC ### Step 2 — backfill the existing quarantine
# MAGIC Open this notebook with widgets:
# MAGIC ```
# MAGIC quarantine_table   = quarantine_customers
# MAGIC passing_filter_sql = region IN ('East', 'West', 'Central', 'South', 'North', 'Northwest')
# MAGIC reason_code        = RULE_LOOSENED_REGIONS
# MAGIC rule_name          = valid_region
# MAGIC notes              = Added Northwest region per business request #142
# MAGIC performed_by       = engineer@example.com
# MAGIC dry_run            = true
# MAGIC ```
# MAGIC Run All. Cell 4 shows the records that would be promoted. If correct, set
# MAGIC `dry_run = false` and re-run.
# MAGIC
# MAGIC ### Step 3 — verify
# MAGIC ```sql
# MAGIC SELECT * FROM primeins.silver.steward_repaired_records
# MAGIC WHERE action = 'rule_backfill_promote'
# MAGIC ORDER BY repaired_at DESC;
# MAGIC
# MAGIC SELECT * FROM primeins.silver.quarantine_resolutions
# MAGIC WHERE path_taken = 'rule_fix'
# MAGIC ORDER BY created_at DESC;
# MAGIC ```
# MAGIC Then re-run notebook 37 (metrics rollup) so the dashboard picks up the new
# MAGIC `rule_fix` count, which feeds the `false_positive_rate` metric.
# MAGIC
# MAGIC ### Common parameter examples
# MAGIC
# MAGIC | Rule fix | passing_filter_sql |
# MAGIC |---|---|
# MAGIC | New region added | `region IN ('East','West','Central','South','North','Northwest')` |
# MAGIC | Premium can be zero (free trial) | `policy_annual_premium >= 0` |
# MAGIC | Negative km_driven was a sign error | `ABS(km_driven) >= 0` |
# MAGIC | Empty `claim_rejected` is treated as 'N' | `claim_rejected IN ('Y','N') OR claim_rejected IS NULL` |
# MAGIC
# MAGIC ### Things this notebook does NOT do
# MAGIC
# MAGIC - It does **not** delete from the quarantine table. Original rows stay for audit.
# MAGIC - It does **not** modify the Silver pipeline. You have to update the rule yourself.
# MAGIC - It does **not** insert promoted records into `silver.<table>`. They go to the sidecar.
# MAGIC   Downstream consumers can UNION the sidecar in via a view if they want.
