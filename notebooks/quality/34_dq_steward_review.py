# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — Steward Review Workflow (Path 3 — Manual Repair)
# MAGIC
# MAGIC **Problem**: some quarantined records are salvageable but need human judgment
# MAGIC (e.g. a customer record where region is missing but the city is "Sacramento" and
# MAGIC a steward can fill in `region = West`). Without a workflow these records sit
# MAGIC indefinitely, eventually going to retention archive.
# MAGIC
# MAGIC **Solution**: a notebook that:
# MAGIC 1. Shows the steward what's waiting for review (queue view + per-rule summary)
# MAGIC 2. Lets them inspect a single record by `(source_table, record_key)`
# MAGIC 3. Lets them take one of four actions:
# MAGIC    - `approve_as_is` — record is fine, promote it (path 3, status=resolved)
# MAGIC    - `repair_and_promote` — provide JSON of corrected fields, promote (path 3, status=resolved)
# MAGIC    - `permanently_reject` — record is unrecoverable (path 4, status=rejected)
# MAGIC    - `escalate` — flag for follow-up (path 3, status=in_progress)
# MAGIC 4. Logs every decision to `silver.quarantine_resolutions` via the helper from notebook 36
# MAGIC 5. Promotes repaired/approved records to a **sidecar table**
# MAGIC    `silver.steward_repaired_records` rather than the main Silver table
# MAGIC
# MAGIC **Why the sidecar?** The main `silver.<table>` tables are managed by DLT. Writing
# MAGIC directly would either be overwritten on the next pipeline run or cause conflicts.
# MAGIC The sidecar table is honest: "these are records the steward salvaged, kept
# MAGIC separately." Downstream consumers can UNION them in via a view if they want.
# MAGIC In Phase 2 with Lakebase, the same logic backs a real app instead of widgets.
# MAGIC
# MAGIC **Workflow**:
# MAGIC 1. Run cells 1–4 to see the queue
# MAGIC 2. Pick a record. Set `inspect_*` widgets, run cell 5 to see full details
# MAGIC 3. Set `action_*` widgets, run cell 6 to execute the decision
# MAGIC 4. Repeat
# MAGIC
# MAGIC **Idempotency**: each action is logged with a timestamp. Re-running an action
# MAGIC for the same `(source_table, record_key)` adds a new resolution entry — the
# MAGIC most recent one wins. The promotion writes are append-only so re-promotion
# MAGIC creates duplicates; check the sidecar before re-promoting.
# MAGIC
# MAGIC **Zero impact on Phase 1**: this notebook reads from `silver.quarantine_*` and
# MAGIC `silver.dq_issues` only. It writes to two new tables in the silver schema:
# MAGIC `silver.steward_repaired_records` (sidecar) and the existing
# MAGIC `silver.quarantine_resolutions` (via notebook 36's helpers).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 1 — CONFIGURATION & IMPORTS ==========

# COMMAND ----------

# MAGIC %run ./36_dq_resolution_logger

# COMMAND ----------

# After %run above, log_resolution and bulk_log_resolution are available.
# CATALOG and RESOLUTIONS_TABLE are also available (set by notebook 36).

dbutils.widgets.text("steward_email", "", "Your email (logged on every action)")
STEWARD_EMAIL = dbutils.widgets.get("steward_email").strip() or "unknown_steward"

REPAIRED_TABLE = f"{CATALOG}.silver.steward_repaired_records"

# Quarantine table list — must match notebook 02_silver_dlt_pipeline.py
QUARANTINE_TABLES = [
    "quarantine_customers",
    "quarantine_claims",
    "quarantine_policy",
    "quarantine_sales",
    "quarantine_cars",
]

# Map quarantine table → primary key column for lookups by record_key
PRIMARY_KEY_MAP = {
    "quarantine_customers": "customer_id",
    "quarantine_claims":    "claim_id",
    "quarantine_policy":    "policy_number",
    "quarantine_sales":     "sales_id",
    "quarantine_cars":      "car_id",
}

print(f"Catalog:           {CATALOG}")
print(f"Steward:           {STEWARD_EMAIL}")
print(f"Resolutions log:   {RESOLUTIONS_TABLE}")
print(f"Repaired sidecar:  {REPAIRED_TABLE}")
print(f"Quarantine tables: {QUARANTINE_TABLES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 2 — REPAIRED RECORDS SIDECAR TABLE DDL ==========
# MAGIC
# MAGIC Sidecar table for promoted records. Wide enough to capture context: the steward
# MAGIC who approved it, the original rejection reason, the action taken, and a JSON
# MAGIC blob of the corrected payload (since each Silver table has a different schema).

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REPAIRED_TABLE} (
    repair_id          BIGINT GENERATED ALWAYS AS IDENTITY,
    source_table       STRING NOT NULL  COMMENT 'e.g. quarantine_customers',
    record_key         STRING NOT NULL  COMMENT 'natural key (customer_id, claim_id, etc.)',
    action             STRING NOT NULL  COMMENT 'approve_as_is | repair_and_promote',
    original_payload   STRING           COMMENT 'JSON of the row as it was in quarantine',
    repaired_payload   STRING           COMMENT 'JSON of the row after steward edits (NULL for approve_as_is)',
    rejection_reason   STRING           COMMENT 'the _rejection_reason from the quarantine row',
    notes              STRING,
    repaired_by        STRING NOT NULL,
    repaired_at        TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Steward-repaired records salvaged from quarantine. Sidecar to silver.<table>; promote via UNION view if needed.'
""")

print(f"Created/verified: {REPAIRED_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 3 — REVIEW QUEUE: PER-RULE SUMMARY ==========
# MAGIC
# MAGIC The 30,000-foot view: how many records are waiting per (table, rule). Run this
# MAGIC first to decide where to focus. Records already resolved (in quarantine_resolutions)
# MAGIC are excluded by record_key match where possible.

# COMMAND ----------

from pyspark.sql import functions as F

# Build a per-table summary by counting rows in each quarantine table grouped by
# rejection reason. We don't need to exclude already-resolved records here — the
# inspector and action cells handle that more precisely.

summary_rows = []
for q_table in QUARANTINE_TABLES:
    fqn = f"{CATALOG}.silver.{q_table}"
    try:
        df = spark.table(fqn)
        grouped = (
            df.groupBy("_rejection_reason")
              .agg(F.count("*").alias("record_count"))
              .withColumn("source_table", F.lit(q_table))
              .select("source_table", "_rejection_reason", "record_count")
        )
        rows = grouped.collect()
        for r in rows:
            summary_rows.append((r["source_table"], r["_rejection_reason"], r["record_count"]))
    except Exception as e:
        print(f"  (skipping {fqn}: {e.__class__.__name__})")

if summary_rows:
    summary_df = spark.createDataFrame(
        summary_rows,
        "source_table STRING, rejection_reason STRING, record_count BIGINT"
    ).orderBy(F.col("record_count").desc())

    print("\n=== Steward review queue — per-rule summary ===")
    summary_df.show(50, truncate=False)

    total_waiting = sum(r[2] for r in summary_rows)
    print(f"\nTotal records awaiting review: {total_waiting}")
else:
    print("No quarantined records found across any table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 4 — REVIEW QUEUE: RECENT 50 RECORDS ==========
# MAGIC
# MAGIC The 100-foot view: actual sample records. Useful for picking specific
# MAGIC `(source_table, record_key)` pairs to inspect.

# COMMAND ----------

# Build a unioned, narrowed-down view of all quarantine tables for browsing.
# We project a small set of common-ish columns for display: source_table, key,
# rejection_reason, source_file, load_timestamp.

queue_rows = []
for q_table in QUARANTINE_TABLES:
    fqn = f"{CATALOG}.silver.{q_table}"
    pk_col = PRIMARY_KEY_MAP.get(q_table)
    if not pk_col:
        continue
    try:
        df = spark.table(fqn)
        # Use a defensive cast to string for record_key (sales_id is int, etc.)
        narrow = (
            df.select(
                F.lit(q_table).alias("source_table"),
                F.col(pk_col).cast("string").alias("record_key"),
                F.col("_rejection_reason").alias("rejection_reason"),
                F.col("_source_file").alias("source_file"),
                F.col("_load_timestamp").alias("load_timestamp"),
            )
        )
        queue_rows.append(narrow)
    except Exception as e:
        print(f"  (skipping {fqn}: {e.__class__.__name__})")

if queue_rows:
    queue_df = queue_rows[0]
    for q in queue_rows[1:]:
        queue_df = queue_df.unionByName(q)

    queue_df = queue_df.orderBy(F.col("load_timestamp").desc())

    print("\n=== Steward review queue — most recent 50 records ===")
    queue_df.show(50, truncate=False)
else:
    print("No quarantined records to review.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 5 — INSPECT A SPECIFIC RECORD ==========
# MAGIC
# MAGIC Set the two `inspect_*` widgets and run this cell to see the full row plus
# MAGIC any prior resolution history. This is the cell you'll come back to most.

# COMMAND ----------

dbutils.widgets.dropdown(
    "inspect_source_table",
    "quarantine_customers",
    QUARANTINE_TABLES,
    "Inspect: source table"
)
dbutils.widgets.text("inspect_record_key", "", "Inspect: record key")

inspect_table = dbutils.widgets.get("inspect_source_table")
inspect_key   = dbutils.widgets.get("inspect_record_key").strip()

if not inspect_key:
    print("Set the 'Inspect: record key' widget to look up a record.")
else:
    pk_col = PRIMARY_KEY_MAP[inspect_table]
    fqn = f"{CATALOG}.silver.{inspect_table}"
    print(f"Inspecting {fqn} where {pk_col} = '{inspect_key}'\n")

    try:
        # Cast the column to string for comparison so the widget input always matches
        record = spark.table(fqn).filter(F.col(pk_col).cast("string") == inspect_key)
        n = record.count()
        if n == 0:
            print(f"No record found.")
        else:
            print(f"Found {n} matching row(s):")
            record.show(truncate=False, vertical=True)

            print("\nPrior resolution history (if any):")
            spark.sql(f"""
                SELECT path_taken, final_status, reason_code, resolved_by, resolved_at, notes
                FROM {RESOLUTIONS_TABLE}
                WHERE source_table = '{inspect_table}'
                  AND record_key   = '{inspect_key}'
                ORDER BY created_at DESC
            """).show(truncate=False)

            print("Prior repair history (if any):")
            spark.sql(f"""
                SELECT action, repaired_by, repaired_at, notes
                FROM {REPAIRED_TABLE}
                WHERE source_table = '{inspect_table}'
                  AND record_key   = '{inspect_key}'
                ORDER BY repaired_at DESC
            """).show(truncate=False)
    except Exception as e:
        print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 6 — TAKE ACTION ON A RECORD ==========
# MAGIC
# MAGIC Set the `action_*` widgets, then run this cell. The cell:
# MAGIC 1. Validates inputs
# MAGIC 2. For repair_and_promote: parses `action_repair_json` as a dict of {col: value}
# MAGIC    and merges with the original quarantine row to produce the repaired payload
# MAGIC 3. Writes to `steward_repaired_records` (for approve_as_is and repair_and_promote)
# MAGIC 4. Logs the decision to `quarantine_resolutions` via `log_resolution`
# MAGIC
# MAGIC **Safety**: this cell does NOT delete from the quarantine table. The original
# MAGIC quarantine row stays put for audit. The sidecar table is the place where
# MAGIC promoted records live.

# COMMAND ----------

dbutils.widgets.dropdown(
    "action_source_table",
    "quarantine_customers",
    QUARANTINE_TABLES,
    "Action: source table"
)
dbutils.widgets.text("action_record_key", "", "Action: record key")
dbutils.widgets.dropdown(
    "action_decision",
    "approve_as_is",
    ["approve_as_is", "repair_and_promote", "permanently_reject", "escalate"],
    "Action: decision"
)
dbutils.widgets.text(
    "action_repair_json",
    "",
    "Action: JSON of fields to overwrite (only for repair_and_promote)"
)
dbutils.widgets.text("action_reason_code", "", "Action: reason code")
dbutils.widgets.text("action_notes", "", "Action: notes (free text)")
dbutils.widgets.dropdown(
    "execute_action", "false", ["false", "true"], "EXECUTE? (set true to actually run)"
)

action_table   = dbutils.widgets.get("action_source_table")
action_key     = dbutils.widgets.get("action_record_key").strip()
decision       = dbutils.widgets.get("action_decision")
repair_json    = dbutils.widgets.get("action_repair_json").strip()
reason_code    = dbutils.widgets.get("action_reason_code").strip() or None
notes          = dbutils.widgets.get("action_notes").strip() or None
execute        = dbutils.widgets.get("execute_action").lower() == "true"

print(f"=== Action plan ===")
print(f"  Source table: {action_table}")
print(f"  Record key:   {action_key}")
print(f"  Decision:     {decision}")
print(f"  Reason code:  {reason_code}")
print(f"  Notes:        {notes}")
print(f"  Steward:      {STEWARD_EMAIL}")
print(f"  EXECUTE:      {execute}")
print()

if not action_key:
    print("ABORT: action_record_key is empty.")
elif decision == "repair_and_promote" and not repair_json:
    print("ABORT: repair_and_promote requires action_repair_json.")
elif not execute:
    print("DRY RUN. Set EXECUTE? = true to commit this action.")
else:
    pk_col = PRIMARY_KEY_MAP[action_table]
    fqn = f"{CATALOG}.silver.{action_table}"

    # Fetch the original row
    original = spark.table(fqn).filter(F.col(pk_col).cast("string") == action_key).limit(1).collect()
    if not original:
        print(f"ABORT: no record found in {fqn} where {pk_col} = '{action_key}'")
    else:
        original_row = original[0]
        original_dict = original_row.asDict()
        rejection_reason_value = original_dict.get("_rejection_reason")

        import json
        original_payload_str = json.dumps(original_dict, default=str)

        # Branch by decision
        if decision in ("approve_as_is", "repair_and_promote"):
            repaired_payload_str = None
            if decision == "repair_and_promote":
                try:
                    overrides = json.loads(repair_json)
                except Exception as e:
                    raise ValueError(f"action_repair_json is not valid JSON: {e}")
                if not isinstance(overrides, dict):
                    raise ValueError("action_repair_json must be a JSON object {col: value}")
                merged = dict(original_dict)
                merged.update(overrides)
                repaired_payload_str = json.dumps(merged, default=str)

            # Insert into the sidecar table
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType
            from datetime import datetime
            now = datetime.utcnow()

            repair_schema = StructType([
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
            repair_row = [(
                action_table,
                action_key,
                decision,
                original_payload_str,
                repaired_payload_str,
                rejection_reason_value,
                notes,
                STEWARD_EMAIL,
                now,
            )]
            (
                spark.createDataFrame(repair_row, repair_schema)
                     .write.format("delta").mode("append").saveAsTable(REPAIRED_TABLE)
            )
            print(f"  -> Wrote 1 row to {REPAIRED_TABLE}")

            log_resolution(
                source_table=action_table,
                record_key=action_key,
                rule_name=None,
                path_taken="manual_repair",
                final_status="resolved",
                reason_code=reason_code or "STEWARD_APPROVED",
                resolved_by=STEWARD_EMAIL,
                affected_records=1,
                notes=notes,
            )
            print(f"  -> Logged manual_repair / resolved in {RESOLUTIONS_TABLE}")

        elif decision == "permanently_reject":
            log_resolution(
                source_table=action_table,
                record_key=action_key,
                rule_name=None,
                path_taken="permanent_reject",
                final_status="rejected",
                reason_code=reason_code or "STEWARD_REJECTED",
                resolved_by=STEWARD_EMAIL,
                affected_records=1,
                notes=notes,
            )
            print(f"  -> Logged permanent_reject / rejected in {RESOLUTIONS_TABLE}")

        elif decision == "escalate":
            log_resolution(
                source_table=action_table,
                record_key=action_key,
                rule_name=None,
                path_taken="manual_repair",
                final_status="in_progress",
                reason_code=reason_code or "STEWARD_ESCALATED",
                resolved_by=STEWARD_EMAIL,
                affected_records=1,
                notes=notes,
            )
            print(f"  -> Logged manual_repair / in_progress in {RESOLUTIONS_TABLE}")

        else:
            print(f"ABORT: unknown decision '{decision}'")

        print("\nDone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 7 — VERIFY RECENT ACTIVITY ==========

# COMMAND ----------

print(f"Most recent 10 resolutions logged by stewards:")
spark.sql(f"""
    SELECT source_table, record_key, path_taken, final_status, reason_code,
           resolved_by, created_at, notes
    FROM {RESOLUTIONS_TABLE}
    WHERE resolved_by NOT IN ('system')
    ORDER BY created_at DESC
    LIMIT 10
""").show(truncate=False)

print(f"\nMost recent 10 records in {REPAIRED_TABLE}:")
spark.sql(f"""
    SELECT source_table, record_key, action, repaired_by, repaired_at, notes
    FROM {REPAIRED_TABLE}
    ORDER BY repaired_at DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use this notebook
# MAGIC
# MAGIC 1. **Open** the notebook in the workspace and attach to a cluster.
# MAGIC 2. **Set your email**: top widget `steward_email`. This goes into every audit row.
# MAGIC 3. **Run cells 1–4**: imports, sidecar DDL, queue summary, recent records.
# MAGIC 4. **Pick a record to look at**: set `inspect_source_table` and
# MAGIC    `inspect_record_key` widgets, run cell 5. You'll see the full row plus any
# MAGIC    prior history.
# MAGIC 5. **Take an action**: set the `action_*` widgets:
# MAGIC    - `action_source_table` and `action_record_key`: which record
# MAGIC    - `action_decision`: one of approve_as_is / repair_and_promote / permanently_reject / escalate
# MAGIC    - `action_repair_json`: only for repair_and_promote — a JSON object of fields to overwrite,
# MAGIC      e.g. `{"region": "West"}`
# MAGIC    - `action_reason_code`: short tag like `STEWARD_FIX` or `UNRECOVERABLE`
# MAGIC    - `action_notes`: free text
# MAGIC    - `execute_action`: leave at `false` for a dry run; set to `true` to actually commit
# MAGIC    Run cell 6.
# MAGIC 6. **Verify**: cell 7 shows the most recent steward activity.
# MAGIC 7. **Repeat** for the next record.
# MAGIC
# MAGIC ## Phase 2 path
# MAGIC
# MAGIC When Lakebase lands, the same logic moves behind a real web app:
# MAGIC - Cells 3–4 become a queue list view
# MAGIC - Cell 5 becomes a record detail page
# MAGIC - Cell 6's branches become buttons (Approve / Repair / Reject / Escalate)
# MAGIC - The repaired records sidecar moves into a Lakebase OLTP table with row-level
# MAGIC   locking so two stewards can't act on the same record concurrently
# MAGIC
# MAGIC The schemas, the audit logging, and the four-decision branching all carry over
# MAGIC unchanged. Only the UI changes.
