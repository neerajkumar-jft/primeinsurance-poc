# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — DQ Trend Alerting (Guardrail 1)
# MAGIC
# MAGIC **Problem**: `silver.dq_issues` is recomputed by the Silver DLT pipeline on every
# MAGIC run and only reflects the most recent state. We have no history, so we cannot tell
# MAGIC if quarantine for a given table is suddenly growing — the kind of signal that
# MAGIC almost always means a source system change.
# MAGIC
# MAGIC **Solution**: a small standalone notebook that runs after each Silver pipeline run
# MAGIC and does three things:
# MAGIC 1. Snapshots the current `silver.dq_issues` rows into `silver.dq_issues_history`
# MAGIC 2. Compares this snapshot to the snapshot from ~7 days ago and computes a
# MAGIC    growth ratio per (table_name, rule_name)
# MAGIC 3. Writes any (table, rule) pair whose growth exceeded the configured threshold
# MAGIC    (default 2.0× week-over-week) to `silver.dq_trend_alerts`
# MAGIC
# MAGIC `silver.dq_trend_alerts` can be wired to a Databricks SQL Alert
# MAGIC (`SELECT COUNT(*) FROM silver.dq_trend_alerts WHERE status='open'`) which pages
# MAGIC the on-call data engineer when any alert fires.
# MAGIC
# MAGIC **Inputs**:  `{catalog}.silver.dq_issues`
# MAGIC **Outputs**: `{catalog}.silver.dq_issues_history`, `{catalog}.silver.dq_trend_metrics`,
# MAGIC             `{catalog}.silver.dq_trend_alerts`
# MAGIC
# MAGIC **Zero impact on Phase 1**: this notebook only reads from `silver.dq_issues`. It
# MAGIC writes to three new tables in the existing silver schema. The Silver pipeline
# MAGIC itself is untouched. Safe to schedule independently or as a downstream task in the
# MAGIC end-to-end workflow.

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

dbutils.widgets.text("catalog", "primeins", "Unity Catalog")
dbutils.widgets.text("threshold", "2.0", "Growth ratio threshold (e.g. 2.0 = 2x week-over-week)")
dbutils.widgets.text("comparison_lookback_days", "7", "How many days back to compare against")
dbutils.widgets.text("min_records_floor", "10", "Skip alerting on rules with fewer current records than this")

CATALOG = dbutils.widgets.get("catalog")
THRESHOLD = float(dbutils.widgets.get("threshold"))
LOOKBACK_DAYS = int(dbutils.widgets.get("comparison_lookback_days"))
MIN_FLOOR = int(dbutils.widgets.get("min_records_floor"))

DQ_ISSUES_TABLE   = f"{CATALOG}.silver.dq_issues"
HISTORY_TABLE     = f"{CATALOG}.silver.dq_issues_history"
METRICS_TABLE     = f"{CATALOG}.silver.dq_trend_metrics"
ALERTS_TABLE      = f"{CATALOG}.silver.dq_trend_alerts"

print(f"Catalog:                    {CATALOG}")
print(f"Source:                     {DQ_ISSUES_TABLE}")
print(f"History:                    {HISTORY_TABLE}")
print(f"Metrics:                    {METRICS_TABLE}")
print(f"Alerts:                     {ALERTS_TABLE}")
print(f"Growth threshold:           {THRESHOLD}x")
print(f"Lookback (days):            {LOOKBACK_DAYS}")
print(f"Min records floor:          {MIN_FLOOR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== TABLE DDL ==========

# COMMAND ----------

# History table — append-only snapshots of dq_issues over time
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    snapshot_at       TIMESTAMP NOT NULL,
    table_name        STRING,
    column_name       STRING,
    rule_name         STRING,
    rule_condition    STRING,
    action_taken      STRING,
    severity          STRING,
    affected_records  INT,
    affected_ratio    DOUBLE,
    suggested_fix     STRING,
    detected_at       TIMESTAMP
)
USING DELTA
COMMENT 'Append-only snapshots of silver.dq_issues. Used for week-over-week trend computation.'
""")

# Metrics table — overwritten on each run with the latest week-over-week comparison
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
    computed_at       TIMESTAMP NOT NULL,
    table_name        STRING,
    rule_name         STRING,
    current_count     INT,
    prior_count       INT,
    delta             INT,
    growth_ratio      DOUBLE,
    breached          BOOLEAN
)
USING DELTA
COMMENT 'Latest week-over-week growth metrics per (table, rule). Recomputed each run.'
""")

# Alerts table — append-only, status-tracked. Each new alert is a new row.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ALERTS_TABLE} (
    alert_id          BIGINT GENERATED ALWAYS AS IDENTITY,
    alert_at          TIMESTAMP NOT NULL,
    table_name        STRING,
    rule_name         STRING,
    current_count     INT,
    prior_count       INT,
    growth_ratio      DOUBLE,
    threshold         DOUBLE,
    severity          STRING,
    message           STRING,
    status            STRING NOT NULL  COMMENT 'open | acknowledged | resolved — set explicitly by writers'
)
USING DELTA
COMMENT 'Open trend alerts. Wire a Databricks SQL Alert to: SELECT COUNT(*) FROM this WHERE status=open.'
-- Note: status uses no DEFAULT clause because that Delta feature is not enabled in this
-- workspace. The alert insert below explicitly sets status='open' on every row.
""")

print("Created/verified: history, metrics, alerts tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 1 — SNAPSHOT CURRENT dq_issues ==========
# MAGIC
# MAGIC Idempotency note: we tag each snapshot with `current_timestamp()` at write time.
# MAGIC Re-running this cell within the same minute will create two near-identical
# MAGIC snapshots. That's harmless — the comparison logic uses the most recent snapshot
# MAGIC per (table, rule) anyway.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

snapshot_at = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

current_dq = spark.table(DQ_ISSUES_TABLE).withColumn("snapshot_at", F.lit(snapshot_at))

# Reorder columns to match HISTORY_TABLE schema exactly
current_dq.select(
    "snapshot_at",
    "table_name",
    "column_name",
    "rule_name",
    "rule_condition",
    "action_taken",
    "severity",
    "affected_records",
    "affected_ratio",
    "suggested_fix",
    "detected_at",
).write.format("delta").mode("append").saveAsTable(HISTORY_TABLE)

snapshot_count = current_dq.count()
print(f"Snapshot taken at {snapshot_at}: {snapshot_count} rows from dq_issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 2 — COMPUTE WEEK-OVER-WEEK METRICS ==========
# MAGIC
# MAGIC For each (table_name, rule_name):
# MAGIC - `current_count` = affected_records from the most recent snapshot
# MAGIC - `prior_count`   = affected_records from the most recent snapshot at least
# MAGIC                     `LOOKBACK_DAYS` days older
# MAGIC - `growth_ratio`  = current / prior (special-cased for prior=0)
# MAGIC
# MAGIC The two snapshots may not be exactly 7 days apart. We use "most recent that is at
# MAGIC least N days old" so the comparison is meaningful even if the job slipped a day.

# COMMAND ----------

history = spark.table(HISTORY_TABLE)

# Latest snapshot per (table_name, rule_name)
latest_window = Window.partitionBy("table_name", "rule_name").orderBy(F.col("snapshot_at").desc())
latest = (
    history
    .withColumn("rn", F.row_number().over(latest_window))
    .filter("rn = 1")
    .select(
        "table_name",
        "rule_name",
        F.col("affected_records").alias("current_count"),
        F.col("snapshot_at").alias("current_snapshot_at"),
        "severity",
    )
)

# Most recent snapshot per (table_name, rule_name) that is at least LOOKBACK_DAYS old
prior_cutoff = F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_DAYS} DAYS")
prior_window = Window.partitionBy("table_name", "rule_name").orderBy(F.col("snapshot_at").desc())
prior = (
    history
    .filter(F.col("snapshot_at") <= prior_cutoff)
    .withColumn("rn", F.row_number().over(prior_window))
    .filter("rn = 1")
    .select(
        "table_name",
        "rule_name",
        F.col("affected_records").alias("prior_count"),
        F.col("snapshot_at").alias("prior_snapshot_at"),
    )
)

metrics = (
    latest
    .join(prior, on=["table_name", "rule_name"], how="left")
    .withColumn("prior_count", F.coalesce(F.col("prior_count"), F.lit(0)))
    .withColumn("delta", F.col("current_count") - F.col("prior_count"))
    .withColumn(
        "growth_ratio",
        F.when(F.col("prior_count") > 0, F.col("current_count") / F.col("prior_count"))
         # If prior was 0 and current > 0, treat as infinite growth — sentinel value 999.0
         .when((F.col("prior_count") == 0) & (F.col("current_count") > 0), F.lit(999.0))
         .otherwise(F.lit(1.0))
    )
    .withColumn(
        "breached",
        (F.col("growth_ratio") >= THRESHOLD) & (F.col("current_count") >= MIN_FLOOR)
    )
    .withColumn("computed_at", F.current_timestamp())
    .select(
        "computed_at",
        "table_name",
        "rule_name",
        "current_count",
        "prior_count",
        "delta",
        "growth_ratio",
        "breached",
    )
)

# Overwrite metrics table — this is point-in-time, no need to append
(
    metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(METRICS_TABLE)
)

print(f"Wrote {metrics.count()} metric rows to {METRICS_TABLE}")
print("\nMetrics summary:")
metrics.orderBy(F.col("growth_ratio").desc()).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 3 — RAISE ALERTS FOR BREACHES ==========
# MAGIC
# MAGIC Only (table, rule) pairs that exceeded the threshold AND have at least
# MAGIC `MIN_FLOOR` current records get an alert. The floor prevents noise from rules
# MAGIC that legitimately have very few hits week to week.
# MAGIC
# MAGIC Alerts are append-only — each run that detects a breach inserts a new row. The
# MAGIC steward / on-call DE acknowledges by setting `status = 'acknowledged'` or
# MAGIC `'resolved'` later, which the SQL Alert query filters out.

# COMMAND ----------

# Re-read from the just-written metrics table (cleaner than carrying the dataframe)
breaches = (
    spark.table(METRICS_TABLE)
    .filter("breached = TRUE")
    .select(
        "table_name",
        "rule_name",
        "current_count",
        "prior_count",
        "growth_ratio",
    )
)

n_breaches = breaches.count()
print(f"Breaches detected this run: {n_breaches}")

if n_breaches > 0:
    alerts_to_insert = (
        breaches
        .withColumn("alert_at", F.current_timestamp())
        .withColumn("threshold", F.lit(THRESHOLD))
        .withColumn(
            "severity",
            F.when(F.col("growth_ratio") >= THRESHOLD * 2, F.lit("critical"))
             .when(F.col("growth_ratio") >= THRESHOLD,     F.lit("high"))
             .otherwise(F.lit("medium"))
        )
        .withColumn(
            "message",
            F.concat(
                F.lit("DQ trend alert: "),
                F.col("table_name"),
                F.lit(" / "),
                F.col("rule_name"),
                F.lit(" grew "),
                F.format_number(F.col("growth_ratio"), 2),
                F.lit("x in the last "),
                F.lit(str(LOOKBACK_DAYS)),
                F.lit(" days ("),
                F.col("prior_count").cast("string"),
                F.lit(" -> "),
                F.col("current_count").cast("string"),
                F.lit(" records)"),
            )
        )
        .withColumn("status", F.lit("open"))
        .select(
            "alert_at",
            "table_name",
            "rule_name",
            "current_count",
            "prior_count",
            "growth_ratio",
            "threshold",
            "severity",
            "message",
            "status",
        )
    )

    alerts_to_insert.write.format("delta").mode("append").saveAsTable(ALERTS_TABLE)

    print("\nAlerts inserted:")
    alerts_to_insert.show(truncate=False)
else:
    print("No alerts to insert — all (table, rule) pairs within threshold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 4 — RUN SUMMARY ==========

# COMMAND ----------

print(f"=== DQ Trend Alerting run summary ===")
print(f"Snapshot taken at:       {snapshot_at}")
print(f"Rows snapshotted:        {snapshot_count}")
print(f"Threshold:               {THRESHOLD}x  over {LOOKBACK_DAYS} days")
print(f"Min current records:     {MIN_FLOOR}")
print(f"Breaches this run:       {n_breaches}")

print(f"\nOpen alerts in {ALERTS_TABLE}:")
spark.sql(f"""
    SELECT COUNT(*) AS open_alerts
    FROM {ALERTS_TABLE}
    WHERE status = 'open'
""").show()

print("Most recent 5 open alerts:")
spark.sql(f"""
    SELECT alert_at, table_name, rule_name, growth_ratio, severity, message
    FROM {ALERTS_TABLE}
    WHERE status = 'open'
    ORDER BY alert_at DESC
    LIMIT 5
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to wire this up
# MAGIC
# MAGIC 1. **Schedule**: add this notebook as a downstream task in the
# MAGIC    `PrimeInsurance_End_to_End_Pipeline` workflow, depending on `silver_pipeline`.
# MAGIC    Or run it as its own daily job.
# MAGIC
# MAGIC 2. **SQL Alert**: create a Databricks SQL Alert with this query:
# MAGIC    ```sql
# MAGIC    SELECT COUNT(*) AS open_alerts
# MAGIC    FROM primeins.silver.dq_trend_alerts
# MAGIC    WHERE status = 'open'
# MAGIC    ```
# MAGIC    Alert when `open_alerts > 0`. Notify on-call data engineer via email or Slack.
# MAGIC
# MAGIC 3. **Acknowledgement**: when on-call has triaged an alert, mark it acknowledged:
# MAGIC    ```sql
# MAGIC    UPDATE primeins.silver.dq_trend_alerts
# MAGIC    SET status = 'acknowledged'
# MAGIC    WHERE alert_id IN (...)
# MAGIC    ```
# MAGIC
# MAGIC 4. **Resolution**: once the underlying issue is fixed, log it via the resolution
# MAGIC    helper from `36_dq_resolution_logger.py` and mark the alert resolved.
