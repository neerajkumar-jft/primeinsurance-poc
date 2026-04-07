# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — Metrics Rollup for Dashboarding
# MAGIC
# MAGIC **Problem**: the quality resolution loop has produced four Silver tables
# MAGIC (`dq_issues`, `dq_issues_history`, `dq_trend_metrics`, `dq_volume_metrics`,
# MAGIC `dq_trend_alerts`, `quarantine_resolutions`). They're each useful in isolation
# MAGIC but a dashboard tile needs a single, denormalized, "as of today" rollup. This
# MAGIC notebook produces three Gold tables sized for direct dashboard binding.
# MAGIC
# MAGIC **Output tables**:
# MAGIC
# MAGIC | Table | Shape | Drives |
# MAGIC |-------|-------|--------|
# MAGIC | `gold.dq_metrics_daily`        | one row per day, idempotent upsert | trend tiles ("last 30 days") |
# MAGIC | `gold.dq_quarantine_by_table`  | one row per Silver table, overwritten each run | "current state" tile |
# MAGIC | `gold.dq_resolutions_summary`  | one row per resolution path, overwritten each run | "resolution health" tile |
# MAGIC
# MAGIC **Idempotency**: `dq_metrics_daily` uses a DELETE+INSERT pattern keyed on
# MAGIC `snapshot_date`, so re-running on the same calendar day overwrites that day's
# MAGIC row instead of appending duplicates.
# MAGIC
# MAGIC **Dependencies**: this notebook reads from the four guardrail tables produced by
# MAGIC notebooks 30, 31, and 36. It is tolerant of missing tables — if a guardrail
# MAGIC notebook hasn't run yet, the corresponding metric is set to NULL or 0 rather
# MAGIC than failing.
# MAGIC
# MAGIC **Zero impact on Phase 1**: read-only on all sources. Writes only to three new
# MAGIC tables in the existing gold schema.

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

dbutils.widgets.text("catalog", "primeins", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")

# Source tables (silver)
DQ_ISSUES_TABLE       = f"{CATALOG}.silver.dq_issues"
HISTORY_TABLE         = f"{CATALOG}.silver.dq_issues_history"
TREND_METRICS_TABLE   = f"{CATALOG}.silver.dq_trend_metrics"
VOLUME_METRICS_TABLE  = f"{CATALOG}.silver.dq_volume_metrics"
ALERTS_TABLE          = f"{CATALOG}.silver.dq_trend_alerts"
RESOLUTIONS_TABLE     = f"{CATALOG}.silver.quarantine_resolutions"

# Output tables (gold)
DAILY_TABLE           = f"{CATALOG}.gold.dq_metrics_daily"
QUARANTINE_TABLE      = f"{CATALOG}.gold.dq_quarantine_by_table"
RESOLUTIONS_SUMMARY   = f"{CATALOG}.gold.dq_resolutions_summary"

print(f"Catalog:               {CATALOG}")
print(f"Sources:")
for t in [DQ_ISSUES_TABLE, HISTORY_TABLE, TREND_METRICS_TABLE, VOLUME_METRICS_TABLE, ALERTS_TABLE, RESOLUTIONS_TABLE]:
    print(f"  - {t}")
print(f"Outputs:")
for t in [DAILY_TABLE, QUARANTINE_TABLE, RESOLUTIONS_SUMMARY]:
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== HELPERS ==========

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType,
    TimestampType, DateType, IntegerType,
)
from datetime import datetime, date
from typing import Optional


def safe_count(table_fqn: str, where: Optional[str] = None) -> int:
    """COUNT(*) that returns 0 if the table doesn't exist or is empty."""
    try:
        df = spark.table(table_fqn)
        if where:
            df = df.filter(where)
        return df.count()
    except Exception as e:
        print(f"  (table not found, treating as 0: {table_fqn})")
        return 0


def safe_sum(table_fqn: str, column: str, where: Optional[str] = None) -> int:
    """SUM that returns 0 if the table doesn't exist."""
    try:
        df = spark.table(table_fqn)
        if where:
            df = df.filter(where)
        v = df.agg(F.sum(column)).collect()[0][0]
        return int(v) if v is not None else 0
    except Exception:
        print(f"  (table not found, treating as 0: {table_fqn})")
        return 0


def safe_max(table_fqn: str, column: str, where: Optional[str] = None) -> Optional[float]:
    """MAX that returns None if the table doesn't exist."""
    try:
        df = spark.table(table_fqn)
        if where:
            df = df.filter(where)
        v = df.agg(F.max(column)).collect()[0][0]
        return float(v) if v is not None else None
    except Exception:
        return None


def safe_avg(table_fqn: str, column: str, where: Optional[str] = None) -> Optional[float]:
    """AVG that returns None if the table doesn't exist."""
    try:
        df = spark.table(table_fqn)
        if where:
            df = df.filter(where)
        v = df.agg(F.avg(column)).collect()[0][0]
        return float(v) if v is not None else None
    except Exception:
        return None


def table_exists(table_fqn: str) -> bool:
    try:
        spark.table(table_fqn).limit(1).collect()
        return True
    except Exception:
        return False


computed_at = datetime.utcnow()
snapshot_date = computed_at.date()
print(f"Computed at:    {computed_at}")
print(f"Snapshot date:  {snapshot_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== TABLE 1 — gold.dq_metrics_daily ==========
# MAGIC
# MAGIC One row per day. Wide schema, suitable for binding directly to Lakeview tiles
# MAGIC showing trend lines.
# MAGIC
# MAGIC | Field | Source | Meaning |
# MAGIC |-------|--------|---------|
# MAGIC | `snapshot_date` | computed | Date of the rollup |
# MAGIC | `total_quarantine_records` | dq_issues | Sum of affected_records across all rules |
# MAGIC | `total_dq_rules_triggered` | dq_issues | Number of distinct (table, rule) pairs with hits |
# MAGIC | `max_volume_pct` | dq_volume_metrics | Worst quarantine percentage across tables |
# MAGIC | `tables_breached_count` | dq_volume_metrics | How many tables breached their threshold |
# MAGIC | `open_alerts_count` | dq_trend_alerts | Currently-open alerts (volume + trend combined) |
# MAGIC | `new_alerts_today` | dq_trend_alerts | Alerts created on this snapshot date |
# MAGIC | `resolutions_today` | quarantine_resolutions | Triage decisions made today |
# MAGIC | `cumulative_resolutions` | quarantine_resolutions | Lifetime decisions |
# MAGIC | `false_positive_rate` | quarantine_resolutions | rule_fix decisions / total decisions |
# MAGIC | `avg_open_alert_age_hours` | dq_trend_alerts | Mean age of open alerts (graveyard signal) |

# COMMAND ----------

# Create table on first run
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DAILY_TABLE} (
    snapshot_date              DATE      NOT NULL,
    computed_at                TIMESTAMP NOT NULL,
    total_quarantine_records   BIGINT,
    total_dq_rules_triggered   BIGINT,
    max_volume_pct             DOUBLE,
    tables_breached_count      BIGINT,
    open_alerts_count          BIGINT,
    new_alerts_today           BIGINT,
    resolutions_today          BIGINT,
    cumulative_resolutions     BIGINT,
    false_positive_rate        DOUBLE,
    avg_open_alert_age_hours   DOUBLE
)
USING DELTA
COMMENT 'Daily quality rollup, one row per snapshot_date. Idempotent upsert by snapshot_date.'
""")

# Compute every metric. Each safe_* helper handles missing source tables gracefully.
total_quarantine_records = safe_sum(DQ_ISSUES_TABLE, "affected_records")

total_dq_rules_triggered = 0
if table_exists(DQ_ISSUES_TABLE):
    total_dq_rules_triggered = (
        spark.table(DQ_ISSUES_TABLE)
             .filter("affected_records > 0")
             .select("table_name", "rule_name").distinct().count()
    )

max_volume_pct        = safe_max(VOLUME_METRICS_TABLE, "quarantine_pct")
tables_breached_count = safe_count(VOLUME_METRICS_TABLE, "breached = TRUE")

open_alerts_count = safe_count(ALERTS_TABLE, "status = 'open'")
new_alerts_today  = safe_count(
    ALERTS_TABLE, f"status = 'open' AND CAST(alert_at AS DATE) = DATE('{snapshot_date}')"
)

resolutions_today = safe_count(
    RESOLUTIONS_TABLE, f"CAST(created_at AS DATE) = DATE('{snapshot_date}')"
)
cumulative_resolutions = safe_count(RESOLUTIONS_TABLE)

false_positive_rate = None
if table_exists(RESOLUTIONS_TABLE) and cumulative_resolutions > 0:
    fp = safe_count(RESOLUTIONS_TABLE, "path_taken = 'rule_fix'")
    false_positive_rate = round(fp / cumulative_resolutions, 4)

avg_open_alert_age_hours = None
if table_exists(ALERTS_TABLE) and open_alerts_count > 0:
    avg_open_alert_age_hours = (
        spark.table(ALERTS_TABLE)
             .filter("status = 'open'")
             .select(
                 (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("alert_at")))
                 .alias("age_seconds")
             )
             .agg(F.avg("age_seconds")).collect()[0][0]
    )
    if avg_open_alert_age_hours is not None:
        avg_open_alert_age_hours = round(avg_open_alert_age_hours / 3600.0, 2)

print(f"\nDaily metrics for {snapshot_date}:")
print(f"  total_quarantine_records:  {total_quarantine_records}")
print(f"  total_dq_rules_triggered:  {total_dq_rules_triggered}")
print(f"  max_volume_pct:            {max_volume_pct}")
print(f"  tables_breached_count:     {tables_breached_count}")
print(f"  open_alerts_count:         {open_alerts_count}")
print(f"  new_alerts_today:          {new_alerts_today}")
print(f"  resolutions_today:         {resolutions_today}")
print(f"  cumulative_resolutions:    {cumulative_resolutions}")
print(f"  false_positive_rate:       {false_positive_rate}")
print(f"  avg_open_alert_age_hours:  {avg_open_alert_age_hours}")

# COMMAND ----------

# Idempotent upsert: delete today's row, then insert. Equivalent to MERGE on snapshot_date
# but avoids the need for the merge key feature.

spark.sql(f"DELETE FROM {DAILY_TABLE} WHERE snapshot_date = DATE('{snapshot_date}')")

daily_schema = StructType([
    StructField("snapshot_date",            DateType(),      False),
    StructField("computed_at",              TimestampType(), False),
    StructField("total_quarantine_records", LongType(),      True),
    StructField("total_dq_rules_triggered", LongType(),      True),
    StructField("max_volume_pct",           DoubleType(),    True),
    StructField("tables_breached_count",    LongType(),      True),
    StructField("open_alerts_count",        LongType(),      True),
    StructField("new_alerts_today",         LongType(),      True),
    StructField("resolutions_today",        LongType(),      True),
    StructField("cumulative_resolutions",   LongType(),      True),
    StructField("false_positive_rate",      DoubleType(),    True),
    StructField("avg_open_alert_age_hours", DoubleType(),    True),
])

daily_row = [(
    snapshot_date,
    computed_at,
    int(total_quarantine_records) if total_quarantine_records is not None else None,
    int(total_dq_rules_triggered) if total_dq_rules_triggered is not None else None,
    max_volume_pct,
    int(tables_breached_count) if tables_breached_count is not None else None,
    int(open_alerts_count) if open_alerts_count is not None else None,
    int(new_alerts_today) if new_alerts_today is not None else None,
    int(resolutions_today) if resolutions_today is not None else None,
    int(cumulative_resolutions) if cumulative_resolutions is not None else None,
    false_positive_rate,
    avg_open_alert_age_hours,
)]

spark.createDataFrame(daily_row, daily_schema) \
     .write.format("delta").mode("append").saveAsTable(DAILY_TABLE)

print(f"\nUpserted today's row into {DAILY_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== TABLE 2 — gold.dq_quarantine_by_table ==========
# MAGIC
# MAGIC Current snapshot, one row per Silver table. Drives the "right now" tile.
# MAGIC Overwritten each run. If `dq_volume_metrics` doesn't exist yet, this table is
# MAGIC created empty so the dashboard binding doesn't break.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
    table_name        STRING NOT NULL,
    clean_count       BIGINT,
    quarantine_count  BIGINT,
    total_count       BIGINT,
    quarantine_pct    DOUBLE,
    threshold_pct     DOUBLE,
    breached          BOOLEAN,
    excluded          BOOLEAN,
    last_alert_at     TIMESTAMP,
    open_alerts       BIGINT,
    computed_at       TIMESTAMP
)
USING DELTA
COMMENT 'Current quarantine snapshot per Silver table. Overwritten each rollup run.'
""")

if table_exists(VOLUME_METRICS_TABLE):
    volume = spark.table(VOLUME_METRICS_TABLE)

    # Per-table alert summary: latest open alert + count of open alerts
    if table_exists(ALERTS_TABLE):
        alerts_per_table = (
            spark.table(ALERTS_TABLE)
                 .filter("status = 'open'")
                 .groupBy("table_name")
                 .agg(
                     F.max("alert_at").alias("last_alert_at"),
                     F.count("*").cast("long").alias("open_alerts"),
                 )
        )
    else:
        alerts_per_table = spark.createDataFrame(
            [], "table_name STRING, last_alert_at TIMESTAMP, open_alerts BIGINT"
        )

    quarantine_snapshot = (
        volume.alias("v")
              .join(alerts_per_table.alias("a"), on="table_name", how="left")
              .select(
                  F.col("v.table_name"),
                  F.col("v.clean_count"),
                  F.col("v.quarantine_count"),
                  F.col("v.total_count"),
                  F.col("v.quarantine_pct"),
                  F.col("v.threshold_pct"),
                  F.col("v.breached"),
                  F.col("v.excluded"),
                  F.col("a.last_alert_at"),
                  F.coalesce(F.col("a.open_alerts"), F.lit(0).cast("long")).alias("open_alerts"),
                  F.lit(computed_at).alias("computed_at"),
              )
    )

    (
        quarantine_snapshot.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )

    print(f"\nWrote {quarantine_snapshot.count()} rows to {QUARANTINE_TABLE}")
    quarantine_snapshot.orderBy(F.col("quarantine_pct").desc()).show(truncate=False)
else:
    print(f"\nWARNING: {VOLUME_METRICS_TABLE} not found — has notebook 31 been run?")
    print(f"{QUARANTINE_TABLE} created empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== TABLE 3 — gold.dq_resolutions_summary ==========
# MAGIC
# MAGIC One row per resolution path. Drives the "resolution health" tile showing how
# MAGIC many decisions have been made and what the breakdown looks like.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESOLUTIONS_SUMMARY} (
    path_taken         STRING NOT NULL,
    decision_count     BIGINT,
    records_affected   BIGINT,
    latest_decision_at TIMESTAMP,
    pct_of_decisions   DOUBLE,
    computed_at        TIMESTAMP
)
USING DELTA
COMMENT 'Resolution path rollup. Overwritten each rollup run.'
""")

if table_exists(RESOLUTIONS_TABLE) and cumulative_resolutions > 0:
    summary = (
        spark.table(RESOLUTIONS_TABLE)
             .groupBy("path_taken")
             .agg(
                 F.count("*").cast("long").alias("decision_count"),
                 F.sum("affected_records").cast("long").alias("records_affected"),
                 F.max("created_at").alias("latest_decision_at"),
             )
             .withColumn(
                 "pct_of_decisions",
                 F.round(F.col("decision_count") / F.lit(cumulative_resolutions) * 100.0, 2)
             )
             .withColumn("computed_at", F.lit(computed_at))
             .select(
                 "path_taken", "decision_count", "records_affected",
                 "latest_decision_at", "pct_of_decisions", "computed_at",
             )
    )

    (
        summary.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(RESOLUTIONS_SUMMARY)
    )

    print(f"\nWrote {summary.count()} rows to {RESOLUTIONS_SUMMARY}")
    summary.orderBy(F.col("decision_count").desc()).show(truncate=False)
else:
    print(f"\nNo resolutions yet — {RESOLUTIONS_SUMMARY} created empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 4 — RUN SUMMARY ==========

# COMMAND ----------

print("=== Metrics rollup complete ===\n")

print(f"gold.dq_metrics_daily — last 7 days:")
spark.sql(f"""
    SELECT snapshot_date, total_quarantine_records, max_volume_pct,
           tables_breached_count, open_alerts_count, resolutions_today,
           cumulative_resolutions, false_positive_rate, avg_open_alert_age_hours
    FROM {DAILY_TABLE}
    WHERE snapshot_date >= current_date() - INTERVAL 7 DAYS
    ORDER BY snapshot_date DESC
""").show(truncate=False)

print(f"\ngold.dq_quarantine_by_table:")
spark.sql(f"SELECT * FROM {QUARANTINE_TABLE} ORDER BY quarantine_pct DESC").show(truncate=False)

print(f"\ngold.dq_resolutions_summary:")
spark.sql(f"SELECT * FROM {RESOLUTIONS_SUMMARY} ORDER BY decision_count DESC").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to wire this up
# MAGIC
# MAGIC 1. **Schedule**: run after notebooks 30 and 31 so the daily snapshot reflects
# MAGIC    the latest trend and volume metrics. The DAB job `quality_metrics_rollup`
# MAGIC    can be chained downstream of those, or scheduled independently on a daily
# MAGIC    cadence.
# MAGIC
# MAGIC 2. **Lakeview dashboard**: bind the three Gold tables to dashboard tiles —
# MAGIC    - Trend tile: `gold.dq_metrics_daily` (snapshot_date as x-axis,
# MAGIC      quarantine count / open alerts as y-axes)
# MAGIC    - Current state tile: `gold.dq_quarantine_by_table` (one row per Silver
# MAGIC      table with red/green status based on `breached`)
# MAGIC    - Resolution health tile: `gold.dq_resolutions_summary` (donut chart by
# MAGIC      path_taken)
# MAGIC
# MAGIC 3. **Genie**: add the three Gold tables to the compliance Genie space context.
# MAGIC    Sample questions to seed:
# MAGIC    - "How many quarantine records are open today?"
# MAGIC    - "What's our false positive rate this month?"
# MAGIC    - "Which tables are over their volume threshold right now?"
# MAGIC    - "How many records have we permanently rejected vs source-fixed?"
