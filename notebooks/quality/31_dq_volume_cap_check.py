# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — Volume Cap Check (Guardrail 2)
# MAGIC
# MAGIC **Problem**: trend alerting (notebook 30) catches *changes* in quarantine size, but
# MAGIC says nothing about absolute proportions. If a brand new source file lands with 90%
# MAGIC of its rows malformed, trend alerting won't fire on the first run because there is
# MAGIC no prior history to compare against — yet the Silver table for that source is now
# MAGIC missing the vast majority of its expected data. The pipeline ran "successfully"
# MAGIC and downstream consumers see a misleadingly clean Silver.
# MAGIC
# MAGIC **Solution**: a circuit-breaker check that runs after the Silver pipeline. For each
# MAGIC `(silver.<table>, silver.quarantine_<table>)` pair it computes the ratio
# MAGIC `quarantine / (clean + quarantine)`. If the ratio exceeds a configured threshold
# MAGIC for any table, the notebook fails with a non-zero exit so the workflow halts and
# MAGIC pages on-call.
# MAGIC
# MAGIC **Per-table thresholds**: a single global default (5%) is too strict for `sales`,
# MAGIC which has a known 62.9% padding-row problem (3,132 entirely empty rows out of 4,981
# MAGIC at source — see architecture.md). The widget `per_table_thresholds` lets you
# MAGIC override per table. Default override: `sales=70`.
# MAGIC
# MAGIC **Outputs**:
# MAGIC - `{catalog}.silver.dq_volume_metrics` — current ratio per table, overwritten each run
# MAGIC - `{catalog}.silver.dq_trend_alerts`   — append; reuses the same alerts table as
# MAGIC                                          notebook 30, so the same SQL Alert covers
# MAGIC                                          both guardrails
# MAGIC
# MAGIC **Behavior on breach**:
# MAGIC - Always: write the metric, insert an alert row with `severity = 'critical'`
# MAGIC - When `halt_on_breach = true` (default): raise an exception so the job task fails
# MAGIC - When `halt_on_breach = false`: log only, don't fail. Useful for first-time
# MAGIC   tuning when you want to observe ratios without risking pipeline halts.
# MAGIC
# MAGIC **Zero impact on Phase 1**: this notebook only reads from `silver.<table>` and
# MAGIC `silver.quarantine_<table>`. The Silver pipeline itself is untouched. Wired to
# MAGIC the existing end-to-end workflow only when explicitly added as a downstream task.

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

dbutils.widgets.text("catalog", "primeins", "Unity Catalog")
dbutils.widgets.text("default_threshold_pct", "5.0", "Default % threshold (e.g. 5.0 = 5%)")
dbutils.widgets.text(
    "per_table_thresholds",
    "sales=70.0",
    "Per-table overrides as comma-separated 'table=pct' (e.g. sales=70.0,claims=10.0)"
)
dbutils.widgets.text("exclude_tables", "", "Comma-separated tables to skip entirely")
dbutils.widgets.dropdown("halt_on_breach", "true", ["true", "false"], "Fail the job on breach?")

CATALOG = dbutils.widgets.get("catalog")
DEFAULT_THRESHOLD = float(dbutils.widgets.get("default_threshold_pct"))
HALT_ON_BREACH = dbutils.widgets.get("halt_on_breach").lower() == "true"

# Parse per-table overrides into a dict
overrides_str = dbutils.widgets.get("per_table_thresholds").strip()
PER_TABLE_THRESHOLDS = {}
if overrides_str:
    for pair in overrides_str.split(","):
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        try:
            PER_TABLE_THRESHOLDS[k.strip()] = float(v.strip())
        except ValueError:
            print(f"WARNING: bad threshold override '{pair}' — skipping")

# Parse excludes
excludes_str = dbutils.widgets.get("exclude_tables").strip()
EXCLUDE_TABLES = {t.strip() for t in excludes_str.split(",") if t.strip()}

# Hard-coded list of (clean_table, quarantine_table) pairs to check.
# Matches notebooks/silver/02_silver_dlt_pipeline.py.
TABLE_PAIRS = [
    ("customers", "quarantine_customers"),
    ("claims",    "quarantine_claims"),
    ("policy",    "quarantine_policy"),
    ("sales",     "quarantine_sales"),
    ("cars",      "quarantine_cars"),
]

METRICS_TABLE = f"{CATALOG}.silver.dq_volume_metrics"
ALERTS_TABLE  = f"{CATALOG}.silver.dq_trend_alerts"

print(f"Catalog:                 {CATALOG}")
print(f"Default threshold:       {DEFAULT_THRESHOLD}%")
print(f"Per-table overrides:     {PER_TABLE_THRESHOLDS}")
print(f"Excluded tables:         {EXCLUDE_TABLES if EXCLUDE_TABLES else '(none)'}")
print(f"Halt on breach:          {HALT_ON_BREACH}")
print(f"Pairs to check:          {TABLE_PAIRS}")
print(f"Metrics table:           {METRICS_TABLE}")
print(f"Alerts table:            {ALERTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== TABLE DDL ==========
# MAGIC
# MAGIC The metrics table is overwritten on every run with the latest snapshot. The
# MAGIC alerts table is shared with notebook 30 — created here as `IF NOT EXISTS` so
# MAGIC either notebook can run first.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
    computed_at        TIMESTAMP NOT NULL,
    table_name         STRING    NOT NULL,
    clean_count        BIGINT,
    quarantine_count   BIGINT,
    total_count        BIGINT,
    quarantine_pct     DOUBLE,
    threshold_pct      DOUBLE,
    breached           BOOLEAN,
    excluded           BOOLEAN
)
USING DELTA
COMMENT 'Latest quarantine volume snapshot per Silver table. Recomputed each run by 31_dq_volume_cap_check.'
""")

# Same alerts table as notebook 30 — create only if neither notebook has run yet
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
    status            STRING NOT NULL  COMMENT 'open | acknowledged | resolved'
)
USING DELTA
COMMENT 'Open trend and volume alerts. Wire a Databricks SQL Alert to: SELECT COUNT(*) FROM this WHERE status=open.'
""")

print("Created/verified: metrics table, alerts table (shared with notebook 30)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 1 — COMPUTE CURRENT VOLUME PER TABLE ==========

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType,
)
from datetime import datetime

computed_at = datetime.utcnow()
metric_rows = []

for clean_name, quarantine_name in TABLE_PAIRS:
    clean_fqn      = f"{CATALOG}.silver.{clean_name}"
    quarantine_fqn = f"{CATALOG}.silver.{quarantine_name}"

    excluded = clean_name in EXCLUDE_TABLES
    threshold = PER_TABLE_THRESHOLDS.get(clean_name, DEFAULT_THRESHOLD)

    try:
        clean_count = spark.table(clean_fqn).count()
    except Exception as e:
        print(f"WARNING: could not read {clean_fqn} — skipping. {e}")
        continue

    try:
        quarantine_count = spark.table(quarantine_fqn).count()
    except Exception as e:
        print(f"WARNING: could not read {quarantine_fqn} — assuming 0. {e}")
        quarantine_count = 0

    total = clean_count + quarantine_count
    pct = (quarantine_count / total * 100.0) if total > 0 else 0.0
    breached = (pct >= threshold) and not excluded

    metric_rows.append((
        computed_at,
        clean_name,
        clean_count,
        quarantine_count,
        total,
        round(pct, 4),
        threshold,
        breached,
        excluded,
    ))

    flag = "BREACH" if breached else ("excluded" if excluded else "ok")
    print(
        f"  {clean_name:12s}  clean={clean_count:>8d}  quar={quarantine_count:>8d}  "
        f"total={total:>8d}  pct={pct:6.2f}%  threshold={threshold:5.1f}%  -> {flag}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 2 — WRITE METRICS ==========

# COMMAND ----------

schema = StructType([
    StructField("computed_at",      TimestampType(), False),
    StructField("table_name",       StringType(),    False),
    StructField("clean_count",      LongType(),      True),
    StructField("quarantine_count", LongType(),      True),
    StructField("total_count",      LongType(),      True),
    StructField("quarantine_pct",   DoubleType(),    True),
    StructField("threshold_pct",    DoubleType(),    True),
    StructField("breached",         BooleanType(),   True),
    StructField("excluded",         BooleanType(),   True),
])

metrics_df = spark.createDataFrame(metric_rows, schema)

(
    metrics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(METRICS_TABLE)
)

print(f"\nWrote {metrics_df.count()} metric rows to {METRICS_TABLE}")
print("\nMetrics summary:")
metrics_df.orderBy(F.col("quarantine_pct").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 3 — RAISE ALERTS FOR BREACHES ==========
# MAGIC
# MAGIC Volume breaches always go in as `severity = 'critical'` because they imply that
# MAGIC a meaningful fraction of incoming data was rejected — the kind of thing that
# MAGIC should never happen silently.

# COMMAND ----------

breaches = [r for r in metric_rows if r[7]]   # index 7 = breached
n_breaches = len(breaches)

print(f"Volume breaches this run: {n_breaches}")

if n_breaches > 0:
    alert_rows = []
    alert_at = datetime.utcnow()
    for r in breaches:
        table_name       = r[1]
        clean_count      = r[2]
        quarantine_count = r[3]
        total            = r[4]
        pct              = r[5]
        threshold        = r[6]
        message = (
            f"Volume cap breach: silver.{table_name} has "
            f"{quarantine_count} quarantine rows out of {total} total ({pct:.2f}%) "
            f"— threshold is {threshold:.1f}%."
        )
        alert_rows.append((
            alert_at,
            table_name,
            "volume_cap",       # rule_name
            int(quarantine_count) if quarantine_count is not None else None,  # current_count
            int(total) if total is not None else None,                        # prior_count = total for context
            pct,                # growth_ratio repurposed as the percentage
            threshold,
            "critical",
            message,
            "open",
        ))

    alert_schema = StructType([
        StructField("alert_at",      TimestampType(), False),
        StructField("table_name",    StringType(),    True),
        StructField("rule_name",     StringType(),    True),
        StructField("current_count", LongType(),      True),
        StructField("prior_count",   LongType(),      True),
        StructField("growth_ratio",  DoubleType(),    True),
        StructField("threshold",     DoubleType(),    True),
        StructField("severity",      StringType(),    True),
        StructField("message",       StringType(),    True),
        StructField("status",        StringType(),    False),
    ])

    alerts_df = spark.createDataFrame(alert_rows, alert_schema)

    # Cast current_count and prior_count to INT to match the existing alerts table schema
    alerts_df = (
        alerts_df
        .withColumn("current_count", F.col("current_count").cast("int"))
        .withColumn("prior_count",   F.col("prior_count").cast("int"))
    )

    alerts_df.write.format("delta").mode("append").saveAsTable(ALERTS_TABLE)

    print("\nAlerts inserted:")
    alerts_df.show(truncate=False)
else:
    print("No volume breaches — nothing to alert.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 4 — RUN SUMMARY ==========

# COMMAND ----------

print(f"=== Volume cap check summary ===")
print(f"Computed at:           {computed_at}")
print(f"Tables checked:        {len(metric_rows)}")
print(f"Breaches detected:     {n_breaches}")
print(f"Halt on breach:        {HALT_ON_BREACH}")
print()
print(f"Open alerts in {ALERTS_TABLE}:")
spark.sql(f"""
    SELECT COUNT(*) AS open_alerts
    FROM {ALERTS_TABLE}
    WHERE status = 'open'
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== STEP 5 — HALT IF BREACHED ==========
# MAGIC
# MAGIC This is the actual circuit breaker. When `halt_on_breach = true` and any
# MAGIC non-excluded table breached its threshold, this cell raises an exception.
# MAGIC In a Databricks Workflow context, that fails the task and stops downstream
# MAGIC tasks (like Gold pipeline, UC1-4, etc.) from running on partial data.

# COMMAND ----------

if HALT_ON_BREACH and n_breaches > 0:
    breach_summary = ", ".join(
        f"{r[1]} ({r[5]:.2f}% > {r[6]:.1f}%)" for r in breaches
    )
    raise RuntimeError(
        f"VOLUME CAP BREACHED on {n_breaches} table(s): {breach_summary}. "
        f"See {METRICS_TABLE} and {ALERTS_TABLE} for details. "
        f"Investigate before re-running the Silver pipeline."
    )

print("Volume cap check passed — pipeline may continue.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to wire this up
# MAGIC
# MAGIC 1. **Standalone job (default)**: deploy via DAB and run on demand or on a schedule.
# MAGIC    See `quality_volume_cap_check` in `databricks.yml`.
# MAGIC
# MAGIC 2. **Inline guardrail (explicit opt-in)**: add as a downstream task in
# MAGIC    `primeins_end_to_end` between `silver_pipeline` and `gold_pipeline`. When the
# MAGIC    cap breaches, gold and the AI use cases will not run, preventing
# MAGIC    silently-degraded data from reaching dashboards and the LLM. **This change is
# MAGIC    NOT made by default — request it explicitly so you can decide when to flip
# MAGIC    Phase 1 from "always runs" to "fails on breach."**
# MAGIC
# MAGIC 3. **Tuning mode**: if you want to observe ratios for a few runs before letting
# MAGIC    the cap actually halt the pipeline, set the widget `halt_on_breach = false`.
# MAGIC    Metrics and alerts still get written; only the final raise is suppressed.
# MAGIC
# MAGIC 4. **Per-table tuning**: edit the `per_table_thresholds` widget to set per-table
# MAGIC    overrides. Format: `sales=70.0,claims=10.0`. Sales is overridden by default
# MAGIC    because of the known 62.9% padding-row problem.
