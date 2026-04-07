# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — Resolution Outcome Logger
# MAGIC
# MAGIC **Problem**: The Silver pipeline detects quality issues and routes failed records to
# MAGIC quarantine tables, and `silver.dq_issues` logs every issue. What's missing is the
# MAGIC *resolution* side: for any given quarantined record or rule violation, we have no
# MAGIC durable record of what triage decision was made, who made it, and when.
# MAGIC
# MAGIC **Solution**: An append-only `silver.quarantine_resolutions` table plus a small
# MAGIC helper API (`log_resolution`, `bulk_log_resolution`) that other notebooks (the
# MAGIC steward review notebook, the rule-fix backfill notebook, automated scripts) call
# MAGIC every time they make a triage decision.
# MAGIC
# MAGIC **The four resolution paths**:
# MAGIC 1. `source_fix`     — bad data at origin; ticket filed with source owner
# MAGIC 2. `rule_fix`       — Silver rule was wrong; updated and backfilled
# MAGIC 3. `manual_repair`  — steward fixed the record by hand
# MAGIC 4. `permanent_reject` — record is unrecoverable; archived for audit
# MAGIC
# MAGIC **Inputs**: none — this notebook only creates the table and helper functions.
# MAGIC **Output**: `{catalog}.silver.quarantine_resolutions`
# MAGIC
# MAGIC **Zero impact on Phase 1**: this is a new table in the existing silver schema.
# MAGIC No existing tables are read or modified.

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

dbutils.widgets.text("catalog", "primeins", "Unity Catalog")
CATALOG = dbutils.widgets.get("catalog")

RESOLUTIONS_TABLE = f"{CATALOG}.silver.quarantine_resolutions"

VALID_PATHS = ("source_fix", "rule_fix", "manual_repair", "permanent_reject")
VALID_STATUSES = ("pending", "resolved", "rejected", "in_progress")

print(f"Catalog:           {CATALOG}")
print(f"Resolutions table: {RESOLUTIONS_TABLE}")
print(f"Valid paths:       {VALID_PATHS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== TABLE DDL ==========
# MAGIC
# MAGIC Append-only Delta table. One row per triage decision. Use `record_key = NULL` for
# MAGIC rule-level resolutions that apply to a whole batch of records (e.g. "rule X was
# MAGIC too strict, backfilled 47 records") rather than to one specific record.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESOLUTIONS_TABLE} (
    resolution_id     BIGINT GENERATED ALWAYS AS IDENTITY,
    source_table      STRING  NOT NULL  COMMENT 'e.g. quarantine_customers, quarantine_claims',
    record_key        STRING            COMMENT 'natural key of the resolved record; NULL for rule-level resolutions',
    rule_name         STRING            COMMENT 'matches silver.dq_issues.rule_name when applicable',
    path_taken        STRING  NOT NULL  COMMENT 'source_fix | rule_fix | manual_repair | permanent_reject',
    final_status      STRING  NOT NULL  COMMENT 'pending | in_progress | resolved | rejected',
    reason_code       STRING            COMMENT 'short machine-readable code, e.g. SOURCE_CORRUPTED, RULE_TOO_STRICT',
    affected_records  INT               COMMENT 'how many records this single decision applied to',
    resolved_by       STRING  NOT NULL  COMMENT 'user email or system component name',
    resolved_at       TIMESTAMP         COMMENT 'when the resolution actually took effect (NULL while pending)',
    notes             STRING            COMMENT 'free-text context, optional',
    created_at        TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Append-only audit log of every triage decision made against a quarantined record or rule violation. Phase 1.5 — quality resolution loop.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")

print(f"Created/verified: {RESOLUTIONS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== HELPER API ==========
# MAGIC
# MAGIC Two functions:
# MAGIC - `log_resolution(...)` — log a single triage decision
# MAGIC - `bulk_log_resolution(rows)` — log many at once (used by the rule-fix backfill notebook)
# MAGIC
# MAGIC Both validate inputs against `VALID_PATHS` / `VALID_STATUSES` and write to the
# MAGIC append-only table. Importing this notebook via `%run` exposes these functions.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
from typing import Optional, Iterable, Mapping, Any


def _validate(path_taken: str, final_status: str) -> None:
    if path_taken not in VALID_PATHS:
        raise ValueError(
            f"path_taken must be one of {VALID_PATHS}, got '{path_taken}'"
        )
    if final_status not in VALID_STATUSES:
        raise ValueError(
            f"final_status must be one of {VALID_STATUSES}, got '{final_status}'"
        )


def log_resolution(
    *,
    source_table: str,
    path_taken: str,
    final_status: str,
    resolved_by: str,
    record_key: Optional[str] = None,
    rule_name: Optional[str] = None,
    reason_code: Optional[str] = None,
    affected_records: Optional[int] = 1,
    resolved_at: Optional[datetime] = None,
    notes: Optional[str] = None,
) -> None:
    """Log a single triage decision into silver.quarantine_resolutions.

    Example:
        log_resolution(
            source_table='quarantine_claims',
            record_key='CLM-8821',
            rule_name='valid_claim_id',
            path_taken='permanent_reject',
            final_status='rejected',
            reason_code='SOURCE_CORRUPTED',
            resolved_by='neeraj@example.com',
            notes='Date fields unrecoverable from source CSV — see corrupted dates ticket #142'
        )
    """
    _validate(path_taken, final_status)

    if resolved_at is None and final_status in ("resolved", "rejected"):
        resolved_at = datetime.utcnow()

    schema = StructType([
        StructField("source_table",     StringType(),    False),
        StructField("record_key",       StringType(),    True),
        StructField("rule_name",        StringType(),    True),
        StructField("path_taken",       StringType(),    False),
        StructField("final_status",     StringType(),    False),
        StructField("reason_code",      StringType(),    True),
        StructField("affected_records", IntegerType(),   True),
        StructField("resolved_by",      StringType(),    False),
        StructField("resolved_at",      TimestampType(), True),
        StructField("notes",            StringType(),    True),
    ])

    row = [(
        source_table,
        record_key,
        rule_name,
        path_taken,
        final_status,
        reason_code,
        affected_records,
        resolved_by,
        resolved_at,
        notes,
    )]

    (
        spark.createDataFrame(row, schema)
             .write
             .format("delta")
             .mode("append")
             .saveAsTable(RESOLUTIONS_TABLE)
    )


def bulk_log_resolution(rows: Iterable[Mapping[str, Any]]) -> int:
    """Log many triage decisions at once. Returns the count written.

    Each row in `rows` is a dict with the same keys as log_resolution's kwargs.
    Used by the rule-fix backfill notebook when one rule update resolves N records.
    """
    rows = list(rows)
    if not rows:
        return 0

    for r in rows:
        _validate(r["path_taken"], r["final_status"])
        if r.get("resolved_at") is None and r["final_status"] in ("resolved", "rejected"):
            r["resolved_at"] = datetime.utcnow()

    schema = StructType([
        StructField("source_table",     StringType(),    False),
        StructField("record_key",       StringType(),    True),
        StructField("rule_name",        StringType(),    True),
        StructField("path_taken",       StringType(),    False),
        StructField("final_status",     StringType(),    False),
        StructField("reason_code",      StringType(),    True),
        StructField("affected_records", IntegerType(),   True),
        StructField("resolved_by",      StringType(),    False),
        StructField("resolved_at",      TimestampType(), True),
        StructField("notes",            StringType(),    True),
    ])

    tuples = [(
        r["source_table"],
        r.get("record_key"),
        r.get("rule_name"),
        r["path_taken"],
        r["final_status"],
        r.get("reason_code"),
        r.get("affected_records", 1),
        r["resolved_by"],
        r.get("resolved_at"),
        r.get("notes"),
    ) for r in rows]

    (
        spark.createDataFrame(tuples, schema)
             .write
             .format("delta")
             .mode("append")
             .saveAsTable(RESOLUTIONS_TABLE)
    )
    return len(tuples)


print("Helpers ready: log_resolution(...), bulk_log_resolution([...])")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== SEED DEMO ROWS ==========
# MAGIC
# MAGIC Seeds the table with a few representative resolutions so the dashboard and Q&A
# MAGIC story have something to show before the steward review notebook is wired up.
# MAGIC These reflect the real known issues from Phase 1:
# MAGIC - corrupted claim dates  → permanent_reject (path 4)
# MAGIC - swapped customer_6 columns → already handled in Silver, logged for audit
# MAGIC - sales blank padding rows → permanent_reject (path 4)
# MAGIC
# MAGIC Re-running this notebook will append duplicate seed rows. Run once, or comment out
# MAGIC the seed cell after first execution. Set `SEED_DEMO_ROWS = False` below to skip.

# COMMAND ----------

SEED_DEMO_ROWS = True

if SEED_DEMO_ROWS:
    seed_rows = [
        dict(
            source_table="quarantine_claims",
            record_key=None,
            rule_name="corrupted_dates",
            path_taken="permanent_reject",
            final_status="rejected",
            reason_code="SOURCE_CORRUPTED",
            affected_records=1000,
            resolved_by="system",
            notes="Claim date fields stored as '27:00.0' time-only strings at source. "
                  "Unrecoverable. UC4 generates synthetic processing days at consumption layer only. "
                  "See architecture.md known limitations.",
        ),
        dict(
            source_table="quarantine_customers",
            record_key=None,
            rule_name="swapped_columns",
            path_taken="rule_fix",
            final_status="resolved",
            reason_code="SOURCE_LAYOUT_FIX",
            affected_records=199,
            resolved_by="system",
            notes="customers_6.csv: Marital_status and Education columns swapped. "
                  "Silver pipeline detects file by name and swaps reads. Source ticket pending "
                  "with the customers_6 owner for a permanent upstream fix.",
        ),
        dict(
            source_table="quarantine_sales",
            record_key=None,
            rule_name="null_sales_id",
            path_taken="permanent_reject",
            final_status="rejected",
            reason_code="EMPTY_PADDING_ROWS",
            affected_records=3132,
            resolved_by="system",
            notes="62.9% of sales source rows are entirely empty padding from the export script. "
                  "Source owner notified; awaiting fix to the export query to drop trailing blank rows.",
        ),
    ]
    n = bulk_log_resolution(seed_rows)
    print(f"Seeded {n} demo resolutions")
else:
    print("Seeding skipped (SEED_DEMO_ROWS = False)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== VERIFY ==========

# COMMAND ----------

print(f"Total rows in {RESOLUTIONS_TABLE}:")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {RESOLUTIONS_TABLE}").show()

print(f"\nResolutions by path:")
spark.sql(f"""
    SELECT path_taken, COUNT(*) AS decisions, SUM(affected_records) AS total_records
    FROM {RESOLUTIONS_TABLE}
    GROUP BY path_taken
    ORDER BY decisions DESC
""").show(truncate=False)

print(f"\nMost recent 10 resolutions:")
spark.sql(f"""
    SELECT source_table, rule_name, path_taken, final_status, affected_records, resolved_by, created_at
    FROM {RESOLUTIONS_TABLE}
    ORDER BY created_at DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use this from another notebook
# MAGIC
# MAGIC ```python
# MAGIC %run ../quality/36_dq_resolution_logger
# MAGIC
# MAGIC # ... your steward / backfill / triage code ...
# MAGIC
# MAGIC log_resolution(
# MAGIC     source_table='quarantine_customers',
# MAGIC     record_key='12345',
# MAGIC     rule_name='invalid_region',
# MAGIC     path_taken='manual_repair',
# MAGIC     final_status='resolved',
# MAGIC     reason_code='STEWARD_FIX',
# MAGIC     resolved_by='steward@example.com',
# MAGIC     notes='Filled in region=West based on city=Sacramento'
# MAGIC )
# MAGIC ```
