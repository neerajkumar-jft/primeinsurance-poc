# Databricks notebook source
# MAGIC %md
# MAGIC # Serving Layer — Direct Load to Lakebase (Pure Python)
# MAGIC
# MAGIC **Why this notebook exists**: the managed "synced tables" feature on this
# MAGIC workspace failed to provision its internal pipeline (UC deployment error on
# MAGIC the pipeline cluster). Spark's native postgresql connector is not usable on
# MAGIC serverless compute either. So we use the approach that always works:
# MAGIC **pure Python with psycopg2**, writing each Gold table row-by-row into
# MAGIC Lakebase. Data volumes are tiny (max 2,500 rows per table, ~8,000 rows
# MAGIC total), so this takes seconds.
# MAGIC
# MAGIC **What it does**:
# MAGIC 1. Generates a short-lived OAuth token for the Lakebase instance
# MAGIC 2. For each of 6 Gold tables: reads rows via Spark, creates the Lakebase
# MAGIC    table with inferred column types, and bulk-inserts the data
# MAGIC 3. Creates the business-enrichment view `public.fact_sales_enriched` with
# MAGIC    an `aging_bucket` column derived from `days_listed`
# MAGIC 4. Verifies row counts against Gold
# MAGIC
# MAGIC **How to keep it current**: re-run this notebook as a scheduled task after
# MAGIC the Gold DLT pipeline completes. Each run overwrites — safe and idempotent.
# MAGIC
# MAGIC **Note on sync approach**: this is a "manual sync" fallback. Production
# MAGIC would use Databricks managed synced tables once the underlying UC pipeline
# MAGIC issue is resolved. See architecture notes for the migration path.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "primeins", "UC catalog")
dbutils.widgets.text("instance_name", "primeins-lakebase", "Lakebase instance name")
dbutils.widgets.text("database_name", "primeins", "Lakebase database name")

CATALOG = dbutils.widgets.get("catalog")
INSTANCE_NAME = dbutils.widgets.get("instance_name")
DB_NAME = dbutils.widgets.get("database_name")

# COMMAND ----------

# ============================================================
# GET LAKEBASE CONNECTION DETAILS
# ============================================================

from databricks.sdk import WorkspaceClient
import uuid

w = WorkspaceClient()

inst = w.database.get_database_instance(name=INSTANCE_NAME)
host = inst.read_write_dns
print(f"Lakebase host: {host}")
print(f"Lakebase state: {inst.state}")

cred = w.database.generate_database_credential(
    instance_names=[INSTANCE_NAME],
    request_id=f"primeins-load-{uuid.uuid4().hex[:8]}"
)
token = cred.token
print(f"Credential token obtained (length={len(token)})")

current_user = w.current_user.me().user_name
print(f"User (pg role): {current_user}")

# COMMAND ----------

# ============================================================
# CONNECT TO LAKEBASE
# ============================================================

import psycopg2
from psycopg2.extras import execute_values

conn = psycopg2.connect(
    host=host, port=5432, dbname=DB_NAME,
    user=current_user, password=token, sslmode="require"
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT version()")
print(f"Connected to: {cur.fetchone()[0]}")

# COMMAND ----------

# ============================================================
# HELPER — infer postgres type from Spark type
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, StringType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType, DecimalType, ArrayType,
)

def pg_type(spark_field):
    t = spark_field.dataType
    if isinstance(t, (IntegerType, LongType)):
        return "BIGINT"
    if isinstance(t, (DoubleType, FloatType)):
        return "DOUBLE PRECISION"
    if isinstance(t, DecimalType):
        return f"NUMERIC({t.precision},{t.scale})"
    if isinstance(t, BooleanType):
        return "BOOLEAN"
    if isinstance(t, TimestampType):
        return "TIMESTAMP"
    if isinstance(t, DateType):
        return "DATE"
    if isinstance(t, ArrayType):
        return "TEXT"   # JSON-encoded arrays, safest for this POC
    return "TEXT"


def load_table(source_fqn: str, target_table: str, pk_col: str):
    """Full-copy overwrite of one Gold table into Lakebase."""
    print(f"--- loading {source_fqn} -> public.{target_table} ---")
    df = spark.table(source_fqn)

    # Deduplicate on PK column before load.
    # Some Gold tables have duplicate natural keys (e.g. fact_sales.sales_id
    # repeats across the 3 sales source files). Keep the first occurrence.
    if pk_col:
        raw_count = df.count()
        df = df.dropDuplicates([pk_col])
        deduped_count = df.count()
        if raw_count != deduped_count:
            print(f"    deduped {raw_count - deduped_count} duplicate {pk_col} values "
                  f"({raw_count} -> {deduped_count})")

    # Build CREATE TABLE DDL
    col_defs = []
    col_names = []
    for f in df.schema.fields:
        col_defs.append(f'"{f.name}" {pg_type(f)}')
        col_names.append(f.name)
    pk_clause = f', PRIMARY KEY ("{pk_col}")' if pk_col else ""
    ddl = f'DROP TABLE IF EXISTS public."{target_table}" CASCADE; ' \
          f'CREATE TABLE public."{target_table}" ({", ".join(col_defs)}{pk_clause})'
    cur.execute(ddl)

    # Collect rows (tiny — max few thousand per table)
    rows = df.collect()
    print(f"    source rows to insert: {len(rows)}")
    if not rows:
        print("    (no rows — skipping insert)")
        return 0

    # Bulk insert via execute_values
    col_list = ", ".join(f'"{c}"' for c in col_names)
    insert_sql = f'INSERT INTO public."{target_table}" ({col_list}) VALUES %s'
    # Convert each row to a tuple; serialize arrays to text
    def conv(v):
        if isinstance(v, list):
            import json as _j
            return _j.dumps(v)
        return v
    tuples = [tuple(conv(r[c]) for c in col_names) for r in rows]
    execute_values(cur, insert_sql, tuples, page_size=500)
    print(f"    wrote {len(rows)} rows to public.{target_table}")
    return len(rows)

# COMMAND ----------

# ============================================================
# LOAD THE 6 GOLD TABLES
# ============================================================

TABLES = [
    ("fact_claims",  "claim_id"),
    ("fact_sales",   "sales_id"),
    ("dim_customer", "customer_id"),
    ("dim_policy",   "policy_number"),
    ("dim_car",      "car_id"),
    ("dim_region",   "region"),
]

total = 0
for tbl, pk in TABLES:
    total += load_table(f"{CATALOG}.gold.{tbl}", tbl, pk)
print(f"\nTotal rows loaded across 6 tables: {total}")

# COMMAND ----------

# ============================================================
# CREATE BUSINESS ENRICHMENT VIEW
# ============================================================
# This view is the whole point of the "enrich the serving layer" ask.
# It adds an aging_bucket column to fact_sales, directly useful for the
# sales head who wants to see aging inventory at a glance. No touching Gold.

cur.execute("""
CREATE OR REPLACE VIEW public.fact_sales_enriched AS
SELECT
  *,
  CASE
    WHEN days_listed IS NULL  THEN 'Unknown'
    WHEN days_listed < 30     THEN 'Fresh'
    WHEN days_listed < 60     THEN 'Watch'
    WHEN days_listed < 90     THEN 'Stale'
    ELSE                           'Critical'
  END AS aging_bucket
FROM public.fact_sales
""")
print("Created view: public.fact_sales_enriched (adds aging_bucket column)")

# COMMAND ----------

# ============================================================
# VERIFY — row counts and a sample of the enriched view
# ============================================================

print("Row-count parity check (Lakebase vs Gold):")
all_ok = True
for tbl, _ in TABLES:
    cur.execute(f'SELECT COUNT(*) FROM public."{tbl}"')
    pg_count = cur.fetchone()[0]
    gold_count = spark.table(f"{CATALOG}.gold.{tbl}").count()
    match = "OK" if pg_count == gold_count else "MISMATCH"
    if match == "MISMATCH":
        all_ok = False
    print(f"  {tbl:15s} lakebase={pg_count:>5d}  gold={gold_count:>5d}  {match}")
print(f"\nOverall parity: {'PASS' if all_ok else 'FAIL'}")

print("\nAging bucket distribution in public.fact_sales_enriched:")
cur.execute("""
    SELECT aging_bucket, COUNT(*) AS rows
    FROM public.fact_sales_enriched
    GROUP BY aging_bucket
    ORDER BY
      CASE aging_bucket
        WHEN 'Critical' THEN 1
        WHEN 'Stale'    THEN 2
        WHEN 'Watch'    THEN 3
        WHEN 'Fresh'    THEN 4
        ELSE 5
      END
""")
for row in cur.fetchall():
    print(f"  {row[0]:10s} {row[1]}")

print("\nSample of aging inventory (days_listed > 60):")
cur.execute("""
    SELECT sales_id, region, days_listed, aging_bucket
    FROM public.fact_sales_enriched
    WHERE days_listed > 60
    ORDER BY days_listed DESC
    LIMIT 10
""")
for row in cur.fetchall():
    print(f"  sales_id={row[0]}  region={row[1]}  days_listed={row[2]}  aging_bucket={row[3]}")

cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done.
# MAGIC
# MAGIC Lakebase serving layer is populated. Any application with a standard Postgres
# MAGIC driver can now connect to the instance and query six Gold tables plus the
# MAGIC enriched view.
# MAGIC
# MAGIC **Keeping it current**: schedule this notebook downstream of the existing
# MAGIC `primeins_end_to_end` workflow. Full overwrite each run, idempotent, takes
# MAGIC seconds at POC scale.
