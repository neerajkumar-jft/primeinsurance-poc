# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer - Combined Data Quality Issues
# MAGIC
# MAGIC Merge all entity-level DQ issue tables into a single unified view.
# MAGIC Handles missing tables gracefully (some entities may have zero issues).

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------

# collect all entity DQ tables - skip any that don't exist
entities = ["customers", "claims", "policy", "sales", "cars"]
dq_tables = []

for entity in entities:
    table_name = f"`databricks-hackathon-insurance`.silver.dq_issues_{entity}"
    try:
        df = spark.table(table_name)
        cnt = df.count()
        if cnt > 0:
            dq_tables.append(df)
            print(f"  loaded {table_name}: {cnt} issues")
        else:
            print(f"  skipping {table_name}: 0 issues")
    except Exception:
        print(f"  skipping {table_name}: table doesn't exist (no issues found for {entity})")

# COMMAND ----------

if dq_tables:
    all_dq = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        dq_tables
    )

    (all_dq.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("`databricks-hackathon-insurance`.silver.dq_issues"))

    total_issues = all_dq.count()
    print(f"\ndq_issues: {total_issues} total issues")
    display(all_dq.orderBy(F.desc("severity"), F.desc("affected_ratio")))
else:
    # create empty table so downstream doesn't fail
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    schema = StructType([
        StructField("entity", StringType()),
        StructField("rule_name", StringType()),
        StructField("records_affected", IntegerType()),
        StructField("total_records", IntegerType()),
        StructField("affected_ratio", DoubleType()),
        StructField("severity", StringType()),
        StructField("suggested_fix", StringType()),
        StructField("detected_at", StringType()),
    ])
    empty_df = spark.createDataFrame([], schema)
    (empty_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("`databricks-hackathon-insurance`.silver.dq_issues"))
    print("no DQ issues found across any entity - created empty table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Scorecard

# COMMAND ----------

# use python instead of %sql to avoid TABLE_NOT_FOUND errors
try:
    dq = spark.table("`databricks-hackathon-insurance`.silver.dq_issues")
    if dq.count() > 0:
        scorecard = (dq
            .groupBy("entity")
            .agg(
                F.count("*").alias("issue_count"),
                F.sum("records_affected").alias("total_affected"),
                F.round(F.avg("affected_ratio") * 100, 2).alias("avg_affected_pct"),
                F.sum(F.when(F.col("severity") == "HIGH", 1).otherwise(0)).alias("high"),
                F.sum(F.when(F.col("severity") == "MEDIUM", 1).otherwise(0)).alias("medium"),
                F.sum(F.when(F.col("severity") == "LOW", 1).otherwise(0)).alias("low"),
            )
            .orderBy(F.desc("high"))
        )
        display(scorecard)
    else:
        print("no issues to summarize")
except Exception:
    print("dq_issues table not available")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer Summary

# COMMAND ----------

silver_tables = ["customers", "claims", "policy", "sales", "cars",
                 "dq_issues", "customer_resolution_audit",
                 "quarantine_customers", "quarantine_claims",
                 "quarantine_policy", "quarantine_sales", "quarantine_cars"]

print("=== Silver Layer Summary ===\n")
for t in silver_tables:
    try:
        count = spark.table(f"`databricks-hackathon-insurance`.silver.{t}").count()
        print(f"  silver.{t:35s} | {count:6d} rows")
    except Exception:
        print(f"  silver.{t:35s} | NOT FOUND")

# COMMAND ----------

# MAGIC %md
# MAGIC Silver layer complete.
