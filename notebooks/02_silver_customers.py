# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer - Customers
# MAGIC
# MAGIC Hardest transformation in the whole pipeline.
# MAGIC 7 regional systems, 7 different schemas, overlapping customers.
# MAGIC
# MAGIC Schema chaos found in Bronze:
# MAGIC - ID columns: `CustomerID`, `Customer_ID`, `cust_id`
# MAGIC - Region: `Region`, `Reg`
# MAGIC - City: `City`, `City_in_state`
# MAGIC - Education: `Education`, `Edu`, or missing entirely
# MAGIC - Marital: `Marital`, `Marital_status`, or missing
# MAGIC - Job: present in most files, missing from customers_1
# MAGIC - HHInsurance: missing from customers_2

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
from functools import reduce

# COMMAND ----------

# skip if silver customers already populated
try:
    cnt = spark.table("`databricks-hackathon-insurance`.silver.customers").count()
    if cnt > 0:
        print(f"silver.customers already has {cnt} rows - skipping")
        dbutils.notebook.exit(f"SKIPPED - silver.customers has {cnt} rows")
except Exception:
    print("silver.customers doesn't exist yet - running transformation")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `databricks-hackathon-insurance`;

# COMMAND ----------

bronze_customers = spark.table("`databricks-hackathon-insurance`.bronze.customers")
print(f"bronze customers: {bronze_customers.count()} rows")
print(f"columns: {bronze_customers.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Unify Column Names
# MAGIC
# MAGIC Different regions used different column names for the same thing.
# MAGIC We need to coalesce them into a standard set.

# COMMAND ----------

# unify the mess of column names into standard ones
customers_unified = (bronze_customers
    # customer ID - three variants
    .withColumn("customer_id",
        F.coalesce(
            F.col("CustomerID"),
            F.col("Customer_ID"),
            F.col("cust_id"),
        ).cast("string")
    )
    # region - two variants
    .withColumn("region",
        F.coalesce(
            F.col("Region"),
            F.col("Reg"),
        )
    )
    # city - two variants
    .withColumn("city",
        F.coalesce(
            F.col("City"),
            F.col("City_in_state"),
        )
    )
    # education - two variants
    .withColumn("education",
        F.coalesce(
            F.col("Education"),
            F.col("Edu"),
        )
    )
    # marital - two variants
    .withColumn("marital",
        F.coalesce(
            F.col("Marital"),
            F.col("Marital_status"),
        )
    )
    # job - may be null for some files
    .withColumn("job",
        F.when(F.col("Job").isNotNull(), F.col("Job")).otherwise(F.lit(None))
    )
    # numeric cols - straightforward
    .withColumn("has_default", F.col("Default").cast("integer"))
    .withColumn("balance", F.col("Balance").cast("integer"))
    .withColumn("hh_insurance",
        F.when(F.col("HHInsurance").isNotNull(), F.col("HHInsurance").cast("integer")).otherwise(F.lit(None))
    )
    .withColumn("car_loan",
        F.when(F.col("CarLoan").isNotNull(), F.col("CarLoan").cast("integer")).otherwise(F.lit(None))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Standardize Values

# COMMAND ----------

# region normalization - handle abbreviations and casing
region_map = {
    "east": "East", "west": "West", "north": "North",
    "south": "South", "central": "Central",
    "e": "East", "w": "West", "n": "North", "s": "South", "c": "Central",
}

region_expr = F.col("region")
for old_val, new_val in region_map.items():
    region_expr = F.when(
        F.lower(F.trim(F.col("region"))) == old_val, new_val
    ).otherwise(region_expr)

customers_std = (customers_unified
    .withColumn("region", region_expr)
    .withColumn("state", F.upper(F.trim(F.col("State"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("job", F.initcap(F.trim(F.col("job"))))
    .withColumn("marital", F.initcap(F.trim(F.col("marital"))))
    .withColumn("education", F.initcap(F.trim(F.col("education"))))
)

# COMMAND ----------

# quick look at what we have now
print("region values after standardization:")
display(customers_std.groupBy("region").count().orderBy("region"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Quality Checks & Quarantine

# COMMAND ----------

quality_rules = [
    ("customer_id_null", F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")),
    ("region_invalid", F.col("region").isNull() | (~F.col("region").isin("East", "West", "North", "South", "Central"))),
    ("state_null", F.col("state").isNull() | (F.trim(F.col("state")) == "")),
]

# flag each rule
for rule_name, condition in quality_rules:
    customers_std = customers_std.withColumn(
        f"_fail_{rule_name}",
        F.when(condition, True).otherwise(False)
    )

fail_columns = [f"_fail_{r[0]}" for r in quality_rules]
customers_std = customers_std.withColumn(
    "_quality_passed",
    ~reduce(lambda a, b: a | b, [F.col(c) for c in fail_columns])
)

# report
total = customers_std.count()
passed = customers_std.filter("_quality_passed = true").count()
failed = total - passed
print(f"passed: {passed} ({passed/total*100 if total > 0 else 0:.1f}%)")
print(f"failed: {failed} ({failed/total*100 if total > 0 else 0:.1f}%)")

for rule_name, _ in quality_rules:
    cnt = customers_std.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        print(f"  {rule_name}: {cnt}")

# COMMAND ----------

# quarantine bad records
quarantine_df = (customers_std
    .filter("_quality_passed = false")
    .withColumn("_quarantine_reason",
        F.concat_ws(", ", *[
            F.when(F.col(f"_fail_{r[0]}"), F.lit(r[0]))
            for r in quality_rules
        ])
    )
    .withColumn("_quarantined_at", F.current_timestamp())
)

(quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.quarantine_customers"))

print(f"quarantined {quarantine_df.count()} customer records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Customer Identity Resolution
# MAGIC
# MAGIC Same customer appears across regions with different IDs.
# MAGIC We match on normalized (state + city + balance + has_default) as a composite key.
# MAGIC Not perfect but good enough for a POC - production would use fuzzy matching.

# COMMAND ----------

good_customers = (customers_std
    .filter("_quality_passed = true")
    .drop(*[c for c in customers_std.columns if c.startswith("_fail_")])
    .drop("_quality_passed")
)

# matching key from attributes that should be consistent for same person
good_customers = good_customers.withColumn(
    "_match_key",
    F.md5(F.concat_ws("|",
        F.lower(F.trim(F.col("state"))),
        F.lower(F.trim(F.col("city"))),
        F.coalesce(F.lower(F.trim(F.col("job"))), F.lit("unknown")),
        F.coalesce(F.lower(F.trim(F.col("marital"))), F.lit("unknown")),
        F.col("balance").cast("string"),
    ))
)

# COMMAND ----------

# dedup stats
total_records = good_customers.count()
unique_keys = good_customers.select("_match_key").distinct().count()
duplicates = total_records - unique_keys

print(f"total records: {total_records}")
print(f"unique identities: {unique_keys}")
print(f"duplicates found: {duplicates} ({duplicates/total_records*100 if total_records > 0 else 0:.1f}%)")

# COMMAND ----------

# assign master customer ID - keep first occurrence per match key
window = Window.partitionBy("_match_key").orderBy("_source_file")

customers_resolved = (good_customers
    .withColumn("_row_num", F.row_number().over(window))
    .withColumn("_group_size", F.count("*").over(Window.partitionBy("_match_key")))
    .withColumn("master_customer_id",
        F.concat(F.lit("CUST-"), F.dense_rank().over(Window.orderBy("_match_key")))
    )
    .withColumn("original_customer_id", F.col("customer_id"))
    .withColumn("is_duplicate", F.when(F.col("_row_num") > 1, True).otherwise(False))
)

# COMMAND ----------

# select only the master record for each customer
silver_customers = (customers_resolved
    .filter(F.col("_row_num") == 1)
    .select(
        "master_customer_id",
        "original_customer_id",
        "region",
        "state",
        "city",
        "job",
        "marital",
        "education",
        "has_default",
        "balance",
        "hh_insurance",
        "car_loan",
        "_source_file",
        F.current_timestamp().alias("_processed_at"),
    )
)

(silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.customers"))

print(f"silver.customers: {silver_customers.count()} unique customers")

# COMMAND ----------

# save resolution audit trail for regulatory compliance
resolution_audit = (customers_resolved
    .select(
        "master_customer_id",
        "original_customer_id",
        "region",
        "_source_file",
        "_row_num",
        "_group_size",
        "is_duplicate",
    )
    .withColumn("_resolved_at", F.current_timestamp())
)

(resolution_audit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.customer_resolution_audit"))

print(f"resolution audit: {resolution_audit.count()} records")
print(f"  unique: {resolution_audit.filter(~F.col('is_duplicate')).count()}")
print(f"  duplicates: {resolution_audit.filter(F.col('is_duplicate')).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Quality Issues

# COMMAND ----------

from datetime import datetime

dq_entries = []
for rule_name, condition in quality_rules:
    cnt = customers_std.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        dq_entries.append({
            "entity": "customers",
            "rule_name": rule_name,
            "records_affected": cnt,
            "total_records": total,
            "affected_ratio": round(cnt / total, 4) if total > 0 else 0.0,
            "severity": "HIGH" if total > 0 and cnt / total > 0.05 else "MEDIUM" if total > 0 and cnt / total > 0.01 else "LOW",
            "suggested_fix": f"Review and fix records failing {rule_name}",
            "detected_at": datetime.now().isoformat(),
        })

# dedup stats as quality issue
dq_entries.append({
    "entity": "customers",
    "rule_name": "cross_region_duplicates",
    "records_affected": duplicates,
    "total_records": total_records,
    "affected_ratio": round(duplicates / total_records, 4) if total_records > 0 else 0,
    "severity": "HIGH",
    "suggested_fix": "Unified via master_customer_id using identity resolution",
    "detected_at": datetime.now().isoformat(),
})

if dq_entries:
    dq_df = spark.createDataFrame(dq_entries)
    (dq_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("`databricks-hackathon-insurance`.silver.dq_issues_customers"))
    print(f"logged {len(dq_entries)} quality issues")

# COMMAND ----------

display(spark.table("`databricks-hackathon-insurance`.silver.dq_issues_customers"))
