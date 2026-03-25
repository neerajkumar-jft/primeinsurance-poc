# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer - Policy
# MAGIC
# MAGIC Policy data links customers to their coverage.
# MAGIC Key fields: policy_csl (coverage/split limit), deductible, premium.
# MAGIC Business needs to query rejection rates by policy_csl type.

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce
from datetime import datetime

# COMMAND ----------

try:
    cnt = spark.table("`databricks-hackathon-insurance`.silver.policy").count()
    if cnt > 0:
        print(f"silver.policy already has {cnt} rows - skipping")
        dbutils.notebook.exit(f"SKIPPED - silver.policy has {cnt} rows")
except Exception:
    print("silver.policy doesn't exist yet - running transformation")

# COMMAND ----------

bronze_policy = spark.table("`databricks-hackathon-insurance`.bronze.policy")
print(f"bronze policy: {bronze_policy.count()} rows")
bronze_policy.printSchema()

# COMMAND ----------

display(bronze_policy.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean & Transform

# COMMAND ----------

# standardize column names
def clean_col_names(df):
    for c in df.columns:
        new_name = c.strip().lower().replace(" ", "_")
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)
    return df

policy_clean = clean_col_names(bronze_policy)

# COMMAND ----------

# parse dates - use try_to_date via SQL to avoid throws on bad input
policy_clean = policy_clean.withColumn("policy_bind_date",
    F.expr("""coalesce(
        try_to_date(policy_bind_date, 'yyyy-MM-dd'),
        try_to_date(policy_bind_date, 'MM/dd/yyyy'),
        try_to_date(policy_bind_date, 'dd/MM/yyyy'),
        try_to_date(policy_bind_date, 'yyyy/MM/dd')
    )""")
)

# COMMAND ----------

# cast numeric fields properly
policy_clean = (policy_clean
    .withColumn("policy_deductable", F.col("policy_deductable").cast("integer"))
    .withColumn("policy_annual_premium", F.col("policy_annual_premium").cast("double"))
    .withColumn("umbrella_limit", F.col("umbrella_limit").cast("integer"))
)

# standardize state
policy_clean = policy_clean.withColumn(
    "policy_state",
    F.upper(F.trim(F.col("policy_state")))
)

# standardize policy_csl - should be in format like "100/300", "250/500" etc
policy_clean = policy_clean.withColumn(
    "policy_csl",
    F.trim(F.col("policy_csl"))
)

# COMMAND ----------

# check unique policy_csl values
print("=== policy_csl values ===")
display(policy_clean.groupBy("policy_csl").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Rules

# COMMAND ----------

quality_rules = [
    ("policy_number_null", F.col("policy_number").isNull() | (F.trim(F.col("policy_number")) == "")),
    ("bind_date_invalid", F.col("policy_bind_date").isNull()),
    ("premium_not_positive", F.col("policy_annual_premium") <= 0),
    ("deductible_negative", F.col("policy_deductable") < 0),
    ("csl_invalid", F.col("policy_csl").isNull() | (F.trim(F.col("policy_csl")) == "")),
    ("customer_id_null", F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")),
]

for rule_name, condition in quality_rules:
    policy_clean = policy_clean.withColumn(
        f"_fail_{rule_name}",
        F.when(condition, True).otherwise(False)
    )

fail_cols = [f"_fail_{r[0]}" for r in quality_rules]
policy_clean = policy_clean.withColumn(
    "_quality_passed",
    ~reduce(lambda a, b: a | b, [F.col(c) for c in fail_cols])
)

# report
total = policy_clean.count()
passed = policy_clean.filter("_quality_passed = true").count()
failed = total - passed
print(f"passed: {passed} ({passed/total*100 if total > 0 else 0:.1f}%) | failed: {failed} ({failed/total*100 if total > 0 else 0:.1f}%)")

for rule_name, _ in quality_rules:
    cnt = policy_clean.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        print(f"  {rule_name}: {cnt}")

# COMMAND ----------

# quarantine
quarantine = (policy_clean
    .filter("_quality_passed = false")
    .withColumn("_quarantine_reason",
        F.concat_ws(", ", *[F.when(F.col(f"_fail_{r[0]}"), F.lit(r[0])) for r in quality_rules]))
    .withColumn("_quarantined_at", F.current_timestamp())
)

(quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.quarantine_policy"))

print(f"quarantined {quarantine.count()} policy records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Policy

# COMMAND ----------

silver_policy = (policy_clean
    .filter("_quality_passed = true")
    .select(
        "policy_number",
        "policy_bind_date",
        "policy_state",
        "policy_csl",
        F.col("policy_deductable").alias("policy_deductible"),  # fix the typo in source
        "policy_annual_premium",
        "umbrella_limit",
        "car_id",
        "customer_id",
        "_source_file",
        F.current_timestamp().alias("_processed_at"),
    )
)

(silver_policy.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.policy"))

print(f"silver.policy: {silver_policy.count()} rows")

# COMMAND ----------

# dq issues log
dq_entries = []
for rule_name, _ in quality_rules:
    cnt = policy_clean.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        dq_entries.append({
            "entity": "policy",
            "rule_name": rule_name,
            "records_affected": cnt,
            "total_records": total,
            "affected_ratio": round(cnt / total, 4) if total > 0 else 0.0,
            "severity": "HIGH" if total > 0 and cnt / total > 0.05 else "MEDIUM" if total > 0 and cnt / total > 0.01 else "LOW",
            "suggested_fix": f"Fix {rule_name} in policy records",
            "detected_at": datetime.now().isoformat(),
        })

if dq_entries:
    dq_df = spark.createDataFrame(dq_entries)
    (dq_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("`databricks-hackathon-insurance`.silver.dq_issues_policy"))
    display(dq_df)
else:
    print("no quality issues found in policy data")
