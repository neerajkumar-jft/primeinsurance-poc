# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer - Claims
# MAGIC
# MAGIC Claims data is critical - it's the core of the backlog problem.
# MAGIC
# MAGIC Data quirks found:
# MAGIC - All JSON values are strings (amounts, dates, counts)
# MAGIC - Dates are Excel serial fragments like "27:00.0"
# MAGIC - Claim_Processed_On has literal "NULL" strings
# MAGIC - Column names are mixed case (ClaimID, PolicyID, etc)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from functools import reduce
from datetime import datetime

# COMMAND ----------

# skip if already done
try:
    cnt = spark.table("`databricks-hackathon-insurance`.silver.claims").count()
    if cnt > 0:
        print(f"silver.claims already has {cnt} rows - skipping")
        dbutils.notebook.exit(f"SKIPPED - silver.claims has {cnt} rows")
except Exception:
    print("silver.claims doesn't exist yet - running transformation")

# COMMAND ----------

bronze_claims = spark.table("`databricks-hackathon-insurance`.bronze.claims")
total_bronze = bronze_claims.count()
print(f"bronze claims: {total_bronze} rows")
print(f"columns: {bronze_claims.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Lowercase all column names

# COMMAND ----------

def clean_col_names(df):
    for c in df.columns:
        new_name = c.strip().lower().replace(" ", "_")
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)
    return df

claims = clean_col_names(bronze_claims)
print("columns after clean:", claims.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Clean NULL strings and parse dates

# COMMAND ----------

# replace literal "NULL" strings with actual nulls across all string columns
for c in claims.columns:
    if dict(claims.dtypes).get(c) == "string":
        claims = claims.withColumn(c,
            F.when(F.upper(F.trim(F.col(c))).isin("NULL", "NONE", "N/A", ""), F.lit(None))
             .otherwise(F.col(c))
        )

# COMMAND ----------

# dates are in format like "27:00.0" - these are NOT real dates
# to_date throws on invalid input, so we use try_to_date via SQL expr
# which returns NULL instead of throwing

def parse_date_col(col_name):
    """use SQL try_to_date (safe) instead of F.to_date (throws)"""
    # try standard formats via SQL try_to_date
    standard = F.expr(f"""coalesce(
        try_to_date(`{col_name}`, 'yyyy-MM-dd'),
        try_to_date(`{col_name}`, 'MM/dd/yyyy'),
        try_to_date(`{col_name}`, 'dd/MM/yyyy')
    )""")

    # for "27:00.0" serial format: extract day, build safe date string
    day_str = F.regexp_extract(F.col(col_name), r"^(\d+)", 1)
    day_num = day_str.cast("int")
    date_str = F.concat(F.lit("2024-01-"), F.lpad(day_str, 2, "0"))
    serial = F.when(
        (day_num.isNotNull()) & (day_num >= 1) & (day_num <= 28),
        F.expr(f"try_to_date(concat('2024-01-', lpad(regexp_extract(`{col_name}`, '^(\\\\d+)', 1), 2, '0')), 'yyyy-MM-dd')")
    )

    return F.coalesce(standard, serial)

for dc in ["incident_date", "claim_logged_on", "claim_processed_on"]:
    if dc in claims.columns:
        claims = claims.withColumn(dc, parse_date_col(dc))

# COMMAND ----------

# date parse stats
for dc in ["incident_date", "claim_logged_on", "claim_processed_on"]:
    if dc in claims.columns:
        parsed = claims.filter(F.col(dc).isNotNull()).count()
        print(f"{dc}: {parsed}/{total_bronze} parsed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Cast amounts and compute total

# COMMAND ----------

for c in ["injury", "property", "vehicle"]:
    if c in claims.columns:
        claims = claims.withColumn(c, F.col(c).cast("double"))

claims = claims.withColumn(
    "total_claim_amount",
    F.coalesce(F.col("injury"), F.lit(0.0)) +
    F.coalesce(F.col("property"), F.lit(0.0)) +
    F.coalesce(F.col("vehicle"), F.lit(0.0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Standardize categoricals

# COMMAND ----------

for c in ["incident_severity", "incident_type", "collision_type", "authorities_contacted", "property_damage"]:
    if c in claims.columns:
        claims = claims.withColumn(c, F.initcap(F.trim(F.col(c))))

if "claim_rejected" in claims.columns:
    claims = claims.withColumn("claim_rejected",
        F.when(F.upper(F.trim(F.col("claim_rejected"))).isin("Y", "YES", "TRUE", "1"), "Y")
         .when(F.upper(F.trim(F.col("claim_rejected"))).isin("N", "NO", "FALSE", "0"), "N")
         .otherwise(F.col("claim_rejected"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Processing time

# COMMAND ----------

claims = claims.withColumn(
    "processing_days",
    F.datediff(F.col("claim_processed_on"), F.col("claim_logged_on"))
)
claims = claims.withColumn(
    "sla_breach",
    F.when(F.col("processing_days") > 7, True).otherwise(False)
)

# safe stats
avg_days = claims.agg(F.mean("processing_days")).collect()[0][0]
sla_cnt = claims.filter("sla_breach = true").count()
print(f"avg processing days: {avg_days if avg_days else 'N/A'}")
print(f"SLA breaches: {sla_cnt}/{total_bronze}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Quality rules & quarantine

# COMMAND ----------

quality_rules = [
    ("claim_id_null", F.col("claimid").isNull() | (F.trim(F.col("claimid")) == "")),
    ("policy_id_null", F.col("policyid").isNull() | (F.trim(F.col("policyid")) == "")),
    ("incident_date_invalid", F.col("incident_date").isNull()),
    ("negative_amounts", (F.coalesce(F.col("injury"), F.lit(0.0)) < 0) | (F.coalesce(F.col("property"), F.lit(0.0)) < 0) | (F.coalesce(F.col("vehicle"), F.lit(0.0)) < 0)),
]

for rule_name, condition in quality_rules:
    claims = claims.withColumn(f"_fail_{rule_name}", F.when(condition, True).otherwise(False))

fail_cols = [f"_fail_{r[0]}" for r in quality_rules]
claims = claims.withColumn(
    "_quality_passed",
    ~reduce(lambda a, b: a | b, [F.col(c) for c in fail_cols])
)

total = claims.count()
passed = claims.filter("_quality_passed = true").count()
failed = total - passed
print(f"passed: {passed}/{total} | failed: {failed}/{total}")

for rn, _ in quality_rules:
    c = claims.filter(F.col(f"_fail_{rn}") == True).count()
    if c > 0:
        print(f"  {rn}: {c}")

# COMMAND ----------

# quarantine
quarantine = (claims.filter("_quality_passed = false")
    .withColumn("_quarantine_reason",
        F.concat_ws(", ", *[F.when(F.col(f"_fail_{r[0]}"), F.lit(r[0])) for r in quality_rules]))
    .withColumn("_quarantined_at", F.current_timestamp())
)

(quarantine.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.quarantine_claims"))

print(f"quarantined {quarantine.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write silver claims

# COMMAND ----------

silver_claims = (claims.filter("_quality_passed = true")
    .select(
        F.col("claimid").alias("claim_id"),
        F.col("policyid").alias("policy_id"),
        "incident_date",
        "incident_state",
        "incident_city",
        "incident_location",
        "incident_type",
        "collision_type",
        "incident_severity",
        "injury", "property", "vehicle",
        "total_claim_amount",
        "authorities_contacted",
        F.col("number_of_vehicles_involved").cast("integer").alias("vehicles_involved"),
        "property_damage",
        F.col("bodily_injuries").cast("integer").alias("bodily_injuries"),
        F.col("witnesses").cast("integer").alias("witnesses"),
        "police_report_available",
        "claim_rejected",
        "claim_logged_on",
        "claim_processed_on",
        "processing_days",
        "sla_breach",
        "_source_file",
        F.current_timestamp().alias("_processed_at"),
    )
)

(silver_claims.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.claims"))

print(f"silver.claims: {silver_claims.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log DQ issues

# COMMAND ----------

dq_entries = []
for rn, _ in quality_rules:
    c = claims.filter(F.col(f"_fail_{rn}") == True).count()
    if c > 0:
        dq_entries.append({
            "entity": "claims", "rule_name": rn,
            "records_affected": c, "total_records": total,
            "affected_ratio": round(c / total, 4) if total > 0 else 0.0,
            "severity": "HIGH" if total > 0 and c / total > 0.05 else "MEDIUM" if total > 0 and c / total > 0.01 else "LOW",
            "suggested_fix": f"Investigate {rn} failures",
            "detected_at": datetime.now().isoformat(),
        })

if total > 0:
    dq_entries.append({
        "entity": "claims", "rule_name": "sla_breach_above_7_days",
        "records_affected": sla_cnt, "total_records": total,
        "affected_ratio": round(sla_cnt / total, 4) if total > 0 else 0.0,
        "severity": "HIGH",
        "suggested_fix": "Claims exceeding 7-day SLA benchmark need process review",
        "detected_at": datetime.now().isoformat(),
    })

if dq_entries:
    dq_df = spark.createDataFrame(dq_entries)
    (dq_df.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("`databricks-hackathon-insurance`.silver.dq_issues_claims"))
    display(dq_df)
else:
    print("no DQ issues to log")
