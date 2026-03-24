# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer - Sales
# MAGIC
# MAGIC Sales data tracks car listings and sales across regions.
# MAGIC The key business problem: unsold cars aging unnoticed = revenue leakage.
# MAGIC
# MAGIC We need to calculate days-on-lot and flag aging inventory.

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce
from datetime import datetime

# COMMAND ----------

# auto-detect catalog
CATALOG = "prime-ins-jellsinki-poc"
spark.sql(f"USE CATALOG `{CATALOG}`")
print(f"using catalog: {CATALOG}")

# COMMAND ----------

try:
    cnt = spark.table(f"`{CATALOG}`.silver.sales").count()
    if cnt > 0:
        print(f"silver.sales already has {cnt} rows - skipping")
        dbutils.notebook.exit(f"SKIPPED - silver.sales has {cnt} rows")
except Exception:
    print("silver.sales doesn't exist yet - running transformation")

# COMMAND ----------

bronze_sales = spark.table(f"`{CATALOG}`.bronze.sales")
print(f"bronze sales: {bronze_sales.count()} rows")
bronze_sales.printSchema()

# COMMAND ----------

display(bronze_sales.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean & Transform

# COMMAND ----------

def clean_col_names(df):
    for c in df.columns:
        new_name = c.strip().lower().replace(" ", "_")
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)
    return df

sales_clean = clean_col_names(bronze_sales)

# COMMAND ----------

# parse dates
# actual format in the data: "22-01-2017 11:15" = dd-MM-yyyy HH:mm
# some records have NULL sold_on (unsold cars)
# clean NULL strings first, then parse dates via SQL try_to_date
for dc in ["ad_placed_on", "sold_on"]:
    sales_clean = sales_clean.withColumn(dc,
        F.when(
            (F.upper(F.trim(F.col(dc))) == "NULL") | (F.trim(F.col(dc)) == ""),
            F.lit(None)
        ).otherwise(F.col(dc))
    )

# use try_to_date via SQL - won't throw on bad input
sales_clean = (sales_clean
    .withColumn("ad_placed_on", F.expr("""coalesce(
        try_to_date(ad_placed_on, 'dd-MM-yyyy HH:mm'),
        try_to_date(ad_placed_on, 'dd-MM-yyyy'),
        try_to_date(ad_placed_on, 'yyyy-MM-dd'),
        try_to_date(ad_placed_on, 'MM/dd/yyyy')
    )"""))
    .withColumn("sold_on", F.expr("""coalesce(
        try_to_date(sold_on, 'dd-MM-yyyy HH:mm'),
        try_to_date(sold_on, 'dd-MM-yyyy'),
        try_to_date(sold_on, 'yyyy-MM-dd'),
        try_to_date(sold_on, 'MM/dd/yyyy')
    )"""))
)

# COMMAND ----------

# validate dates parsed ok
for dc in ["ad_placed_on", "sold_on"]:
    total = sales_clean.count()
    parsed = sales_clean.filter(F.col(dc).isNotNull()).count()
    print(f"{dc}: parsed {parsed}/{total}")

# COMMAND ----------

# cast price to double
sales_clean = sales_clean.withColumn(
    "original_selling_price",
    F.col("original_selling_price").cast("double")
)

# standardize region and state
sales_clean = (sales_clean
    .withColumn("region", F.initcap(F.trim(F.col("region"))))
    .withColumn("state", F.upper(F.trim(F.col("state"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("seller_type", F.initcap(F.trim(F.col("seller_type"))))
    .withColumn("owner", F.initcap(F.trim(F.col("owner"))))
)

# COMMAND ----------

# calculate days on lot and flag aging inventory
# if sold: days between ad placed and sold
# if unsold: days between ad placed and today
sales_clean = (sales_clean
    .withColumn("days_on_lot",
        F.when(F.col("sold_on").isNotNull(),
            F.datediff(F.col("sold_on"), F.col("ad_placed_on"))
        ).otherwise(
            F.datediff(F.current_date(), F.col("ad_placed_on"))
        )
    )
    .withColumn("is_sold", F.col("sold_on").isNotNull())
    .withColumn("aging_flag",
        F.when(
            (F.col("sold_on").isNull()) & (F.datediff(F.current_date(), F.col("ad_placed_on")) > 60),
            "AGING"
        ).when(
            (F.col("sold_on").isNull()) & (F.datediff(F.current_date(), F.col("ad_placed_on")) > 90),
            "CRITICAL"
        ).otherwise("OK")
    )
)

# COMMAND ----------

# how many aging?
print("=== Inventory Aging Summary ===")
display(sales_clean.groupBy("aging_flag").count())

unsold = sales_clean.filter(~F.col("is_sold")).count()
total = sales_clean.count()
print(f"\nunsold: {unsold}/{total} ({unsold/total*100 if total > 0 else 0:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Rules

# COMMAND ----------

quality_rules = [
    ("sales_id_null", F.col("sales_id").isNull()),
    ("ad_date_invalid", F.col("ad_placed_on").isNull()),
    ("price_not_positive", F.col("original_selling_price") <= 0),
    ("region_null", F.col("region").isNull() | (F.trim(F.col("region")) == "")),
    ("days_on_lot_negative", F.col("days_on_lot") < 0),
]

for rule_name, condition in quality_rules:
    sales_clean = sales_clean.withColumn(
        f"_fail_{rule_name}",
        F.when(condition, True).otherwise(False)
    )

fail_cols = [f"_fail_{r[0]}" for r in quality_rules]
sales_clean = sales_clean.withColumn(
    "_quality_passed",
    ~reduce(lambda a, b: a | b, [F.col(c) for c in fail_cols])
)

total = sales_clean.count()
passed = sales_clean.filter("_quality_passed = true").count()
failed = total - passed
print(f"passed: {passed} | failed: {failed}")

for rule_name, _ in quality_rules:
    cnt = sales_clean.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        print(f"  {rule_name}: {cnt}")

# COMMAND ----------

# quarantine
quarantine = (sales_clean
    .filter("_quality_passed = false")
    .withColumn("_quarantine_reason",
        F.concat_ws(", ", *[F.when(F.col(f"_fail_{r[0]}"), F.lit(r[0])) for r in quality_rules]))
    .withColumn("_quarantined_at", F.current_timestamp())
)

(quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.silver.quarantine_sales"))

print(f"quarantined {quarantine.count()} sales records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Sales

# COMMAND ----------

silver_sales = (sales_clean
    .filter("_quality_passed = true")
    .select(
        F.col("sales_id").cast("integer").alias("sales_id"),
        "ad_placed_on",
        "sold_on",
        "original_selling_price",
        "region",
        "state",
        "city",
        "seller_type",
        "owner",
        "car_id",
        "days_on_lot",
        "is_sold",
        "aging_flag",
        "_source_file",
        F.current_timestamp().alias("_processed_at"),
    )
)

(silver_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.silver.sales"))

print(f"silver.sales: {silver_sales.count()} rows")

# COMMAND ----------

# dq issues
dq_entries = []
for rule_name, _ in quality_rules:
    cnt = sales_clean.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        dq_entries.append({
            "entity": "sales",
            "rule_name": rule_name,
            "records_affected": cnt,
            "total_records": total,
            "affected_ratio": round(cnt / total, 4) if total > 0 else 0.0,
            "severity": "HIGH" if total > 0 and cnt / total > 0.05 else "MEDIUM" if total > 0 and cnt / total > 0.01 else "LOW",
            "suggested_fix": f"Fix {rule_name} in sales data",
            "detected_at": datetime.now().isoformat(),
        })

# log aging inventory as business quality issue
aging_count = sales_clean.filter(F.col("aging_flag").isin("AGING", "CRITICAL")).count()
if aging_count > 0:
    dq_entries.append({
        "entity": "sales",
        "rule_name": "inventory_aging_above_60_days",
        "records_affected": aging_count,
        "total_records": total,
        "affected_ratio": round(aging_count / total, 4),
        "severity": "HIGH",
        "suggested_fix": "Review aging unsold inventory for cross-regional redistribution",
        "detected_at": datetime.now().isoformat(),
    })

if dq_entries:
    dq_df = spark.createDataFrame(dq_entries)
    (dq_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"`{CATALOG}`.silver.dq_issues_sales"))
    display(dq_df)
