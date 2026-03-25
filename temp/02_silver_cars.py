# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer - Cars
# MAGIC
# MAGIC Car inventory data. Important for the revenue leakage analysis
# MAGIC and linking policies to vehicles.

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce
from datetime import datetime

# COMMAND ----------

try:
    cnt = spark.table("`databricks-hackathon-insurance`.silver.cars").count()
    if cnt > 0:
        print(f"silver.cars already has {cnt} rows - skipping")
        dbutils.notebook.exit(f"SKIPPED - silver.cars has {cnt} rows")
except Exception:
    print("silver.cars doesn't exist yet - running transformation")

# COMMAND ----------

bronze_cars = spark.table("`databricks-hackathon-insurance`.bronze.cars")
print(f"bronze cars: {bronze_cars.count()} rows")
bronze_cars.printSchema()

# COMMAND ----------

display(bronze_cars.limit(10))

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

cars_clean = clean_col_names(bronze_cars)

# COMMAND ----------

# cast numeric fields
cars_clean = (cars_clean
    .withColumn("km_driven", F.col("km_driven").cast("integer"))
    .withColumn("seats", F.col("seats").cast("integer"))
)

# standardize categorical columns
cars_clean = (cars_clean
    .withColumn("fuel", F.initcap(F.trim(F.col("fuel"))))
    .withColumn("transmission", F.initcap(F.trim(F.col("transmission"))))
    .withColumn("name", F.trim(F.col("name")))
    .withColumn("model", F.trim(F.col("model")))
)

# COMMAND ----------

# max_power and torque might have units mixed in (e.g. "82 bhp", "190 Nm")
# extract just the numeric part

cars_clean = (cars_clean
    .withColumn("max_power_raw", F.col("max_power"))
    .withColumn("max_power",
        F.regexp_extract(F.col("max_power"), r"([\d.]+)", 1).cast("double")
    )
    .withColumn("torque_raw", F.col("torque"))
    .withColumn("torque_value",
        F.regexp_extract(F.col("torque"), r"([\d.]+)", 1).cast("double")
    )
)

# COMMAND ----------

# check fuel types
print("=== Fuel types ===")
display(cars_clean.groupBy("fuel").count().orderBy(F.desc("count")))

print("\n=== Transmission ===")
display(cars_clean.groupBy("transmission").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Rules

# COMMAND ----------

quality_rules = [
    ("car_id_null", F.col("car_id").isNull() | (F.trim(F.col("car_id")) == "")),
    ("name_null", F.col("name").isNull() | (F.trim(F.col("name")) == "")),
    ("km_driven_negative", F.col("km_driven") < 0),
    ("seats_invalid", (F.col("seats") < 1) | (F.col("seats") > 20)),
    ("fuel_invalid", ~F.col("fuel").isin("Diesel", "Petrol", "Cng", "Lpg", "Electric")),
]

for rule_name, condition in quality_rules:
    cars_clean = cars_clean.withColumn(
        f"_fail_{rule_name}",
        F.when(condition, True).otherwise(False)
    )

fail_cols = [f"_fail_{r[0]}" for r in quality_rules]
cars_clean = cars_clean.withColumn(
    "_quality_passed",
    ~reduce(lambda a, b: a | b, [F.col(c) for c in fail_cols])
)

total = cars_clean.count()
passed = cars_clean.filter("_quality_passed = true").count()
failed = total - passed
print(f"passed: {passed} | failed: {failed}")

for rule_name, _ in quality_rules:
    cnt = cars_clean.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        print(f"  {rule_name}: {cnt}")

# COMMAND ----------

# quarantine
quarantine = (cars_clean
    .filter("_quality_passed = false")
    .withColumn("_quarantine_reason",
        F.concat_ws(", ", *[F.when(F.col(f"_fail_{r[0]}"), F.lit(r[0])) for r in quality_rules]))
    .withColumn("_quarantined_at", F.current_timestamp())
)

(quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.quarantine_cars"))

print(f"quarantined {quarantine.count()} cars records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Cars

# COMMAND ----------

silver_cars = (cars_clean
    .filter("_quality_passed = true")
    .select(
        "car_id",
        "name",
        "km_driven",
        "fuel",
        "transmission",
        "mileage",
        "engine",
        "max_power",
        "max_power_raw",
        "torque_value",
        "torque_raw",
        "seats",
        "model",
        "_source_file",
        F.current_timestamp().alias("_processed_at"),
    )
)

(silver_cars.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.silver.cars"))

print(f"silver.cars: {silver_cars.count()} rows")

# COMMAND ----------

# dq issues
dq_entries = []
for rule_name, _ in quality_rules:
    cnt = cars_clean.filter(F.col(f"_fail_{rule_name}") == True).count()
    if cnt > 0:
        dq_entries.append({
            "entity": "cars",
            "rule_name": rule_name,
            "records_affected": cnt,
            "total_records": total,
            "affected_ratio": round(cnt / total, 4) if total > 0 else 0.0,
            "severity": "HIGH" if total > 0 and cnt / total > 0.05 else "MEDIUM" if total > 0 and cnt / total > 0.01 else "LOW",
            "suggested_fix": f"Fix {rule_name} in cars data",
            "detected_at": datetime.now().isoformat(),
        })

if dq_entries:
    dq_df = spark.createDataFrame(dq_entries)
    (dq_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("`databricks-hackathon-insurance`.silver.dq_issues_cars"))
    display(dq_df)
else:
    print("no quality issues in cars data")
