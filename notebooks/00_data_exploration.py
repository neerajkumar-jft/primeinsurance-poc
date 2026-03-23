# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Exploration - Understanding What We're Working With
# MAGIC
# MAGIC Before writing any pipeline code, let's dig into the raw files and understand:
# MAGIC - What schemas look like across regions
# MAGIC - Where the inconsistencies are
# MAGIC - What quality issues we'll need to handle in Silver
# MAGIC
# MAGIC This feeds directly into our Silver layer transformation rules.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

volume_path = "/Volumes/databricks-hackathon-insurance/bronze/workshop_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers
# MAGIC
# MAGIC We have 7 customer files across different regions. Each region used their own
# MAGIC ID format and conventions. Let's see how bad the inconsistencies are.

# COMMAND ----------

# load all customer CSVs and tag them with source file
customer_files = [
    ("Insurance 1/customers_1.csv", "region_1"),
    ("Insurance 2/customers_2.csv", "region_2"),
    ("Insurance 3/customers_3.csv", "region_3"),
    ("Insurance 4/customers_4.csv", "region_4"),
    ("Insurance 5/customers_5.csv", "region_5"),
    ("Insurance 6/customers_6.csv", "region_6"),
    ("customers_7.csv", "region_7"),
]

customer_dfs = []
for file_path, region_tag in customer_files:
    full_path = f"{volume_path}/{file_path}"
    try:
        df = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(full_path)
              .withColumn("_source_region", F.lit(region_tag))
              .withColumn("_source_file", F.lit(file_path)))
        customer_dfs.append(df)
        print(f"{region_tag}: {df.count()} rows, columns: {df.columns}")
    except Exception as e:
        print(f"failed to load {file_path}: {e}")

# COMMAND ----------

# check schema differences across files
print("=== Schema comparison across customer files ===\n")
for i, df in enumerate(customer_dfs):
    print(f"Region {i+1}: {[f'{c.name} ({c.dataType})' for c in df.schema.fields if not c.name.startswith('_')]}")
    print()

# COMMAND ----------

# combine all into one df for profiling
from functools import reduce

# need to align schemas first since they might differ
all_customers = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    customer_dfs
)

print(f"total customer records across all files: {all_customers.count()}")
display(all_customers.limit(20))

# COMMAND ----------

# profile customer data - check for nulls and distinct values
print("=== Customer Data Profile ===\n")

for col_name in [c for c in all_customers.columns if not c.startswith('_')]:
    total = all_customers.count()
    nulls = all_customers.filter(F.col(col_name).isNull()).count()
    distinct = all_customers.select(col_name).distinct().count()
    print(f"{col_name:20s} | nulls: {nulls:5d} ({nulls/total*100:.1f}%) | distinct: {distinct}")

# COMMAND ----------

# check customer ID formats across regions - this is where the regulatory pain is
print("=== CustomerID Format by Region ===\n")

for region_tag in [f"region_{i}" for i in range(1, 8)]:
    subset = all_customers.filter(F.col("_source_region") == region_tag)
    if subset.count() > 0:
        sample_ids = [row["CustomerID"] for row in subset.select("CustomerID").limit(5).collect()]
        print(f"{region_tag}: {sample_ids}")

# COMMAND ----------

# check for potential duplicates - same name appearing across regions
# this is the core of the regulatory problem
name_counts = (all_customers
    .filter(F.col("CustomerID").isNotNull())
    .groupBy(
        F.lower(F.trim(F.col("Region"))),
        F.lower(F.trim(F.col("State")))
    )
    .count()
    .orderBy(F.desc("count")))

display(name_counts)

# COMMAND ----------

# check region and state values for standardization issues
print("=== Region values ===")
display(all_customers.groupBy("Region").count().orderBy("Region"))

print("\n=== State values ===")
display(all_customers.groupBy("State").count().orderBy(F.desc("count")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Claims
# MAGIC
# MAGIC Two JSON files. Claims are the core of the backlog problem.
# MAGIC Need to check: date formats, amount fields, severity values, status fields.

# COMMAND ----------

# load claims JSON files
claims_1 = (spark.read.json(f"{volume_path}/Insurance 6/claims_1.json")
            .withColumn("_source_file", F.lit("claims_1.json")))
claims_2 = (spark.read.json(f"{volume_path}/claims_2.json")
            .withColumn("_source_file", F.lit("claims_2.json")))

print(f"claims_1: {claims_1.count()} rows")
print(f"claims_2: {claims_2.count()} rows")

# COMMAND ----------

# compare schemas between the two claims files
print("claims_1 schema:")
claims_1.printSchema()
print("\nclaims_2 schema:")
claims_2.printSchema()

# COMMAND ----------

# merge and profile
all_claims = claims_1.unionByName(claims_2, allowMissingColumns=True)
print(f"total claims: {all_claims.count()}\n")

for col_name in [c for c in all_claims.columns if not c.startswith('_')]:
    total = all_claims.count()
    nulls = all_claims.filter(F.col(col_name).isNull()).count()
    distinct = all_claims.select(col_name).distinct().count()
    print(f"{col_name:30s} | nulls: {nulls:5d} ({nulls/total*100:.1f}%) | distinct: {distinct}")

# COMMAND ----------

# check date formats in claims - expecting inconsistencies
print("=== Date field samples ===\n")
for date_col in ["incident_date", "Claim_Logged_On", "Claim_Processed_On"]:
    try:
        samples = [row[date_col] for row in all_claims.select(date_col).filter(F.col(date_col).isNotNull()).limit(10).collect()]
        print(f"{date_col}: {samples}")
    except Exception:
        print(f"{date_col}: column not found")

# COMMAND ----------

# check claim amounts and severity distribution
print("=== Severity distribution ===")
display(all_claims.groupBy("incident_severity").count().orderBy(F.desc("count")))

# COMMAND ----------

# check for amount outliers
for amt_col in ["injury", "property", "vehicle"]:
    try:
        stats = all_claims.select(
            F.mean(amt_col).alias("avg"),
            F.stddev(amt_col).alias("std"),
            F.min(amt_col).alias("min"),
            F.max(amt_col).alias("max")
        ).collect()[0]
        print(f"{amt_col:15s} | avg: {stats['avg']:.2f} | std: {stats['std']:.2f} | min: {stats['min']} | max: {stats['max']}")
    except Exception:
        print(f"{amt_col}: issue reading")

# COMMAND ----------

# check Claim_Rejected values
print("=== Claim Rejection Status ===")
display(all_claims.groupBy("Claim_Rejected").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Policy

# COMMAND ----------

policy_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{volume_path}/Insurance 5/policy.csv"))

print(f"policy records: {policy_df.count()}")
policy_df.printSchema()

# COMMAND ----------

display(policy_df.limit(10))

# COMMAND ----------

# profile policy
for col_name in policy_df.columns:
    total = policy_df.count()
    nulls = policy_df.filter(F.col(col_name).isNull()).count()
    distinct = policy_df.select(col_name).distinct().count()
    print(f"{col_name:25s} | nulls: {nulls:5d} ({nulls/total*100:.1f}%) | distinct: {distinct}")

# COMMAND ----------

# check policy_csl values - important for business queries
print("=== policy_csl values ===")
display(policy_df.groupBy("policy_csl").count().orderBy(F.desc("count")))

# COMMAND ----------

# check date formats
print("=== policy_bind_date samples ===")
samples = [row["policy_bind_date"] for row in policy_df.select("policy_bind_date").limit(10).collect()]
print(samples)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sales

# COMMAND ----------

sales_files = [
    "Insurance 1/Sales_2.csv",  # note the capital S - inconsistent naming
    "Insurance 2/sales_1.csv",
    "Insurance 3/sales_4.csv",
]

sales_dfs = []
for fp in sales_files:
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{volume_path}/{fp}")
          .withColumn("_source_file", F.lit(fp)))
    sales_dfs.append(df)
    print(f"{fp}: {df.count()} rows, columns: {df.columns}")

# COMMAND ----------

all_sales = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    sales_dfs
)

print(f"total sales: {all_sales.count()}")
display(all_sales.limit(10))

# COMMAND ----------

# profile sales
for col_name in [c for c in all_sales.columns if not c.startswith('_')]:
    total = all_sales.count()
    nulls = all_sales.filter(F.col(col_name).isNull()).count()
    distinct = all_sales.select(col_name).distinct().count()
    print(f"{col_name:25s} | nulls: {nulls:5d} ({nulls/total*100:.1f}%) | distinct: {distinct}")

# COMMAND ----------

# check for sold_on null - these are unsold cars (revenue leakage!)
unsold = all_sales.filter(F.col("sold_on").isNull()).count()
total = all_sales.count()
print(f"unsold cars: {unsold} out of {total} ({unsold/total*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cars

# COMMAND ----------

cars_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{volume_path}/Insurance 4/cars.csv"))

print(f"cars records: {cars_df.count()}")
cars_df.printSchema()

# COMMAND ----------

display(cars_df.limit(10))

# COMMAND ----------

# profile cars
for col_name in cars_df.columns:
    total = cars_df.count()
    nulls = cars_df.filter(F.col(col_name).isNull()).count()
    distinct = cars_df.select(col_name).distinct().count()
    print(f"{col_name:20s} | nulls: {nulls:5d} ({nulls/total*100:.1f}%) | distinct: {distinct}")

# COMMAND ----------

# check for weird values in fuel, transmission etc
print("=== fuel types ===")
display(cars_df.groupBy("fuel").count().orderBy(F.desc("count")))

print("=== transmission ===")
display(cars_df.groupBy("transmission").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of Findings
# MAGIC
# MAGIC ### Customers
# MAGIC - 7 files with different ID formats per region
# MAGIC - Same people appear across regions with different IDs (regulatory problem)
# MAGIC - Region naming inconsistencies (abbreviations vs full names)
# MAGIC - Some files may have extra columns (schema evolution)
# MAGIC
# MAGIC ### Claims
# MAGIC - 2 JSON files with potential schema drift
# MAGIC - Date formats likely inconsistent across files
# MAGIC - Need to validate amount fields (injury, property, vehicle) for negative/outlier values
# MAGIC - Claim_Rejected field needs standardization
# MAGIC
# MAGIC ### Policy
# MAGIC - Single CSV but needs date format standardization
# MAGIC - policy_csl values need validation
# MAGIC - Premium and deductible ranges need sanity checks
# MAGIC
# MAGIC ### Sales
# MAGIC - File naming inconsistent (Sales_2 vs sales_1)
# MAGIC - sold_on NULL = unsold cars (key metric for revenue leakage)
# MAGIC - Price values need range validation
# MAGIC
# MAGIC ### Cars
# MAGIC - Fuel/transmission values may have inconsistencies
# MAGIC - km_driven and max_power need type/range validation
# MAGIC - torque field likely has unit inconsistencies
