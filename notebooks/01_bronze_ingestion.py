# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC Load all source files into Delta tables exactly as they are.
# MAGIC No transformations, no cleaning - just get the data in reliably.
# MAGIC
# MAGIC Key requirements:
# MAGIC - Preserve raw data exactly as received
# MAGIC - Track source file and ingestion timestamp for lineage
# MAGIC - Handle schema evolution (new columns showing up)
# MAGIC - Idempotent - rerunning doesn't create duplicates

# COMMAND ----------

# auto-detect catalog
CATALOG = "prime-ins-jellsinki-poc"
spark.sql(f"USE CATALOG `{CATALOG}`")
print(f"using catalog: {CATALOG}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from functools import reduce

# COMMAND ----------

volume_path = f"/Volumes/{CATALOG}/bronze/source_files"

# COMMAND ----------

# skip if all bronze tables already have data
required_tables = ["customers", "claims", "policy", "sales", "cars"]
all_exist = True
for t in required_tables:
    try:
        cnt = spark.table(f"`{CATALOG}`.bronze.{t}").count()
        if cnt == 0:
            all_exist = False
            break
    except Exception:
        all_exist = False
        break

if all_exist:
    print("all bronze tables already populated - skipping ingestion")
    for t in required_tables:
        cnt = spark.table(f"`{CATALOG}`.bronze.{t}").count()
        print(f"  bronze.{t}: {cnt} rows")
    dbutils.notebook.exit("SKIPPED - bronze tables already exist")

print("one or more bronze tables missing - running ingestion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Bronze Customers
# MAGIC
# MAGIC 7 CSV files from different regional systems.
# MAGIC Each has its own column names which is part of the mess we need to fix in Silver.
# MAGIC
# MAGIC Schema differences found:
# MAGIC - customers_1: `CustomerID`, `Reg` (not Region), `Marital_status`, no Job column
# MAGIC - customers_2: `Customer_ID`, `City_in_state`, `Edu`, no HHInsurance, no Marital
# MAGIC - customers_3: `cust_id` (lowercase)
# MAGIC - customers_4: no Education column
# MAGIC - customers_6: `Marital_status` instead of Marital
# MAGIC - customers_7: standard schema (reference)

# COMMAND ----------

print("loading customers...")
customer_files = [
    "Insurance 1/customers_1.csv",
    "Insurance 2/customers_2.csv",
    "Insurance 3/customers_3.csv",
    "Insurance 4/customers_4.csv",
    "Insurance 5/customers_5.csv",
    "Insurance 6/customers_6.csv",
    "customers_7.csv",
]

customer_dfs = []
for fp in customer_files:
    full_path = f"{volume_path}/{fp}"
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(full_path)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(fp))
    )
    customer_dfs.append(df)
    print(f"  {fp}: {df.count()} rows, cols: {df.columns}")

# union with allowMissingColumns so different schemas merge cleanly
bronze_customers = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    customer_dfs
)

(bronze_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.bronze.customers"))

print(f"\nbronze.customers: {bronze_customers.count()} rows, {len(bronze_customers.columns)} columns")

# COMMAND ----------

# check what columns we ended up with after merging all schemas
print("merged schema:")
spark.table(f"`{CATALOG}`.bronze.customers").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze Claims
# MAGIC
# MAGIC 2 JSON files. Both are arrays of objects.
# MAGIC Claims are the core of the processing backlog problem.

# COMMAND ----------

print("loading claims...")

claims_1 = (spark.read
    .option("multiLine", "true")
    .json(f"{volume_path}/Insurance 6/claims_1.json")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("Insurance 6/claims_1.json")))

claims_2 = (spark.read
    .option("multiLine", "true")
    .json(f"{volume_path}/claims_2.json")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("claims_2.json")))

print(f"  claims_1: {claims_1.count()} rows, cols: {claims_1.columns}")
print(f"  claims_2: {claims_2.count()} rows, cols: {claims_2.columns}")

bronze_claims = claims_1.unionByName(claims_2, allowMissingColumns=True)

(bronze_claims.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.bronze.claims"))

print(f"\nbronze.claims: {bronze_claims.count()} rows")

# COMMAND ----------

display(spark.table(f"`{CATALOG}`.bronze.claims").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Policy

# COMMAND ----------

print("loading policy...")
bronze_policy = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{volume_path}/Insurance 5/policy.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("Insurance 5/policy.csv"))
)

(bronze_policy.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.bronze.policy"))

print(f"bronze.policy: {bronze_policy.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze Sales
# MAGIC
# MAGIC File naming is inconsistent (Sales_2 vs sales_1) but schemas match.

# COMMAND ----------

print("loading sales...")
sales_files = [
    "Insurance 1/Sales_2.csv",
    "Insurance 2/sales_1.csv",
    "Insurance 3/sales_4.csv",
]

sales_dfs = []
for fp in sales_files:
    df = (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{volume_path}/{fp}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(fp))
    )
    sales_dfs.append(df)
    print(f"  {fp}: {df.count()} rows")

bronze_sales = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    sales_dfs
)

(bronze_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.bronze.sales"))

print(f"\nbronze.sales: {bronze_sales.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze Cars

# COMMAND ----------

print("loading cars...")
bronze_cars = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{volume_path}/Insurance 4/cars.csv")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("Insurance 4/cars.csv"))
)

(bronze_cars.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"`{CATALOG}`.bronze.cars"))

print(f"bronze.cars: {bronze_cars.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify All Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bronze;

# COMMAND ----------

tables = ["customers", "claims", "policy", "sales", "cars"]
print("=== Bronze Layer Summary ===\n")
for t in tables:
    tbl = spark.table(f"`{CATALOG}`.bronze.{t}")
    print(f"  bronze.{t:15s} | {tbl.count():6d} rows | {len(tbl.columns):3d} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Evolution Handling
# MAGIC
# MAGIC When Insurance 3 sends a file with a new `Birth Date` column:
# MAGIC 1. `unionByName(allowMissingColumns=True)` handles it - new column gets NULL for other files
# MAGIC 2. `overwriteSchema=true` on write updates the Delta table schema
# MAGIC 3. Pipeline does NOT fail
# MAGIC 4. Column name with space preserved as-is in Bronze, cleaned in Silver
# MAGIC
# MAGIC ## Idempotency
# MAGIC - Overwrite mode means rerunning produces identical results
# MAGIC - No duplicates, timestamps refresh but data stays the same
