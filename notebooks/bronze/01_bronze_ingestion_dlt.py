# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion Pipeline
# MAGIC
# MAGIC This DLT pipeline ingests all 14 source files from 6 regional systems into 5 Bronze tables.
# MAGIC
# MAGIC **Design decisions:**
# MAGIC - Auto Loader (cloudFiles) tracks processed files, so re-runs with no new files add zero records
# MAGIC - mergeSchema handles different columns across regional files (e.g. CustomerID vs Customer_ID vs cust_id)
# MAGIC - No renaming, no cleaning, no type casting. Data lands exactly as received
# MAGIC - _source_file and _load_timestamp added to every record for traceability
# MAGIC
# MAGIC **Tables created:**
# MAGIC - primeins.bronze.customers (7 CSV files from all regions)
# MAGIC - primeins.bronze.claims (2 JSON files)
# MAGIC - primeins.bronze.policy (1 CSV file)
# MAGIC - primeins.bronze.sales (3 CSV files)
# MAGIC - primeins.bronze.cars (1 CSV file)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Volume path where all raw files are stored
VOLUME_PATH = "/Volumes/primeins/bronze/raw_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers
# MAGIC 7 CSV files across all 6 Insurance folders + root level.
# MAGIC Each file has different column names (CustomerID, Customer_ID, cust_id),
# MAGIC different column sets (some missing HHInsurance, Education), and
# MAGIC different region formats (W/C/E vs West/Central/East).
# MAGIC All of this is preserved as-is in Bronze.

# COMMAND ----------

@dlt.table(
    name="customers",
    comment="Raw customer data from 7 regional CSV files. Schema varies across files — preserved as received.",
    table_properties={"quality": "bronze"},
)
def customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{VOLUME_PATH}/*/customers_*.csv")
        .union(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/customers_*.csv")
        )
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_load_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims
# MAGIC 2 JSON files: claims_1.json in Insurance_6, claims_2.json at root level.
# MAGIC All fields are stored as strings in the source JSON (including numeric fields
# MAGIC like injury, property, vehicle). Date fields contain corrupted values like "27:00.0".
# MAGIC Everything is preserved as-is — Silver handles the type casting and date repair.

# COMMAND ----------

@dlt.table(
    name="claims",
    comment="Raw claims data from 2 regional JSON files. All fields are strings as received from source.",
    table_properties={"quality": "bronze"},
)
def claims():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
        .load(f"{VOLUME_PATH}/*/claims_*.json")
        .union(
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{VOLUME_PATH}/claims_*.json")
        )
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_load_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy
# MAGIC 1 CSV file from Insurance_5. Cleanest source file — consistent schema,
# MAGIC proper types. Contains foreign keys (customer_id, car_id) that link
# MAGIC customers to cars. Loaded as-is.

# COMMAND ----------

@dlt.table(
    name="policy",
    comment="Raw policy data from 1 CSV file. Contains coverage details and customer/car linkage.",
    table_properties={"quality": "bronze"},
)
def policy():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{VOLUME_PATH}/*/policy.csv")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_load_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales
# MAGIC 3 CSV files across Insurance_1, Insurance_2, Insurance_3.
# MAGIC Consistent schema across all three files (rare for this dataset).
# MAGIC Note: Insurance_1 has "Sales_2.csv" with capital S — the glob pattern
# MAGIC handles both cases. Records with no sold_on date represent unsold inventory.

# COMMAND ----------

@dlt.table(
    name="sales",
    comment="Raw sales/listing data from 3 regional CSV files. Consistent schema across regions.",
    table_properties={"quality": "bronze"},
)
def sales():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "[Ss]ales_*.csv")
        .load(f"{VOLUME_PATH}")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_load_timestamp", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cars
# MAGIC 1 CSV file from Insurance_4. Vehicle reference data.
# MAGIC Fields like mileage, engine, max_power include unit strings
# MAGIC ("23.4 kmpl", "1248 CC", "74 bhp") — preserved as-is in Bronze.

# COMMAND ----------

@dlt.table(
    name="cars",
    comment="Raw vehicle catalog from 1 CSV file. Unit strings in mileage/engine/power preserved as received.",
    table_properties={"quality": "bronze"},
)
def cars():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(f"{VOLUME_PATH}/*/cars.csv")
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_load_timestamp", F.current_timestamp())
    )
