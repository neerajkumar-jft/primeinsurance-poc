# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer - DLT Pipeline (Auto Loader)
# MAGIC
# MAGIC This is the DLT version of our Bronze ingestion.
# MAGIC Uses Auto Loader (cloudFiles) for incremental, schema-evolving ingestion.
# MAGIC
# MAGIC To run this: Create a DLT Pipeline in Databricks UI pointing to this notebook.
# MAGIC
# MAGIC Why Auto Loader instead of spark.read?
# MAGIC - Incremental: only processes NEW files (tracks via checkpoint)
# MAGIC - Schema evolution: new columns added automatically without failure
# MAGIC - Idempotent: rerun safe, no duplicates
# MAGIC - Production-grade: what Databricks recommends for real workloads

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

volume_path = "/Volumes/databricks-hackathon-insurance/bronze/workshop_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers (7 CSV files across regions)
# MAGIC
# MAGIC Each regional system uses different ID formats.
# MAGIC Auto Loader with mergeSchema handles columns that differ across files.
# MAGIC
# MAGIC Note: _metadata.file_path is used instead of input_file_name() — required in Unity Catalog.

# COMMAND ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from all 6 regional systems, loaded incrementally via Auto Loader",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    regional = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .option("mergeSchema", "true")
            .load(f"{volume_path}/Insurance */customers_*.csv")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )
    root = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .load(f"{volume_path}/customers_*.csv")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )
    return regional.unionByName(root, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Claims (2 JSON files)
# MAGIC
# MAGIC JSON files from different regions. Auto Loader handles schema differences.

# COMMAND ----------

@dlt.table(
    name="bronze_claims",
    comment="Raw claims data from regional systems (JSON format)",
    table_properties={"quality": "bronze"}
)
def bronze_claims():
    claims_regional = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("multiLine", "true")
            .load(f"{volume_path}/Insurance 6/claims_*.json")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )
    claims_root = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("multiLine", "true")
            .load(f"{volume_path}/claims_*.json")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )
    return claims_regional.unionByName(claims_root, allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Policy
# MAGIC
# MAGIC Single file — Auto Loader watches the parent directory and filters by filename.

# COMMAND ----------

@dlt.table(
    name="bronze_policy",
    comment="Raw policy data",
    table_properties={"quality": "bronze"}
)
def bronze_policy():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .option("pathGlobFilter", "policy*.csv")
            .load(f"{volume_path}/Insurance 5/")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales (3 CSV files)
# MAGIC
# MAGIC Note: file naming is inconsistent (Sales_2 vs sales_1).
# MAGIC Auto Loader's glob pattern picks up both.

# COMMAND ----------

@dlt.table(
    name="bronze_sales",
    comment="Raw sales/inventory data from regional systems",
    table_properties={"quality": "bronze"}
)
def bronze_sales():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .option("mergeSchema", "true")
            .load(f"{volume_path}/Insurance */[Ss]ales_*.csv")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cars
# MAGIC
# MAGIC Single file — Auto Loader watches the parent directory and filters by filename.

# COMMAND ----------

@dlt.table(
    name="bronze_cars",
    comment="Raw vehicle inventory data",
    table_properties={"quality": "bronze"}
)
def bronze_cars():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .option("pathGlobFilter", "cars*.csv")
            .load(f"{volume_path}/Insurance 4/")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Evolution Handling
# MAGIC
# MAGIC When Insurance 3 sends a file with a new `Birth Date` column:
# MAGIC
# MAGIC 1. `cloudFiles.schemaEvolutionMode = "addNewColumns"` detects the new column
# MAGIC 2. The column is added to the Bronze table schema automatically
# MAGIC 3. Existing records get NULL for `Birth Date`
# MAGIC 4. The pipeline does NOT fail or stop
# MAGIC 5. The column name with space is preserved as-is in Bronze
# MAGIC 6. We rename/handle it properly in Silver layer
# MAGIC
# MAGIC This is the key advantage of Auto Loader over static `spark.read` - it handles
# MAGIC schema drift gracefully without manual intervention.
