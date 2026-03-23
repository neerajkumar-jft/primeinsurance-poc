-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze - Customer ingestion
-- MAGIC 7 CSV files from 6 regional systems, each with different column names
-- MAGIC and a different subset of columns. We load them all into one table and
-- MAGIC let schema evolution add new columns as it encounters them. Files with
-- MAGIC CustomerID, Customer_ID, and cust_id all end up in the same table with
-- MAGIC those as separate columns. Silver sorts out the naming later.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_customers
COMMENT 'Raw customer data from all 7 regional CSV files. Schema varies across files - merged via schema evolution.'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/prime_insurance_jellsinki_poc/bronze/raw_data',
  format => 'csv',
  header => true,
  inferSchema => true,
  recursiveFileLookup => true,
  pathGlobFilter => 'customers_*.csv',
  schemaEvolutionMode => 'addNewColumns',
  rescuedDataColumn => '_rescued_data'
);
