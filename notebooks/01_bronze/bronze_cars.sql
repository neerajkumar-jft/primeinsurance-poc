-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze - Cars ingestion
-- MAGIC Single CSV, 2500 records. Vehicle catalog. Relatively clean compared
-- MAGIC to customers and claims, but mileage and engine columns have units
-- MAGIC baked into the values ("23.4 kmpl", "1248 CC"). Silver parses those out.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_cars
COMMENT 'Raw vehicle catalog. 2500 records. Units embedded in numeric values.'
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
  pathGlobFilter => 'cars*.csv',
  schemaEvolutionMode => 'addNewColumns',
  rescuedDataColumn => '_rescued_data'
);
