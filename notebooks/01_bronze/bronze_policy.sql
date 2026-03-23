-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze - Policy ingestion
-- MAGIC One file, 1000 records. Links customers to cars through policy_number.
-- MAGIC Dates here are clean (YYYY-MM-DD). Probably the tidiest file we got
-- MAGIC from the regional systems.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_policy
COMMENT 'Raw policy data. 1000 records linking customers to cars via policy_number.'
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
  pathGlobFilter => 'policy*.csv',
  schemaEvolutionMode => 'addNewColumns',
  rescuedDataColumn => '_rescued_data'
);
