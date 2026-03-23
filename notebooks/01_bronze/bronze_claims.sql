-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze - Claims ingestion
-- MAGIC 2 JSON files, 499 and 501 records. Both share the same schema so no
-- MAGIC merge issues here. The date fields are broken (values like "27:00.0"
-- MAGIC instead of real dates) and "?" shows up where nulls should be.
-- MAGIC We leave all of that alone. Bronze just loads what it gets.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_claims
COMMENT 'Raw claims from 2 JSON files. 1000 records total. Corrupted dates and ? nulls left as-is.'
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingestion_timestamp
FROM STREAM read_files(
  '/Volumes/prime_insurance_jellsinki_poc/bronze/raw_data',
  format => 'json',
  inferSchema => true,
  recursiveFileLookup => true,
  pathGlobFilter => 'claims_*.json',
  schemaEvolutionMode => 'addNewColumns',
  rescuedDataColumn => '_rescued_data'
);
