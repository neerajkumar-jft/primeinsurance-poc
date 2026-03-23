-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze - Sales ingestion
-- MAGIC 3 CSV files from different regions. Dates are DD-MM-YYYY HH:MM which
-- MAGIC Silver will need to parse. Some sold_on values are blank, meaning
-- MAGIC those cars haven't sold yet. That's the revenue leakage problem,
-- MAGIC but we don't flag it here. Just load and track.
-- MAGIC
-- MAGIC File names are mixed case (sales_1, Sales_2, sales_4) so the
-- MAGIC glob pattern handles both.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE raw_sales
COMMENT 'Raw sales from 3 regional CSV files. Empty sold_on = unsold inventory.'
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
  pathGlobFilter => '[sS]ales_*.csv',
  schemaEvolutionMode => 'addNewColumns',
  rescuedDataColumn => '_rescued_data'
);
