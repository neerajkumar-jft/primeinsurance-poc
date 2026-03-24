-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver - Sales normalization
-- MAGIC Three issues here: dates are DD-MM-YYYY HH:MM instead of ISO,
-- MAGIC empty sold_on values are empty strings not NULLs, and some files
-- MAGIC have trailing junk rows that are entirely empty.
-- MAGIC
-- MAGIC Empty sold_on is NOT an error. It means the car hasn't sold yet.
-- MAGIC We keep those records and add an is_sold flag for downstream use.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sales_cleaned (
  CONSTRAINT valid_sales_id EXPECT (sales_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_car_id EXPECT (car_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_ad_placed EXPECT (ad_placed_on IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price EXPECT (original_selling_price > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_region EXPECT (region IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sold_after_ad EXPECT (sold_on IS NULL OR sold_on >= ad_placed_on)
)
COMMENT 'Normalized sales with parsed dates. Empty sold_on = unsold inventory, kept intentionally.'
AS
SELECT
  CAST(sales_id AS INT) AS sales_id,
  TO_TIMESTAMP(ad_placed_on, 'dd-MM-yyyy HH:mm') AS ad_placed_on,

  -- Empty string -> NULL. Unsold inventory, not an error.
  CASE
    WHEN TRIM(COALESCE(sold_on, '')) = '' THEN NULL
    ELSE TO_TIMESTAMP(sold_on, 'dd-MM-yyyy HH:mm')
  END AS sold_on,

  CAST(original_selling_price AS DOUBLE) AS original_selling_price,
  Region AS region,
  State AS state,
  City AS city,
  seller_type,
  owner,
  CAST(car_id AS INT) AS car_id,

  CASE
    WHEN TRIM(COALESCE(sold_on, '')) = '' THEN FALSE
    ELSE TRUE
  END AS is_sold,

  _source_file,
  _ingestion_timestamp
FROM prime_insurance_jellsinki_poc.bronze.raw_sales
WHERE
  -- Drop completely empty junk rows from CSV export
  NOT (sales_id IS NULL AND ad_placed_on IS NULL AND car_id IS NULL);

-- COMMAND ----------

-- Quarantine: sales records that failed validation

CREATE OR REFRESH MATERIALIZED VIEW quarantine_sales
COMMENT 'Sales records that failed validation. Junk rows already filtered, these are real records with data issues.'
AS
SELECT
  sales_id,
  ad_placed_on AS raw_ad_placed_on,
  sold_on AS raw_sold_on,
  original_selling_price,
  Region AS region,
  car_id,
  _source_file,
  _ingestion_timestamp,
  CASE
    WHEN sales_id IS NULL THEN 'null_sales_id'
    WHEN car_id IS NULL THEN 'null_car_id'
    WHEN ad_placed_on IS NULL THEN 'null_ad_placed_on'
    WHEN CAST(original_selling_price AS DOUBLE) <= 0 OR original_selling_price IS NULL THEN 'invalid_price'
    WHEN Region IS NULL THEN 'null_region'
    ELSE 'validation_failure'
  END AS quarantine_reason,
  current_timestamp() AS quarantined_at
FROM prime_insurance_jellsinki_poc.bronze.raw_sales
WHERE
  NOT (sales_id IS NULL AND ad_placed_on IS NULL AND car_id IS NULL)
  AND (
    sales_id IS NULL
    OR car_id IS NULL
    OR ad_placed_on IS NULL
    OR CAST(original_selling_price AS DOUBLE) <= 0
    OR original_selling_price IS NULL
    OR Region IS NULL
  );
