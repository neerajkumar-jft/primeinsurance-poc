-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver - Policy standardization
-- MAGIC Cleanest source file we have. Dates are already YYYY-MM-DD, column
-- MAGIC names are consistent, numerics inferred correctly. Not much to fix
-- MAGIC here besides type casting and renaming "policy_deductable" (typo in
-- MAGIC source) to "policy_deductible".

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW policy_cleaned (
  CONSTRAINT valid_policy_number EXPECT (policy_number IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_car_id EXPECT (car_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_bind_date EXPECT (policy_bind_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_premium EXPECT (policy_annual_premium > 0),
  CONSTRAINT valid_deductible EXPECT (policy_deductible >= 0)
)
COMMENT 'Standardized policy records. Source was clean, minimal transformations needed.'
AS
SELECT
  CAST(policy_number AS INT) AS policy_number,
  CAST(policy_bind_date AS DATE) AS policy_bind_date,
  policy_state,
  policy_csl,
  CAST(policy_deductable AS INT) AS policy_deductible,
  CAST(policy_annual_premium AS DOUBLE) AS policy_annual_premium,
  CAST(umbrella_limit AS INT) AS umbrella_limit,
  CAST(car_id AS INT) AS car_id,
  CAST(customer_id AS INT) AS customer_id,
  _source_file,
  _ingestion_timestamp
FROM prime_insurance_jellsinki_poc.bronze.raw_policy;
