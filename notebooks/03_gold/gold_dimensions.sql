-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold - Dimension tables
-- MAGIC Four dimensions: customers (from deduplicated silver), policies, cars,
-- MAGIC and a generated date dimension covering 2010-2030.
-- MAGIC
-- MAGIC Each dimension gets a surrogate integer key via ROW_NUMBER.
-- MAGIC Business keys (customer_id, policy_number, car_id) are kept as
-- MAGIC attributes for traceability.

-- COMMAND ----------

-- dim_customers: built from the deduplicated customers_unified.
-- This is THE source for regulatory customer counts. No duplicates.

CREATE OR REFRESH MATERIALIZED VIEW dim_customers
COMMENT 'Customer dimension from deduplicated registry. One row per unique customer.'
AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY customer_id) AS INT) AS customer_key,
  customer_id,
  region,
  state,
  city,
  job,
  education,
  marital_status,
  default_flag,
  balance,
  hh_insurance,
  car_loan
FROM prime_insurance_jellsinki_poc.silver.customers_unified;

-- COMMAND ----------

-- dim_policies: one row per policy. customer_id and car_id are excluded
-- because those relationships live on the fact table, not the dimension.

CREATE OR REFRESH MATERIALIZED VIEW dim_policies
COMMENT 'Policy dimension. One row per policy.'
AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY policy_number) AS INT) AS policy_key,
  policy_number,
  policy_bind_date,
  policy_state,
  policy_csl,
  policy_deductible,
  policy_annual_premium,
  umbrella_limit
FROM prime_insurance_jellsinki_poc.silver.policy_cleaned;

-- COMMAND ----------

-- dim_cars: shared dimension used by both fact_claims (via policy->car)
-- and fact_sales (directly). Numeric values already extracted in silver.

CREATE OR REFRESH MATERIALIZED VIEW dim_cars
COMMENT 'Vehicle catalog dimension. Shared by claims and sales facts.'
AS
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY car_id) AS INT) AS car_key,
  car_id,
  name,
  km_driven,
  fuel,
  transmission,
  mileage_kmpl,
  engine_cc,
  max_power_bhp,
  torque_nm,
  torque_rpm,
  seats,
  model
FROM prime_insurance_jellsinki_poc.silver.cars_cleaned;

-- COMMAND ----------

-- dim_date: generated calendar dimension. YYYYMMDD integer keys
-- for fast joins and human-readable results. Covers 2010-2030.

CREATE OR REFRESH MATERIALIZED VIEW dim_date
COMMENT 'Calendar date dimension. Generated for 2010-2030. Key format YYYYMMDD.'
AS
SELECT
  CAST(DATE_FORMAT(d.date_val, 'yyyyMMdd') AS INT) AS date_key,
  d.date_val AS full_date,
  YEAR(d.date_val) AS year,
  QUARTER(d.date_val) AS quarter,
  MONTH(d.date_val) AS month,
  DATE_FORMAT(d.date_val, 'MMMM') AS month_name,
  DAYOFWEEK(d.date_val) AS day_of_week,
  DATE_FORMAT(d.date_val, 'EEEE') AS day_name,
  CASE WHEN DAYOFWEEK(d.date_val) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
  SELECT EXPLODE(SEQUENCE(DATE'2010-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS date_val
) d;
