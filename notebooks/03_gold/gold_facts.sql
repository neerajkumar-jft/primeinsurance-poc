-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold - Fact tables
-- MAGIC Two fact tables: fact_claims (one row per claim) and fact_sales
-- MAGIC (one row per sales listing, including unsold inventory).
-- MAGIC
-- MAGIC Both resolve surrogate keys by joining to dimension tables.
-- MAGIC fact_claims goes through policy_cleaned to get customer_id and car_id
-- MAGIC since claims reference policies, not customers/cars directly.

-- COMMAND ----------

-- fact_claims: grain is one claim.
-- Join path: claims -> policy (for customer_id, car_id) -> dimensions (for surrogate keys)
-- Measures: dollar amounts, days_to_process, is_rejected
-- days_to_process = DATEDIFF(claim_processed_on, claim_logged_on)
-- total_claim_amount = injury + property_amount + vehicle (with COALESCE for NULLs)

CREATE OR REFRESH MATERIALIZED VIEW fact_claims (
  CONSTRAINT valid_claim_id EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_policy_key EXPECT (policy_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_key EXPECT (customer_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_total EXPECT (total_claim_amount >= 0)
)
COMMENT 'Claims fact table. Grain: one row per claim.'
AS
SELECT
  c.claim_id,
  dp.policy_key,
  dc.customer_key,
  dcar.car_key,
  CAST(DATE_FORMAT(c.incident_date, 'yyyyMMdd') AS INT) AS incident_date_key,
  CAST(DATE_FORMAT(c.claim_logged_on, 'yyyyMMdd') AS INT) AS logged_date_key,

  -- Dollar measures
  c.injury,
  c.property_amount,
  c.vehicle,
  COALESCE(c.injury, 0) + COALESCE(c.property_amount, 0) + COALESCE(c.vehicle, 0) AS total_claim_amount,

  -- Processing time: NULL for rejected claims where processed_on is NULL
  CASE
    WHEN c.claim_processed_on IS NOT NULL AND c.claim_logged_on IS NOT NULL
    THEN DATEDIFF(c.claim_processed_on, c.claim_logged_on)
    ELSE NULL
  END AS days_to_process,

  CASE WHEN c.claim_rejected = 'Y' THEN TRUE ELSE FALSE END AS is_rejected,

  -- Degenerate dimensions (at the grain of the claim, no separate dim table)
  c.incident_type,
  c.incident_severity,
  c.collision_type,
  c.authorities_contacted,
  c.number_of_vehicles_involved,
  c.property_damage,
  c.bodily_injuries,
  c.witnesses,
  c.police_report_available,
  c.incident_state,
  c.incident_city

FROM prime_insurance_jellsinki_poc.silver.claims_cleaned c

-- Claims -> policy -> dimensions
INNER JOIN prime_insurance_jellsinki_poc.silver.policy_cleaned p
  ON c.policy_id = p.policy_number
INNER JOIN LIVE.dim_policies dp
  ON c.policy_id = dp.policy_number
INNER JOIN LIVE.dim_customers dc
  ON p.customer_id = dc.customer_id
INNER JOIN LIVE.dim_cars dcar
  ON p.car_id = dcar.car_id;

-- COMMAND ----------

-- fact_sales: grain is one sales listing. Includes unsold inventory.
-- days_on_market uses current_date() for unsold, so it updates every pipeline run.
-- is_sold = FALSE means unsold inventory (the revenue leakage signal).

CREATE OR REFRESH MATERIALIZED VIEW fact_sales (
  CONSTRAINT valid_sales_id EXPECT (sales_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_car_key EXPECT (car_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date_key EXPECT (date_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_price EXPECT (original_selling_price > 0)
)
COMMENT 'Sales fact table. Grain: one row per listing. Unsold inventory included with days_on_market from current_date.'
AS
SELECT
  s.sales_id,
  dcar.car_key,
  CAST(DATE_FORMAT(s.ad_placed_on, 'yyyyMMdd') AS INT) AS date_key,
  CASE
    WHEN s.sold_on IS NOT NULL
    THEN CAST(DATE_FORMAT(s.sold_on, 'yyyyMMdd') AS INT)
    ELSE NULL
  END AS sold_date_key,

  s.original_selling_price,
  CASE
    WHEN s.is_sold = TRUE THEN DATEDIFF(s.sold_on, s.ad_placed_on)
    ELSE DATEDIFF(current_date(), s.ad_placed_on)
  END AS days_on_market,
  s.is_sold,

  -- Degenerate dimensions (listing location, not customer location)
  s.region,
  s.state,
  s.city,
  s.seller_type,
  s.owner

FROM prime_insurance_jellsinki_poc.silver.sales_cleaned s
INNER JOIN LIVE.dim_cars dcar
  ON s.car_id = dcar.car_id;
