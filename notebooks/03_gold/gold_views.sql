-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold - Business views (pre-computed aggregations)
-- MAGIC Four materialized views, each tied to a specific business failure.
-- MAGIC These refresh on every pipeline run and serve instantly from the
-- MAGIC SQL warehouse. No need to re-scan fact tables on every dashboard load.

-- COMMAND ----------

-- Claims processing: avg time by region and severity, % exceeding 7-day benchmark.
-- Addresses the claims backlog failure (18 days avg vs 7-day target).

CREATE OR REFRESH MATERIALIZED VIEW vw_claims_processing
COMMENT 'Claims processing time vs 7-day benchmark by region and severity.'
AS
SELECT
  dc.region AS customer_region,
  fc.incident_severity,
  fc.incident_type,
  COUNT(*) AS total_claims,
  COUNT(CASE WHEN fc.is_rejected = FALSE THEN 1 END) AS approved_claims,
  COUNT(CASE WHEN fc.is_rejected = TRUE THEN 1 END) AS rejected_claims,
  ROUND(AVG(fc.days_to_process), 1) AS avg_days_to_process,
  MAX(fc.days_to_process) AS max_days_to_process,
  COUNT(CASE WHEN fc.days_to_process > 7 THEN 1 END) AS claims_exceeding_benchmark,
  ROUND(
    COUNT(CASE WHEN fc.days_to_process > 7 THEN 1 END) * 100.0 / NULLIF(COUNT(fc.days_to_process), 0),
    1
  ) AS pct_exceeding_benchmark,
  ROUND(AVG(fc.total_claim_amount), 2) AS avg_claim_amount
FROM LIVE.fact_claims fc
INNER JOIN LIVE.dim_customers dc ON fc.customer_key = dc.customer_key
WHERE fc.days_to_process IS NOT NULL
GROUP BY dc.region, fc.incident_severity, fc.incident_type;

-- COMMAND ----------

-- Regulatory customer count: deduplicated unique count by region.
-- Addresses the 12% inflated customer count problem.
-- This is the number that goes on the regulatory filing.

CREATE OR REFRESH MATERIALIZED VIEW vw_regulatory_customer_count
COMMENT 'Auditable unique customer count by region from deduplicated dim_customers.'
AS
SELECT
  region,
  COUNT(*) AS unique_customer_count,
  COUNT(CASE WHEN default_flag = 1 THEN 1 END) AS customers_in_default,
  COUNT(CASE WHEN hh_insurance = 1 THEN 1 END) AS customers_with_hh_insurance,
  COUNT(CASE WHEN car_loan = 1 THEN 1 END) AS customers_with_car_loan,
  ROUND(AVG(balance), 2) AS avg_balance
FROM LIVE.dim_customers
GROUP BY region

UNION ALL

SELECT
  'TOTAL' AS region,
  COUNT(*) AS unique_customer_count,
  COUNT(CASE WHEN default_flag = 1 THEN 1 END) AS customers_in_default,
  COUNT(CASE WHEN hh_insurance = 1 THEN 1 END) AS customers_with_hh_insurance,
  COUNT(CASE WHEN car_loan = 1 THEN 1 END) AS customers_with_car_loan,
  ROUND(AVG(balance), 2) AS avg_balance
FROM LIVE.dim_customers;

-- COMMAND ----------

-- Revenue leakage: unsold inventory by model and region.
-- Shows days on market, count past 60/90 day thresholds, at-risk revenue.

CREATE OR REFRESH MATERIALIZED VIEW vw_revenue_leakage
COMMENT 'Unsold inventory aging by model and region. Flags listings > 60 days.'
AS
SELECT
  dcar.model,
  dcar.name AS car_name,
  dcar.fuel,
  fs.region,
  COUNT(*) AS total_listings,
  COUNT(CASE WHEN fs.is_sold = TRUE THEN 1 END) AS sold_count,
  COUNT(CASE WHEN fs.is_sold = FALSE THEN 1 END) AS unsold_count,
  ROUND(AVG(CASE WHEN fs.is_sold = TRUE THEN fs.days_on_market END), 1) AS avg_days_to_sell,
  ROUND(AVG(CASE WHEN fs.is_sold = FALSE THEN fs.days_on_market END), 1) AS avg_days_unsold,
  COUNT(CASE WHEN fs.is_sold = FALSE AND fs.days_on_market > 60 THEN 1 END) AS unsold_over_60_days,
  COUNT(CASE WHEN fs.is_sold = FALSE AND fs.days_on_market > 90 THEN 1 END) AS unsold_over_90_days,
  ROUND(
    SUM(CASE WHEN fs.is_sold = FALSE AND fs.days_on_market > 60 THEN fs.original_selling_price ELSE 0 END),
    2
  ) AS at_risk_revenue
FROM LIVE.fact_sales fs
INNER JOIN LIVE.dim_cars dcar ON fs.car_key = dcar.car_key
GROUP BY dcar.model, dcar.name, dcar.fuel, fs.region;

-- COMMAND ----------

-- Claims by policy type: rejection rate and loss ratio by CSL tier and region.
-- Answers "which policy type had the highest rejection rate?"

CREATE OR REFRESH MATERIALIZED VIEW vw_claims_by_policy_type
COMMENT 'Claim rejection rates and loss ratio by policy CSL tier and region.'
AS
SELECT
  dp.policy_csl,
  dc.region,
  COUNT(*) AS total_claims,
  COUNT(CASE WHEN fc.is_rejected = TRUE THEN 1 END) AS rejected_claims,
  ROUND(
    COUNT(CASE WHEN fc.is_rejected = TRUE THEN 1 END) * 100.0 / COUNT(*),
    1
  ) AS rejection_rate_pct,
  ROUND(AVG(fc.total_claim_amount), 2) AS avg_claim_amount,
  ROUND(SUM(fc.total_claim_amount), 2) AS total_claim_value,
  ROUND(AVG(fc.days_to_process), 1) AS avg_days_to_process,
  ROUND(AVG(dp.policy_annual_premium), 2) AS avg_premium,
  ROUND(
    SUM(fc.total_claim_amount) / NULLIF(SUM(dp.policy_annual_premium), 0),
    2
  ) AS loss_ratio
FROM LIVE.fact_claims fc
INNER JOIN LIVE.dim_policies dp ON fc.policy_key = dp.policy_key
INNER JOIN LIVE.dim_customers dc ON fc.customer_key = dc.customer_key
GROUP BY dp.policy_csl, dc.region;
