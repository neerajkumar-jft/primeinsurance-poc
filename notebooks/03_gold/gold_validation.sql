-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold layer validation
-- MAGIC Run each cell and screenshot the output for submission.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Dimension tables: row counts

-- COMMAND ----------

SELECT 'dim_customers' AS table_name, COUNT(*) AS rows FROM prime_insurance_jellsinki_poc.gold.dim_customers
UNION ALL SELECT 'dim_policies', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.dim_policies
UNION ALL SELECT 'dim_cars', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.dim_cars
UNION ALL SELECT 'dim_date', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.dim_date
UNION ALL SELECT 'fact_claims', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.fact_claims
UNION ALL SELECT 'fact_sales', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.fact_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Sample rows from fact_claims

-- COMMAND ----------

SELECT claim_id, policy_key, customer_key, car_key, incident_date_key,
       total_claim_amount, days_to_process, is_rejected, incident_severity
FROM prime_insurance_jellsinki_poc.gold.fact_claims LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Sample rows from fact_sales

-- COMMAND ----------

SELECT sales_id, car_key, date_key, sold_date_key,
       original_selling_price, days_on_market, is_sold, region
FROM prime_insurance_jellsinki_poc.gold.fact_sales LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Claims processing view (backlog failure)

-- COMMAND ----------

SELECT customer_region, incident_severity,
       total_claims, avg_days_to_process, claims_exceeding_benchmark, pct_exceeding_benchmark
FROM prime_insurance_jellsinki_poc.gold.vw_claims_processing
ORDER BY avg_days_to_process DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Regulatory customer count (inflated count failure)

-- COMMAND ----------

SELECT * FROM prime_insurance_jellsinki_poc.gold.vw_regulatory_customer_count
ORDER BY CASE WHEN region = 'TOTAL' THEN 'ZZZZ' ELSE region END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Revenue leakage (aging inventory failure)

-- COMMAND ----------

SELECT model, region, total_listings, sold_count, unsold_count,
       avg_days_to_sell, avg_days_unsold, unsold_over_60_days, at_risk_revenue
FROM prime_insurance_jellsinki_poc.gold.vw_revenue_leakage
WHERE unsold_count > 0
ORDER BY at_risk_revenue DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Claims by policy type (rejection rate)

-- COMMAND ----------

SELECT policy_csl, region, total_claims, rejected_claims,
       rejection_rate_pct, avg_claim_amount, avg_days_to_process, loss_ratio
FROM prime_insurance_jellsinki_poc.gold.vw_claims_by_policy_type
ORDER BY rejection_rate_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. End-to-end lineage: bronze -> silver -> gold counts

-- COMMAND ----------

SELECT 'bronze.raw_customers' AS layer_table, COUNT(*) AS rows FROM prime_insurance_jellsinki_poc.bronze.raw_customers
UNION ALL SELECT 'silver.customers_unified', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.customers_unified
UNION ALL SELECT 'gold.dim_customers', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.dim_customers
UNION ALL SELECT 'bronze.raw_claims', COUNT(*) FROM prime_insurance_jellsinki_poc.bronze.raw_claims
UNION ALL SELECT 'silver.claims_cleaned', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.claims_cleaned
UNION ALL SELECT 'gold.fact_claims', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.fact_claims
UNION ALL SELECT 'bronze.raw_sales', COUNT(*) FROM prime_insurance_jellsinki_poc.bronze.raw_sales
UNION ALL SELECT 'silver.sales_cleaned', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.sales_cleaned
UNION ALL SELECT 'gold.fact_sales', COUNT(*) FROM prime_insurance_jellsinki_poc.gold.fact_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Confirm aggregations reflect latest data

-- COMMAND ----------

-- Shows the pipeline run timestamp on the views vs fact tables
SELECT 'fact_claims last row' AS check_point,
       MAX(incident_date_key) AS latest_key
FROM prime_insurance_jellsinki_poc.gold.fact_claims
UNION ALL
SELECT 'vw_claims_processing rows', COUNT(*)
FROM prime_insurance_jellsinki_poc.gold.vw_claims_processing
UNION ALL
SELECT 'vw_regulatory_customer_count rows', COUNT(*)
FROM prime_insurance_jellsinki_poc.gold.vw_regulatory_customer_count
UNION ALL
SELECT 'vw_revenue_leakage rows', COUNT(*)
FROM prime_insurance_jellsinki_poc.gold.vw_revenue_leakage
UNION ALL
SELECT 'vw_claims_by_policy_type rows', COUNT(*)
FROM prime_insurance_jellsinki_poc.gold.vw_claims_by_policy_type;
