-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver layer validation
-- MAGIC Run each cell and screenshot the output for submission.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Before/after: Customers (bronze vs silver)

-- COMMAND ----------

-- BEFORE: bronze has 3 ID columns, Reg/Region, City/City_in_state, Edu/Education
SELECT CustomerID, Customer_ID, cust_id, Reg, Region, City, City_in_state, Edu, Education, Marital, Marital_status
FROM prime_insurance_jellsinki_poc.bronze.raw_customers LIMIT 10;

-- COMMAND ----------

-- AFTER: single customer_id, region, city, education, marital_status. Abbreviations expanded, typo fixed.
SELECT customer_id, region, city, education, marital_status, job, default_flag, balance, _source_file
FROM prime_insurance_jellsinki_poc.silver.customers_unified LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Before/after: Claims (bronze vs silver)

-- COMMAND ----------

-- BEFORE: broken dates like "27:00.0", "?" placeholders, string "NULL", all types are strings
SELECT ClaimID, PolicyID, incident_date, Claim_Logged_On, Claim_Processed_On, property_damage, police_report_available, injury
FROM prime_insurance_jellsinki_poc.bronze.raw_claims LIMIT 10;

-- COMMAND ----------

-- AFTER: real dates, proper nulls, numeric types
SELECT claim_id, policy_id, incident_date, claim_logged_on, claim_processed_on, property_damage, police_report_available, injury
FROM prime_insurance_jellsinki_poc.silver.claims_cleaned LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Before/after: Sales (bronze vs silver)

-- COMMAND ----------

-- BEFORE: DD-MM-YYYY dates as strings, empty sold_on as empty string
SELECT sales_id, ad_placed_on, sold_on, original_selling_price, Region, car_id
FROM prime_insurance_jellsinki_poc.bronze.raw_sales LIMIT 10;

-- COMMAND ----------

-- AFTER: proper timestamps, NULL for unsold, is_sold flag added
SELECT sales_id, ad_placed_on, sold_on, original_selling_price, region, car_id, is_sold
FROM prime_insurance_jellsinki_poc.silver.sales_cleaned LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Before/after: Cars (bronze vs silver)

-- COMMAND ----------

-- BEFORE: units jammed into values
SELECT car_id, name, mileage, engine, max_power, torque
FROM prime_insurance_jellsinki_poc.bronze.raw_cars LIMIT 10;

-- COMMAND ----------

-- AFTER: numeric values extracted, torque split into nm and rpm
SELECT car_id, name, mileage_kmpl, engine_cc, max_power_bhp, torque_nm, torque_rpm
FROM prime_insurance_jellsinki_poc.silver.cars_cleaned LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Before/after: Policy (bronze vs silver)

-- COMMAND ----------

-- BEFORE: raw with "policy_deductable" typo
SELECT policy_number, policy_bind_date, policy_state, policy_deductable, policy_annual_premium, car_id, customer_id
FROM prime_insurance_jellsinki_poc.bronze.raw_policy LIMIT 10;

-- COMMAND ----------

-- AFTER: types cast, typo fixed to policy_deductible
SELECT policy_number, policy_bind_date, policy_state, policy_deductible, policy_annual_premium, car_id, customer_id
FROM prime_insurance_jellsinki_poc.silver.policy_cleaned LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Quality rules: quarantine counts per entity

-- COMMAND ----------

SELECT * FROM prime_insurance_jellsinki_poc.silver.silver_quality_log ORDER BY entity, quarantine_reason;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Quarantine detail: customers

-- COMMAND ----------

SELECT quarantine_reason, COUNT(*) as records
FROM prime_insurance_jellsinki_poc.silver.quarantine_customers
GROUP BY quarantine_reason ORDER BY records DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Quarantine detail: claims

-- COMMAND ----------

SELECT quarantine_reason, COUNT(*) as records
FROM prime_insurance_jellsinki_poc.silver.quarantine_claims
GROUP BY quarantine_reason ORDER BY records DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Quarantine detail: sales

-- COMMAND ----------

SELECT quarantine_reason, COUNT(*) as records
FROM prime_insurance_jellsinki_poc.silver.quarantine_sales
GROUP BY quarantine_reason ORDER BY records DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Record counts: bronze vs silver

-- COMMAND ----------

SELECT 'bronze.raw_customers' AS table_name, COUNT(*) AS records FROM prime_insurance_jellsinki_poc.bronze.raw_customers
UNION ALL SELECT 'silver.customers_harmonized', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.customers_harmonized
UNION ALL SELECT 'silver.customers_unified', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.customers_unified
UNION ALL SELECT 'silver.quarantine_customers', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.quarantine_customers
UNION ALL SELECT 'bronze.raw_claims', COUNT(*) FROM prime_insurance_jellsinki_poc.bronze.raw_claims
UNION ALL SELECT 'silver.claims_cleaned', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.claims_cleaned
UNION ALL SELECT 'silver.quarantine_claims', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.quarantine_claims
UNION ALL SELECT 'bronze.raw_policy', COUNT(*) FROM prime_insurance_jellsinki_poc.bronze.raw_policy
UNION ALL SELECT 'silver.policy_cleaned', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.policy_cleaned
UNION ALL SELECT 'bronze.raw_sales', COUNT(*) FROM prime_insurance_jellsinki_poc.bronze.raw_sales
UNION ALL SELECT 'silver.sales_cleaned', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.sales_cleaned
UNION ALL SELECT 'silver.quarantine_sales', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.quarantine_sales
UNION ALL SELECT 'bronze.raw_cars', COUNT(*) FROM prime_insurance_jellsinki_poc.bronze.raw_cars
UNION ALL SELECT 'silver.cars_cleaned', COUNT(*) FROM prime_insurance_jellsinki_poc.silver.cars_cleaned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. Spot check: "terto" typo fixed in customers

-- COMMAND ----------

-- Should return 0 rows if the typo was fixed
SELECT customer_id, education, _source_file
FROM prime_insurance_jellsinki_poc.silver.customers_harmonized
WHERE education = 'terto';

-- COMMAND ----------

-- Should show "tertiary" for customers_5 records
SELECT customer_id, education, _source_file
FROM prime_insurance_jellsinki_poc.silver.customers_harmonized
WHERE _source_file LIKE '%customers_5%' LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 12. Spot check: region abbreviations expanded

-- COMMAND ----------

-- Should return 0 rows with single-letter regions
SELECT customer_id, region, _source_file
FROM prime_insurance_jellsinki_poc.silver.customers_harmonized
WHERE region IN ('W', 'C', 'E', 'S');

-- COMMAND ----------

-- Should show full region names for customers_5
SELECT customer_id, region, _source_file
FROM prime_insurance_jellsinki_poc.silver.customers_harmonized
WHERE _source_file LIKE '%customers_5%' LIMIT 5;
