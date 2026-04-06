# Databricks notebook source
# MAGIC %md
# MAGIC # Before & After: Bronze vs Silver
# MAGIC
# MAGIC Visual proof of data harmonization across all 5 entities.
# MAGIC Each section shows raw Bronze data alongside cleaned Silver data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customers — Column name harmonization
# MAGIC
# MAGIC Bronze has 3 different customer ID columns, 2 region columns, 2 marital columns,
# MAGIC 2 education columns, and 2 city columns. Silver unifies all of them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### BEFORE: Bronze customers (raw column names from 7 files)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Shows the mess: CustomerID, Customer_ID, cust_id all as separate columns
# MAGIC SELECT
# MAGIC   CustomerID, Customer_ID, cust_id,
# MAGIC   Region, Reg,
# MAGIC   Marital_status, Marital,
# MAGIC   Education, Edu,
# MAGIC   City, City_in_state,
# MAGIC   _source_file
# MAGIC FROM primeins.bronze.customers
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### AFTER: Silver customers (unified columns)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean: one customer_id, one region, one marital, one education, one city
# MAGIC SELECT
# MAGIC   customer_id, region, state, city, job, marital, education,
# MAGIC   _source_file
# MAGIC FROM primeins.silver.customers
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Region abbreviation fix (customers_5.csv)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BEFORE: W, C, E, S abbreviations in customers_5
# MAGIC SELECT DISTINCT Reg, Region, _source_file
# MAGIC FROM primeins.bronze.customers
# MAGIC WHERE _source_file LIKE '%customers_5%'
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AFTER: Full region names
# MAGIC SELECT DISTINCT region, _source_file
# MAGIC FROM primeins.silver.customers
# MAGIC WHERE _source_file LIKE '%customers_5%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Swapped columns fix (customers_6.csv)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BEFORE: Marital_status has education values, Education has marital values
# MAGIC SELECT Marital_status, Education, _source_file
# MAGIC FROM primeins.bronze.customers
# MAGIC WHERE _source_file LIKE '%customers_6%'
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AFTER: Columns corrected
# MAGIC SELECT marital, education, _source_file
# MAGIC FROM primeins.silver.customers
# MAGIC WHERE _source_file LIKE '%customers_6%'
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Education typo fix (customers_5.csv)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BEFORE: "terto" typo
# MAGIC SELECT Education, COUNT(*) as cnt
# MAGIC FROM primeins.bronze.customers
# MAGIC WHERE _source_file LIKE '%customers_5%'
# MAGIC GROUP BY Education

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AFTER: "terto" corrected to "tertiary"
# MAGIC SELECT education, COUNT(*) as cnt
# MAGIC FROM primeins.silver.customers
# MAGIC WHERE _source_file LIKE '%customers_5%'
# MAGIC GROUP BY education

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Bronze' as layer, COUNT(*) as rows FROM primeins.bronze.customers
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Silver', COUNT(*) FROM primeins.silver.customers
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Quarantine', COUNT(*) FROM primeins.silver.quarantine_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Claims — Type casting & null cleanup
# MAGIC
# MAGIC Bronze has all fields as strings (from JSON source). Silver casts types,
# MAGIC replaces string "NULL" and "?" with actual nulls.

# COMMAND ----------

# MAGIC %md
# MAGIC ### BEFORE: Bronze claims (everything is a string)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All fields are strings, including numeric ones
# MAGIC -- Note: "NULL" as a literal string, "?" as a placeholder
# MAGIC SELECT
# MAGIC   ClaimID, PolicyID, injury, property, vehicle,
# MAGIC   incident_date, Claim_Logged_On, Claim_Processed_On,
# MAGIC   property_damage, police_report_available, collision_type
# MAGIC FROM primeins.bronze.claims
# MAGIC WHERE Claim_Processed_On = 'NULL' OR property_damage = '?'
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### AFTER: Silver claims (proper types, real nulls)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Types cast, string "NULL" and "?" replaced with actual null
# MAGIC SELECT
# MAGIC   claim_id, policy_id, injury_amount, property_amount, vehicle_amount,
# MAGIC   incident_date, claim_logged_on, claim_processed_on,
# MAGIC   property_damage, police_report_available, collision_type
# MAGIC FROM primeins.silver.claims
# MAGIC WHERE claim_processed_on IS NULL OR property_damage IS NULL
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### String NULL/? counts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BEFORE: String "NULL" and "?" counts in Bronze
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN Claim_Processed_On = 'NULL' THEN 1 ELSE 0 END) as processed_on_string_null,
# MAGIC   SUM(CASE WHEN Claim_Logged_On = 'NULL' THEN 1 ELSE 0 END) as logged_on_string_null,
# MAGIC   SUM(CASE WHEN property_damage = '?' THEN 1 ELSE 0 END) as property_damage_question,
# MAGIC   SUM(CASE WHEN police_report_available = '?' THEN 1 ELSE 0 END) as police_report_question,
# MAGIC   SUM(CASE WHEN collision_type = '?' THEN 1 ELSE 0 END) as collision_type_question
# MAGIC FROM primeins.bronze.claims

# COMMAND ----------

# MAGIC %sql
# MAGIC -- AFTER: All converted to actual nulls in Silver
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN claim_processed_on IS NULL THEN 1 ELSE 0 END) as processed_on_null,
# MAGIC   SUM(CASE WHEN claim_logged_on IS NULL THEN 1 ELSE 0 END) as logged_on_null,
# MAGIC   SUM(CASE WHEN property_damage IS NULL THEN 1 ELSE 0 END) as property_damage_null,
# MAGIC   SUM(CASE WHEN police_report_available IS NULL THEN 1 ELSE 0 END) as police_report_null,
# MAGIC   SUM(CASE WHEN collision_type IS NULL THEN 1 ELSE 0 END) as collision_type_null
# MAGIC FROM primeins.silver.claims

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Policy — Negative umbrella caught
# MAGIC
# MAGIC Policy is the cleanest source file. One record with negative umbrella_limit
# MAGIC was caught and quarantined.

# COMMAND ----------

# MAGIC %md
# MAGIC ### BEFORE: Bronze policy (negative umbrella_limit)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT policy_number, umbrella_limit, policy_annual_premium
# MAGIC FROM primeins.bronze.policy
# MAGIC WHERE umbrella_limit < 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### AFTER: Silver policy (bad record quarantined)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Record removed from clean table
# MAGIC SELECT COUNT(*) as silver_count FROM primeins.silver.policy
# MAGIC WHERE umbrella_limit < 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sitting in quarantine with rejection reason
# MAGIC SELECT policy_number, umbrella_limit, _rejection_reason
# MAGIC FROM primeins.silver.quarantine_policy

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Bronze' as layer, COUNT(*) as rows FROM primeins.bronze.policy
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Silver', COUNT(*) FROM primeins.silver.policy
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Quarantine', COUNT(*) FROM primeins.silver.quarantine_policy

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Sales — Empty rows removed
# MAGIC
# MAGIC 62.9% of Bronze sales rows were entirely blank (null sales_id, null everything).
# MAGIC Silver quarantines all of them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### BEFORE: Bronze sales (3,132 empty rows)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample of the blank padding rows
# MAGIC SELECT sales_id, ad_placed_on, sold_on, original_selling_price, Region
# MAGIC FROM primeins.bronze.sales
# MAGIC WHERE sales_id IS NULL
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count: 3,132 of 4,981 rows are entirely null
# MAGIC SELECT
# MAGIC   COUNT(*) as total_rows,
# MAGIC   SUM(CASE WHEN sales_id IS NULL THEN 1 ELSE 0 END) as null_rows,
# MAGIC   ROUND(SUM(CASE WHEN sales_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as null_pct
# MAGIC FROM primeins.bronze.sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### AFTER: Silver sales (clean, dates parsed)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Only valid rows, dates parsed to timestamps
# MAGIC SELECT sales_id, ad_placed_on, sold_on, original_selling_price, region
# MAGIC FROM primeins.silver.sales
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'Bronze' as layer, COUNT(*) as rows FROM primeins.bronze.sales
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Silver', COUNT(*) FROM primeins.silver.sales
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Quarantine', COUNT(*) FROM primeins.silver.quarantine_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Cars — Unit strings stripped
# MAGIC
# MAGIC Bronze has unit strings embedded in numeric fields (mileage, engine, max_power).
# MAGIC Silver extracts the numeric values.

# COMMAND ----------

# MAGIC %md
# MAGIC ### BEFORE: Bronze cars (unit strings in values)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT car_id, mileage, engine, max_power, torque
# MAGIC FROM primeins.bronze.cars
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### AFTER: Silver cars (numeric values extracted)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT car_id, mileage, mileage_unit, engine, max_power, torque
# MAGIC FROM primeins.silver.cars
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Entity | Bronze rows | Silver rows | Quarantined | What changed |
# MAGIC |--------|-------------|-------------|-------------|-------------|
# MAGIC | Customers | 3,605 | 3,605 | 0 | 3 ID columns -> 1, regions expanded, swapped columns fixed, typo corrected |
# MAGIC | Claims | 1,000 | 1,000 | 0 | All strings -> proper types, "NULL"/"?" -> actual null |
# MAGIC | Policy | 1,000 | 999 | 1 | Negative umbrella_limit quarantined |
# MAGIC | Sales | 4,981 | 1,849 | 3,132 | 62.9% empty rows quarantined, dates parsed |
# MAGIC | Cars | 2,500 | 2,500 | 0 | Unit strings stripped from mileage/engine/max_power |
