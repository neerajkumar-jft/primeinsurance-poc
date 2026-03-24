-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver - Customer harmonization and dedup
-- MAGIC This is the messiest entity. 7 files, 3 different ID column names,
-- MAGIC 5 column naming variations, abbreviated regions, a typo, and
-- MAGIC duplicate IDs between customers_1 and customers_7.
-- MAGIC
-- MAGIC We produce three tables here:
-- MAGIC - customers_harmonized: all 7 sources unified into one schema with quality rules
-- MAGIC - customers_unified: deduplicated version (resolves the customers_1/7 overlap)
-- MAGIC - quarantine_customers: records that failed validation, kept for audit

-- COMMAND ----------

-- Harmonize all 7 customer sources into a single consistent schema.
-- COALESCE merges the variant column names (CustomerID/Customer_ID/cust_id -> customer_id).
-- Region abbreviations get expanded, "terto" typo gets fixed.
-- Records with null customer_id, invalid region, or rescued data get dropped.

CREATE OR REFRESH MATERIALIZED VIEW customers_harmonized (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_region EXPECT (region IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_default_flag EXPECT (default_flag IN (0, 1)) ON VIOLATION DROP ROW,
  CONSTRAINT valid_balance EXPECT (balance >= 0)
)
COMMENT 'Unified customer schema from 7 regional files. Column names harmonized, regions expanded, typos fixed.'
AS
SELECT
  CAST(COALESCE(CustomerID, Customer_ID, cust_id) AS INT) AS customer_id,

  CASE
    WHEN COALESCE(Region, Reg) = 'W' THEN 'West'
    WHEN COALESCE(Region, Reg) = 'C' THEN 'Central'
    WHEN COALESCE(Region, Reg) = 'E' THEN 'East'
    WHEN COALESCE(Region, Reg) = 'S' THEN 'South'
    ELSE COALESCE(Region, Reg)
  END AS region,

  State AS state,
  COALESCE(City, City_in_state) AS city,
  Job AS job,

  -- Fix "terto" typo from customers_5
  CASE
    WHEN LOWER(COALESCE(Education, Edu)) = 'terto' THEN 'tertiary'
    ELSE COALESCE(Education, Edu)
  END AS education,

  COALESCE(Marital, Marital_status) AS marital_status,

  CAST(`Default` AS INT) AS default_flag,
  CAST(Balance AS INT) AS balance,
  CAST(HHInsurance AS INT) AS hh_insurance,
  CAST(CarLoan AS INT) AS car_loan,
  _source_file,
  _ingestion_timestamp
FROM prime_insurance_jellsinki_poc.bronze.raw_customers;

-- COMMAND ----------

-- Deduplicate customers. customers_7 (1604 records) overlaps with customers_1 (199 records)
-- on the same CustomerIDs but with different Job values. We prefer the record that has
-- more columns populated (customers_7 has Job, customers_1 doesn't).

CREATE OR REFRESH MATERIALIZED VIEW customers_unified
COMMENT 'Deduplicated customer registry. Overlap between customers_1 and customers_7 resolved by preferring the more complete record.'
AS
SELECT * EXCEPT(_row_num) FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY
        CASE WHEN job IS NOT NULL THEN 0 ELSE 1 END,
        _ingestion_timestamp DESC
    ) AS _row_num
  FROM LIVE.customers_harmonized
)
WHERE _row_num = 1;

-- COMMAND ----------

-- Quarantine: customer records that failed validation.
-- Inverse of the expectations on customers_harmonized.

CREATE OR REFRESH MATERIALIZED VIEW quarantine_customers
COMMENT 'Customer records that failed validation. Preserved for compliance audit.'
AS
SELECT
  COALESCE(CustomerID, Customer_ID, cust_id) AS customer_id,
  COALESCE(Region, Reg) AS raw_region,
  State AS state,
  COALESCE(City, City_in_state) AS city,
  Job AS job,
  COALESCE(Education, Edu) AS education,
  COALESCE(Marital, Marital_status) AS marital_status,
  `Default` AS default_flag,
  Balance AS balance,
  HHInsurance AS hh_insurance,
  CarLoan AS car_loan,
  _source_file,
  _ingestion_timestamp,
  CASE
    WHEN COALESCE(CustomerID, Customer_ID, cust_id) IS NULL THEN 'null_customer_id'
    WHEN COALESCE(Region, Reg) IS NULL THEN 'null_region'
    ELSE 'validation_failure'
  END AS quarantine_reason,
  current_timestamp() AS quarantined_at
FROM prime_insurance_jellsinki_poc.bronze.raw_customers
WHERE
  COALESCE(CustomerID, Customer_ID, cust_id) IS NULL
  OR COALESCE(Region, Reg) IS NULL;

-- COMMAND ----------

-- Quality log: aggregated counts of quarantined records across all entities.

CREATE OR REFRESH MATERIALIZED VIEW silver_quality_log
COMMENT 'Quality metrics across all silver quarantine tables. One row per entity per violation reason.'
AS
SELECT 'customers' AS entity, quarantine_reason, COUNT(*) AS record_count, current_timestamp() AS computed_at
FROM LIVE.quarantine_customers GROUP BY quarantine_reason
UNION ALL
SELECT 'claims' AS entity, quarantine_reason, COUNT(*) AS record_count, current_timestamp() AS computed_at
FROM LIVE.quarantine_claims GROUP BY quarantine_reason
UNION ALL
SELECT 'sales' AS entity, quarantine_reason, COUNT(*) AS record_count, current_timestamp() AS computed_at
FROM LIVE.quarantine_sales GROUP BY quarantine_reason;
