-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PrimeInsurance - Catalog & Schema Setup
-- MAGIC Creates the Unity Catalog structure for the medallion architecture.

-- COMMAND ----------

-- Create the main catalog
CREATE CATALOG IF NOT EXISTS prime_insurance_jellsinki_poc;
USE CATALOG prime_insurance_jellsinki_poc;

-- COMMAND ----------

-- Bronze schema: raw ingestion layer
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw data ingested from 6 regional systems. No transformations applied.';

-- COMMAND ----------

-- Silver schema: data quality and harmonization layer
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned, harmonized, deduplicated data with quality enforcement. Includes quarantine tables.';

-- COMMAND ----------

-- Gold schema: dimensional model for business consumption
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Star schema dimensional model with facts, dimensions, and business views.';

-- COMMAND ----------

-- Verify setup
SHOW SCHEMAS IN prime_insurance_jellsinki_poc;
