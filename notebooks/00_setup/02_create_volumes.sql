-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PrimeInsurance - Volume Setup
-- MAGIC Creates a single managed volume in the Bronze schema for raw file ingestion.
-- MAGIC All 14 source files go into this volume, organized by entity (customers/, claims/, etc.)
-- MAGIC with regional sub-folders preserved inside each entity folder.
-- MAGIC
-- MAGIC Upload data files to this volume before running the Bronze pipeline.

-- COMMAND ----------

USE CATALOG prime_insurance_jellsinki_poc;
USE SCHEMA bronze;

-- COMMAND ----------

-- Single volume for all raw data files from 6 regional systems.
-- Folder structure inside: raw_data/customers/, raw_data/claims/, raw_data/policy/, etc.
-- Regional sub-folders (Insurance 1/, Insurance 2/, ...) preserved within each entity folder.
CREATE VOLUME IF NOT EXISTS raw_data
COMMENT 'Raw data files from 6 regional systems. 14 files across 5 entities (customers, claims, policy, sales, cars) with regional sub-folders.';

-- COMMAND ----------

-- Verify volume created
SHOW VOLUMES IN prime_insurance_jellsinki_poc.bronze;
