# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Create Catalog, Schemas, and Volume
# MAGIC
# MAGIC This notebook sets up the storage structure for the PrimeInsurance Data Intelligence Platform.
# MAGIC It creates the Unity Catalog objects and confirms all 14 source files are accessible.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG IF NOT EXISTS primeins
# MAGIC COMMENT 'PrimeInsurance Data Intelligence Platform';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schemas for each layer
# MAGIC CREATE SCHEMA IF NOT EXISTS primeins.bronze
# MAGIC COMMENT 'Raw ingestion layer';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS primeins.silver
# MAGIC COMMENT 'Cleaned, harmonized, quality-enforced data';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS primeins.gold
# MAGIC COMMENT 'Star schema, aggregations, AI outputs';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create volume for raw source files
# MAGIC CREATE VOLUME IF NOT EXISTS primeins.bronze.raw_data
# MAGIC COMMENT 'Raw source files from 6 regional systems';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify volume structure
# MAGIC
# MAGIC After uploading files via CLI or UI, confirm all 14 files are accessible.

# COMMAND ----------

import os

volume_path = "/Volumes/primeins/bronze/raw_data"

# Expected file structure
expected_files = [
    "Insurance_1/customers_1.csv",
    "Insurance_1/Sales_2.csv",
    "Insurance_2/customers_2.csv",
    "Insurance_2/sales_1.csv",
    "Insurance_3/customers_3.csv",
    "Insurance_3/sales_4.csv",
    "Insurance_4/customers_4.csv",
    "Insurance_4/cars.csv",
    "Insurance_5/customers_5.csv",
    "Insurance_5/policy.csv",
    "Insurance_6/customers_6.csv",
    "Insurance_6/claims_1.json",
    "customers_7.csv",
    "claims_2.json",
]

print(f"Checking {len(expected_files)} files in {volume_path}\n")

found = 0
missing = 0
for f in expected_files:
    full_path = os.path.join(volume_path, f)
    if os.path.exists(full_path):
        size = os.path.getsize(full_path)
        print(f"  OK  {f} ({size:,} bytes)")
        found += 1
    else:
        print(f"  MISSING  {f}")
        missing += 1

print(f"\n{found}/{len(expected_files)} files found, {missing} missing")
assert missing == 0, f"{missing} files are missing from the volume"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant access to team members

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant usage on catalog to all team members
# MAGIC GRANT USAGE ON CATALOG primeins TO `abhinav.sarkar@jellyfishtechnologies.com`;
# MAGIC GRANT USAGE ON CATALOG primeins TO `aksingh@jellyfishtechnologies.com`;
# MAGIC GRANT USAGE ON CATALOG primeins TO `paras.dhyani@jellyfishtechnologies.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant schema-level access
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.bronze TO `abhinav.sarkar@jellyfishtechnologies.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.bronze TO `aksingh@jellyfishtechnologies.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.bronze TO `paras.dhyani@jellyfishtechnologies.com`;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.silver TO `abhinav.sarkar@jellyfishtechnologies.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.silver TO `aksingh@jellyfishtechnologies.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.silver TO `paras.dhyani@jellyfishtechnologies.com`;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.gold TO `abhinav.sarkar@jellyfishtechnologies.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.gold TO `aksingh@jellyfishtechnologies.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA primeins.gold TO `paras.dhyani@jellyfishtechnologies.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant volume access
# MAGIC GRANT READ VOLUME ON VOLUME primeins.bronze.raw_data TO `abhinav.sarkar@jellyfishtechnologies.com`;
# MAGIC GRANT READ VOLUME ON VOLUME primeins.bronze.raw_data TO `aksingh@jellyfishtechnologies.com`;
# MAGIC GRANT READ VOLUME ON VOLUME primeins.bronze.raw_data TO `paras.dhyani@jellyfishtechnologies.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Resource | Path | Status |
# MAGIC |----------|------|--------|
# MAGIC | Catalog | `primeins` | Created |
# MAGIC | Bronze schema | `primeins.bronze` | Created |
# MAGIC | Silver schema | `primeins.silver` | Created |
# MAGIC | Gold schema | `primeins.gold` | Created |
# MAGIC | Volume | `primeins.bronze.raw_data` | Created |
# MAGIC | Source files | 14 files in 6 regional folders | Uploaded and verified |
# MAGIC | Team access | All 4 members granted access | Configured |
