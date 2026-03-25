# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Environment Setup - PrimeInsurance Data Platform
# MAGIC
# MAGIC Validates catalog, schemas, volume, and source files.
# MAGIC Uploads missing files from GitHub repo if needed.
# MAGIC **Skips entirely if everything is already in place.**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS `databricks-hackathon-insurance`
# MAGIC COMMENT 'PrimeInsurance Data Intelligence Platform';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `databricks-hackathon-insurance`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS `databricks-hackathon-insurance`.bronze
# MAGIC COMMENT 'Raw ingested data from all 6 regional insurance systems';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `databricks-hackathon-insurance`.silver
# MAGIC COMMENT 'Cleaned and harmonized data with quality rules applied';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `databricks-hackathon-insurance`.gold
# MAGIC COMMENT 'Star schema facts/dims, aggregations, and GenAI output tables';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS `databricks-hackathon-insurance`.bronze.workshop_data
# MAGIC COMMENT 'Raw CSV and JSON files from 6 acquired regional insurance companies';

# COMMAND ----------

import os, requests

volume_path = "/Volumes/databricks-hackathon-insurance/bronze/workshop_data"

GITHUB_RAW = "https://raw.githubusercontent.com/aksingh-jft/primeins/main/data/autoinsurancedata"

expected_files = {
    "Insurance 1/customers_1.csv": f"{GITHUB_RAW}/Insurance%201/customers_1.csv",
    "Insurance 1/Sales_2.csv":     f"{GITHUB_RAW}/Insurance%201/Sales_2.csv",
    "Insurance 2/customers_2.csv": f"{GITHUB_RAW}/Insurance%202/customers_2.csv",
    "Insurance 2/sales_1.csv":     f"{GITHUB_RAW}/Insurance%202/sales_1.csv",
    "Insurance 3/customers_3.csv": f"{GITHUB_RAW}/Insurance%203/customers_3.csv",
    "Insurance 3/sales_4.csv":     f"{GITHUB_RAW}/Insurance%203/sales_4.csv",
    "Insurance 4/customers_4.csv": f"{GITHUB_RAW}/Insurance%204/customers_4.csv",
    "Insurance 4/cars.csv":        f"{GITHUB_RAW}/Insurance%204/cars.csv",
    "Insurance 5/customers_5.csv": f"{GITHUB_RAW}/Insurance%205/customers_5.csv",
    "Insurance 5/policy.csv":      f"{GITHUB_RAW}/Insurance%205/policy.csv",
    "Insurance 6/customers_6.csv": f"{GITHUB_RAW}/Insurance%206/customers_6.csv",
    "Insurance 6/claims_1.json":   f"{GITHUB_RAW}/Insurance%206/claims_1.json",
    "customers_7.csv":             f"{GITHUB_RAW}/customers_7.csv",
    "claims_2.json":               f"{GITHUB_RAW}/claims_2.json",
}

# COMMAND ----------

def list_all_files(path):
    all_files = []
    try:
        items = dbutils.fs.ls(path)
        for item in items:
            if item.isDir():
                all_files.extend(list_all_files(item.path))
            else:
                all_files.append((item.path, item.size))
    except Exception:
        pass
    return all_files

# check what's already there
existing_files = list_all_files(volume_path)

# find missing files (must exist AND be >5KB to be real, not old samples)
missing = []
for file_rel, url in expected_files.items():
    found = any(file_rel in f and size > 5000 for f, size in existing_files)
    if not found:
        missing.append((file_rel, url))

# quick summary
present_count = len(expected_files) - len(missing)
print(f"files present: {present_count}/{len(expected_files)}")

if not missing:
    print("everything is already set up - skipping rest of notebook")
    dbutils.notebook.exit("SKIPPED - all files present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Missing Files

# COMMAND ----------

# only reaches here if something is missing
print(f"need to upload {len(missing)} files:\n")
for file_rel, _ in missing:
    print(f"  [MISSING] {file_rel}")

# COMMAND ----------

for file_rel, url in missing:
    target = f"{volume_path}/{file_rel}"

    parent = "/".join(target.split("/")[:-1])
    dbutils.fs.mkdirs(parent)

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()

        local_tmp = f"/tmp/{file_rel.replace('/', '_')}"
        with open(local_tmp, "wb") as f:
            f.write(resp.content)

        dbutils.fs.cp(f"file:{local_tmp}", target)
        print(f"  uploaded: {file_rel} ({len(resp.content):,} bytes)")
        os.remove(local_tmp)
    except Exception as e:
        print(f"  FAILED: {file_rel} - {e}")

# COMMAND ----------

# verify after upload
files = list_all_files(volume_path)
still_missing = []
for file_rel, _ in expected_files.items():
    found = any(file_rel in f and size > 5000 for f, size in files)
    if not found:
        still_missing.append(file_rel)

if still_missing:
    print(f"WARNING: {len(still_missing)} files still missing after upload:")
    for f in still_missing:
        print(f"  {f}")
    raise Exception(f"Setup incomplete - {len(still_missing)} files missing")
else:
    print(f"all {len(expected_files)} files verified")

# COMMAND ----------

# MAGIC %md
# MAGIC Setup complete.
