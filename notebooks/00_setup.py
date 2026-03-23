# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Environment Setup - PrimeInsurance Data Platform
# MAGIC
# MAGIC Validates catalog, schemas, volume, and source files.
# MAGIC Uploads missing files from GitHub repo if needed.
# MAGIC **Skips entirely if everything is already in place.**

# COMMAND ----------

# use our catalog — create if it doesn't exist
CATALOG_NAME = "prime-ins-jellsinki-poc"
try:
    spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
    print(f"catalog `{CATALOG_NAME}` exists")
except Exception:
    print(f"catalog `{CATALOG_NAME}` not found — creating...")
    spark.sql(f"CREATE CATALOG `{CATALOG_NAME}`")
    spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
    print(f"catalog `{CATALOG_NAME}` created")
CATALOG = CATALOG_NAME
print(f"using catalog: {CATALOG}")

# COMMAND ----------

# create schemas using Python (avoids %sql permission issues)
for schema in ["bronze", "silver", "gold"]:
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{schema}")
        print(f"  schema ready: {CATALOG}.{schema}")
    except Exception as e:
        print(f"  schema {schema}: {e}")

# COMMAND ----------

import os

# create volume if not exists
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.bronze.source_files")
    print(f"  volume ready: {CATALOG}.bronze.source_files")
except Exception as e:
    print(f"  volume: {e}")

volume_path = f"/Volumes/{CATALOG}/bronze/source_files"

# detect repo data directory (cloned via Databricks Repos)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + "/".join(notebook_path.split("/")[:-2])
repo_data = f"{repo_root}/data/autoinsurancedata"
print(f"repo data path: {repo_data}")

expected_files = [
    "Insurance 1/customers_1.csv",
    "Insurance 1/Sales_2.csv",
    "Insurance 2/customers_2.csv",
    "Insurance 2/sales_1.csv",
    "Insurance 3/customers_3.csv",
    "Insurance 3/sales_4.csv",
    "Insurance 4/customers_4.csv",
    "Insurance 4/cars.csv",
    "Insurance 5/customers_5.csv",
    "Insurance 5/policy.csv",
    "Insurance 6/customers_6.csv",
    "Insurance 6/claims_1.json",
    "customers_7.csv",
    "claims_2.json",
]

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

# find missing files (must exist AND be >5KB to be real)
missing = []
for file_rel in expected_files:
    found = any(file_rel in f and size > 5000 for f, size in existing_files)
    if not found:
        missing.append(file_rel)

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
print(f"need to copy {len(missing)} files:\n")
for file_rel in missing:
    print(f"  [MISSING] {file_rel}")

# COMMAND ----------

import shutil

for file_rel in missing:
    target = f"{volume_path}/{file_rel}"
    src = f"{repo_data}/{file_rel}"

    # create parent dirs in volume
    target_dir = os.path.dirname(target)
    os.makedirs(target_dir, exist_ok=True)

    try:
        shutil.copy2(src, target)
        size = os.path.getsize(target)
        print(f"  copied: {file_rel} ({size:,} bytes)")
    except Exception as e:
        print(f"  FAILED: {file_rel} - {e}")

# COMMAND ----------

# verify after upload
files = list_all_files(volume_path)
still_missing = []
for file_rel in expected_files:
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
