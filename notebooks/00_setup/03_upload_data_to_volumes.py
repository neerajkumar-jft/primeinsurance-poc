# Databricks notebook source
# MAGIC %md
# MAGIC # Upload raw data to volumes
# MAGIC 14 files from 6 regional systems need to get into the raw_data volume before the bronze pipelines can run.
# MAGIC We mirror the data/ folder structure as-is: regional folders (Insurance 1/ through Insurance 6/)
# MAGIC and root-level files stay where they are. No reorganization by entity.
# MAGIC
# MAGIC This notebook auto-detects the repo path, so it works regardless of where the repo is cloned.
# MAGIC Make sure you've already run 01_create_catalog_schemas and 02_create_volumes first.

# COMMAND ----------

import os
import shutil

# Auto-detect the repo root. When a repo is cloned in Databricks,
# the notebook runs from within the repo, so we walk up from the
# current notebook's location to find the data/ folder.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# notebook_path looks like /Repos/<email>/prime-ins-jellsinki-poc/notebooks/00_setup/03_upload_data_to_volumes
# We need /Workspace/Repos/<email>/prime-ins-jellsinki-poc

repo_parts = notebook_path.split("/")
# Find the index after "Repos/<user>/<repo_name>"
repo_root = "/".join(repo_parts[:4])  # /Repos/<email>/repo-name
REPO_BASE = f"/Workspace{repo_root}"

# Fallback if the above doesn't resolve correctly
if not os.path.exists(f"{REPO_BASE}/data"):
    cwd = os.getcwd()
    while cwd != "/" and not os.path.exists(os.path.join(cwd, "data")):
        cwd = os.path.dirname(cwd)
    REPO_BASE = cwd

print(f"Repo root: {REPO_BASE}")
print(f"Data folder exists: {os.path.exists(os.path.join(REPO_BASE, 'data'))}")

# COMMAND ----------

CATALOG = "prime_insurance_jellsinki_poc"
VOLUME_BASE = f"/Volumes/{CATALOG}/bronze/raw_data"

# COMMAND ----------

# File mapping. We mirror the data/ folder structure directly into the volume.
# No entity-level grouping -- regional folders and root files stay as they are.
# data/Insurance 1/customers_1.csv -> /Volumes/.../raw_data/Insurance 1/customers_1.csv
# data/customers_7.csv -> /Volumes/.../raw_data/customers_7.csv
file_mappings = [
    # Insurance 1 (customers_1, Sales_2)
    ("data/Insurance 1/customers_1.csv", f"{VOLUME_BASE}/Insurance 1/customers_1.csv"),
    ("data/Insurance 1/Sales_2.csv",     f"{VOLUME_BASE}/Insurance 1/Sales_2.csv"),

    # Insurance 2 (customers_2, sales_1)
    ("data/Insurance 2/customers_2.csv", f"{VOLUME_BASE}/Insurance 2/customers_2.csv"),
    ("data/Insurance 2/sales_1.csv",     f"{VOLUME_BASE}/Insurance 2/sales_1.csv"),

    # Insurance 3 (customers_3, sales_4)
    ("data/Insurance 3/customers_3.csv", f"{VOLUME_BASE}/Insurance 3/customers_3.csv"),
    ("data/Insurance 3/sales_4.csv",     f"{VOLUME_BASE}/Insurance 3/sales_4.csv"),

    # Insurance 4 (customers_4, cars)
    ("data/Insurance 4/customers_4.csv", f"{VOLUME_BASE}/Insurance 4/customers_4.csv"),
    ("data/Insurance 4/cars.csv",        f"{VOLUME_BASE}/Insurance 4/cars.csv"),

    # Insurance 5 (customers_5, policy)
    ("data/Insurance 5/customers_5.csv", f"{VOLUME_BASE}/Insurance 5/customers_5.csv"),
    ("data/Insurance 5/policy.csv",      f"{VOLUME_BASE}/Insurance 5/policy.csv"),

    # Insurance 6 (customers_6, claims_1)
    ("data/Insurance 6/customers_6.csv", f"{VOLUME_BASE}/Insurance 6/customers_6.csv"),
    ("data/Insurance 6/claims_1.json",   f"{VOLUME_BASE}/Insurance 6/claims_1.json"),

    # Root-level files (customers_7, claims_2)
    ("data/customers_7.csv",  f"{VOLUME_BASE}/customers_7.csv"),
    ("data/claims_2.json",    f"{VOLUME_BASE}/claims_2.json"),
]

print(f"Files to upload: {len(file_mappings)}")

# COMMAND ----------

# Copy each file into the volume using Python file I/O.
# Volumes are mounted as FUSE at /Volumes/... so we can write directly.
# We use shutil.copy2 instead of dbutils.fs.cp because newer Databricks
# runtimes block local filesystem access via the file: scheme.
success = 0
failed = 0

for src, dst in file_mappings:
    full_src = os.path.join(REPO_BASE, src)
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(full_src, dst)
        size = os.path.getsize(dst)
        print(f"  OK: {src} -> {dst} ({size} bytes)")
        success += 1
    except Exception as e:
        print(f"  FAILED: {src} -> {e}")
        failed += 1

print(f"\nDone. {success} uploaded, {failed} failed.")

# COMMAND ----------

# Quick check that everything landed. Walk the volume recursively
# to list every file with its size.
print(f"Contents of {VOLUME_BASE}:\n")

for root, dirs, files in os.walk(VOLUME_BASE):
    for fname in sorted(files):
        fpath = os.path.join(root, fname)
        rel = os.path.relpath(fpath, VOLUME_BASE)
        size = os.path.getsize(fpath)
        print(f"  {rel} ({size} bytes)")
