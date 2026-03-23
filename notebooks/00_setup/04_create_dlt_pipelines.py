# Databricks notebook source
# MAGIC %md
# MAGIC # Create DLT pipelines
# MAGIC One pipeline per layer. DLT with Unity Catalog requires a target schema
# MAGIC per pipeline, so bronze and silver are separate pipelines. Silver reads
# MAGIC from bronze tables via fully qualified names.
# MAGIC
# MAGIC Run in order: bronze -> silver -> gold. Run this notebook once after cloning.

# COMMAND ----------

import json

# Auto-detect repo root for notebook paths
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_parts = notebook_path.split("/")
repo_root = "/".join(repo_parts[:4])  # /Repos/<email>/repo-name

CATALOG = "prime_insurance_jellsinki_poc"

print(f"Repo root: {repo_root}")
print(f"Catalog: {CATALOG}")

# COMMAND ----------

pipelines = [
    # Bronze pipeline
    {
        "name": f"{CATALOG}_bronze",
        "target": "bronze",
        "notebooks": [
            f"{repo_root}/notebooks/01_bronze/bronze_customers",
            f"{repo_root}/notebooks/01_bronze/bronze_claims",
            f"{repo_root}/notebooks/01_bronze/bronze_policy",
            f"{repo_root}/notebooks/01_bronze/bronze_sales",
            f"{repo_root}/notebooks/01_bronze/bronze_cars",
        ],
        "continuous": False,
    },
    # Silver pipeline - reads from bronze via fully qualified names
    {
        "name": f"{CATALOG}_silver",
        "target": "silver",
        "notebooks": [
            f"{repo_root}/notebooks/02_silver/silver_customers",
            f"{repo_root}/notebooks/02_silver/silver_claims",
            f"{repo_root}/notebooks/02_silver/silver_policy",
            f"{repo_root}/notebooks/02_silver/silver_sales",
            f"{repo_root}/notebooks/02_silver/silver_cars",
        ],
        "continuous": False,
    },
    # Gold pipeline - dimensions, facts, and business views
    {
        "name": f"{CATALOG}_gold",
        "target": "gold",
        "notebooks": [
            f"{repo_root}/notebooks/03_gold/gold_dimensions",
            f"{repo_root}/notebooks/03_gold/gold_facts",
            f"{repo_root}/notebooks/03_gold/gold_views",
        ],
        "continuous": False,
    },
]

print(f"Pipelines to create: {len(pipelines)}")
for p in pipelines:
    print(f"  {p['name']} -> {p['target']} ({len(p['notebooks'])} notebooks)")

# COMMAND ----------

# Create each pipeline using the Databricks REST API.
# If a pipeline with the same name already exists, we skip it.
import requests

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Get existing pipelines so we don't create duplicates
existing = requests.get(f"{host}/api/2.0/pipelines", headers=headers).json()
existing_names = {p["name"] for p in existing.get("statuses", [])}

created = 0
skipped = 0

for p in pipelines:
    if p["name"] in existing_names:
        print(f"  SKIP (exists): {p['name']}")
        skipped += 1
        continue

    body = {
        "name": p["name"],
        "catalog": CATALOG,
        "target": p["target"],
        "libraries": [{"notebook": {"path": nb}} for nb in p["notebooks"]],
        "continuous": p["continuous"],
        "development": True,  # dev mode for the POC
        "channel": "CURRENT",
        "serverless": True,
    }

    resp = requests.post(f"{host}/api/2.0/pipelines", headers=headers, json=body)
    if resp.status_code == 200:
        pid = resp.json().get("pipeline_id", "unknown")
        print(f"  OK: {p['name']} (id: {pid})")
        created += 1
    else:
        print(f"  FAILED: {p['name']} -> {resp.status_code}: {resp.text}")

print(f"\nDone. {created} created, {skipped} skipped (already exist).")
