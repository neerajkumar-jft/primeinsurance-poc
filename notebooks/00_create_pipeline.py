# Databricks notebook source
# MAGIC %md
# MAGIC # Create Pipeline Job
# MAGIC Run this notebook **once** after cloning the repo to Databricks.
# MAGIC It auto-detects the repo path and creates the full workflow job with correct DAG.

# COMMAND ----------

import json
import requests

# Auto-detect workspace context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = ctx.notebookPath().get()
api_url = ctx.apiUrl().get()
api_token = ctx.apiToken().get()

# Derive the repo notebooks folder path
# notebook_path = /Repos/<user>/<repo>/notebooks/00_create_pipeline
repo_notebooks_path = "/".join(notebook_path.split("/")[:-1])
repo_root = "/".join(notebook_path.split("/")[:-2])

print(f"Repo root:      {repo_root}")
print(f"Notebooks path: {repo_notebooks_path}")
print(f"API URL:        {api_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Pipeline DAG

# COMMAND ----------

job_config = {
    "name": "Prime Insurance - POC",
    "max_concurrent_runs": 1,
    "queue": {"enabled": True},
    "tasks": [
        {
            "task_key": "setup",
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/00_setup",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 600
        },
        {
            "task_key": "bronze_ingestion",
            "depends_on": [{"task_key": "setup"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/01_bronze_ingestion",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "silver_customers",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/02_silver_customers",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "silver_claims",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/02_silver_claims",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "silver_policy",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/02_silver_policy",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "silver_sales",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/02_silver_sales",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "silver_cars",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/02_silver_cars",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "silver_dq_combined",
            "depends_on": [
                {"task_key": "silver_customers"},
                {"task_key": "silver_claims"},
                {"task_key": "silver_policy"},
                {"task_key": "silver_sales"},
                {"task_key": "silver_cars"}
            ],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/02_silver_dq_combined",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 600
        },
        {
            "task_key": "gold_dimensions",
            "depends_on": [
                {"task_key": "silver_customers"},
                {"task_key": "silver_claims"},
                {"task_key": "silver_policy"},
                {"task_key": "silver_sales"},
                {"task_key": "silver_cars"}
            ],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/03_gold_dimensions",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "gold_facts",
            "depends_on": [{"task_key": "gold_dimensions"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/03_gold_facts",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "uc1_dq_explanations",
            "depends_on": [{"task_key": "silver_dq_combined"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/04_uc1_dq_explanations",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "uc2_anomaly_engine",
            "depends_on": [{"task_key": "gold_facts"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/04_uc2_anomaly_engine",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "uc3_policy_rag",
            "depends_on": [{"task_key": "gold_facts"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/04_uc3_policy_rag",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "uc4_executive_insights",
            "depends_on": [{"task_key": "gold_facts"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/04_uc4_executive_insights",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "dashboard",
            "depends_on": [
                {"task_key": "gold_facts"},
                {"task_key": "uc1_dq_explanations"},
                {"task_key": "uc2_anomaly_engine"},
                {"task_key": "uc3_policy_rag"},
                {"task_key": "uc4_executive_insights"}
            ],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/05_dashboard",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 600
        },
        {
            "task_key": "create_lakeview_dashboard",
            "depends_on": [{"task_key": "dashboard"}],
            "notebook_task": {
                "notebook_path": f"{repo_notebooks_path}/06_create_dashboard",
                "source": "WORKSPACE"
            },
            "timeout_seconds": 600
        }
    ]
}

print(f"Pipeline: {len(job_config['tasks'])} tasks configured")
print("DAG:")
print("  setup -> bronze_ingestion")
print("               ├── silver_customers  ─┐")
print("               ├── silver_claims      ├── silver_dq_combined ── uc1_dq_explanations")
print("               ├── silver_policy      │")
print("               ├── silver_sales       ├── gold_dimensions ── gold_facts")
print("               └── silver_cars        ┘        ├── uc2_anomaly_engine")
print("                                                ├── uc3_policy_rag")
print("                                                ├── uc4_executive_insights")
print("                                                └── dashboard → create_lakeview_dashboard")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update Job

# COMMAND ----------

headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

JOB_NAME = "Prime Insurance - POC"

# Check if job already exists
list_resp = requests.get(f"{api_url}/api/2.1/jobs/list", headers=headers)
list_resp.raise_for_status()
existing_jobs = list_resp.json().get("jobs", [])
existing_job_id = None
for j in existing_jobs:
    if j["settings"]["name"] == JOB_NAME:
        existing_job_id = j["job_id"]
        break

if existing_job_id:
    # Update existing job
    print(f"Updating existing job: {existing_job_id}")
    reset_payload = {"job_id": existing_job_id, "new_settings": job_config}
    resp = requests.post(f"{api_url}/api/2.1/jobs/reset", headers=headers, json=reset_payload)
    resp.raise_for_status()
    job_id = existing_job_id
    print(f"Job updated successfully")
else:
    # Create new job
    print("Creating new job...")
    resp = requests.post(f"{api_url}/api/2.1/jobs/create", headers=headers, json=job_config)
    resp.raise_for_status()
    job_id = resp.json()["job_id"]
    print(f"Job created: {job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Ready

# COMMAND ----------

# trigger the pipeline immediately
print("Triggering pipeline run...")
run_resp = requests.post(f"{api_url}/api/2.1/jobs/run-now", headers=headers, json={"job_id": job_id})
if run_resp.status_code == 200:
    run_id = run_resp.json().get("run_id")
    print(f"Pipeline triggered! Run ID: {run_id}")
else:
    run_id = None
    print(f"Could not trigger: {run_resp.text}")

# COMMAND ----------

print("=" * 50)
print("  Prime Insurance - POC Pipeline")
print("=" * 50)
print(f"  Job ID:   {job_id}")
print(f"  Job URL:  {api_url}/#job/{job_id}")
if run_id:
    print(f"  Run URL:  {api_url}/#job/{job_id}/run/{run_id}")
print(f"  Notebooks: {repo_notebooks_path}/")
print("=" * 50)
print()
print("Notes:")
print("  - Pipeline has been triggered automatically")
print("  - Setup notebook auto-uploads data files on first run")
print("  - Notebooks are idempotent (safe to rerun)")
print("  - Catalog: prime-ins-jellsinki-poc")
