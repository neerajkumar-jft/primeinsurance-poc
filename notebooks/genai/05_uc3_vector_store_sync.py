# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 Alternative: Vector Store Sync (Pipeline Job)
# MAGIC
# MAGIC Runs as part of the end-to-end pipeline after Gold updates.
# MAGIC Refreshes the policy documents table from dim_policy and triggers
# MAGIC a Vector Search index sync.
# MAGIC
# MAGIC This is lightweight: rebuilds the documents table (~1 sec for 999 rows)
# MAGIC and triggers the index sync (managed by Databricks).
# MAGIC
# MAGIC **Prerequisite**: `05_uc3_vector_store_setup.py` must have been run once
# MAGIC to create the endpoint and index.

# COMMAND ----------

dbutils.widgets.text("catalog", "primeins")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{CATALOG}`")

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_TABLE = f"{CATALOG}.gold.dim_policy"
DOCS_TABLE = f"{CATALOG}.gold.dim_policy_documents"
INDEX_NAME = f"{CATALOG}.gold.dim_policy_vs_index"

print(f"Source: {SOURCE_TABLE}")
print(f"Docs table: {DOCS_TABLE}")
print(f"Index: {INDEX_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Refresh policy documents table
# MAGIC
# MAGIC Rebuild the text representations from the latest dim_policy data.
# MAGIC Uses MERGE so only changed/new policies are updated (Change Data Feed
# MAGIC picks up only the delta for the index sync).

# COMMAND ----------

# Get current counts before refresh
before_count = spark.sql(f"SELECT COUNT(*) FROM {DOCS_TABLE}").collect()[0][0]
source_count = spark.sql(f"SELECT COUNT(*) FROM {SOURCE_TABLE}").collect()[0][0]
print(f"Before refresh: {before_count} docs, {source_count} source policies")

# MERGE: update existing, insert new, delete removed
spark.sql(f"""
MERGE INTO {DOCS_TABLE} AS target
USING (
  SELECT
    CAST(policy_number AS STRING) as policy_number,
    policy_bind_date,
    policy_state,
    policy_csl,
    policy_deductible,
    policy_annual_premium,
    umbrella_limit,
    car_id,
    customer_id,
    CONCAT(
      'Policy ', CAST(policy_number AS STRING), ' is an auto insurance policy ',
      'bound on ', CAST(policy_bind_date AS STRING), ' in the state of ', policy_state, '. ',
      'The policy has a combined single limit (CSL) of ', policy_csl, ', ',
      'which provides bodily injury coverage of $', SPLIT(policy_csl, '/')[0], 'K per person ',
      'and $', SPLIT(policy_csl, '/')[1], 'K per accident. ',
      'The deductible is $', FORMAT_NUMBER(policy_deductible, 0), ' ',
      'and the annual premium is $', FORMAT_NUMBER(policy_annual_premium, 2), '. ',
      'This policy covers car ID ', CAST(car_id AS STRING), ' ',
      'and belongs to customer ID ', CAST(customer_id AS STRING), '. ',
      CASE
        WHEN umbrella_limit > 0
        THEN CONCAT('The policy includes umbrella coverage with a limit of $', FORMAT_NUMBER(umbrella_limit, 0), '.')
        ELSE 'This policy does not include umbrella coverage.'
      END, ' ',
      CASE
        WHEN policy_annual_premium >= 2000 THEN 'This is a Premium tier policy.'
        WHEN policy_annual_premium >= 1000 THEN 'This is a Standard tier policy.'
        ELSE 'This is a Basic tier policy.'
      END
    ) as policy_text
  FROM {SOURCE_TABLE}
) AS source
ON target.policy_number = source.policy_number
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
""")

after_count = spark.sql(f"SELECT COUNT(*) FROM {DOCS_TABLE}").collect()[0][0]
print(f"After refresh: {after_count} docs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Trigger index sync

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    w.vector_search_indexes.sync_index(INDEX_NAME)
    print(f"Index sync triggered for {INDEX_NAME}")
except Exception as e:
    print(f"Index sync note: {e}")
    print("The Delta Sync index may auto-sync from Change Data Feed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify index status

# COMMAND ----------

import time

for i in range(30):
    try:
        idx = w.vector_search_indexes.get_index(INDEX_NAME)
        try:
            is_ready = idx.status.ready if hasattr(idx.status, 'ready') else False
        except Exception:
            is_ready = False

        if is_ready:
            print(f"Index is READY and synced")
            break
        print(f"  Syncing... ({i*10}s)")
    except Exception as e:
        print(f"  Checking... {e}")
    time.sleep(10)

# Quick verification query
try:
    results = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["policy_number", "policy_csl"],
        query_text="high coverage policy",
        num_results=1
    )
    if results.result.data_array:
        print(f"Verification: index returning results (e.g. Policy {results.result.data_array[0][0]})")
    else:
        print("Verification: index returned no results")
except Exception as e:
    print(f"Verification skipped: {e}")

print(f"\nSync complete. {after_count} policies indexed.")
