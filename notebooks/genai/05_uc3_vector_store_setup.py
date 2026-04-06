# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 Alternative: Vector Store Setup (Run Once)
# MAGIC
# MAGIC One-time setup for the persistent Vector Search infrastructure.
# MAGIC Creates the endpoint and Delta Sync index. Run this manually once.
# MAGIC After that, the sync notebook handles data refreshes as part of the pipeline.
# MAGIC
# MAGIC **This notebook**: Creates endpoint + index (one-time)
# MAGIC **05_uc3_vector_store_sync.py**: Refreshes data + triggers sync (pipeline job)
# MAGIC **05_uc3_vector_search_inference.py**: Query interface

# COMMAND ----------

dbutils.widgets.text("catalog", "primeins")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{CATALOG}`")

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

VECTOR_SEARCH_ENDPOINT = "primeins_policy_vs"
SOURCE_TABLE = f"{CATALOG}.gold.dim_policy"
DOCS_TABLE = f"{CATALOG}.gold.dim_policy_documents"
INDEX_NAME = f"{CATALOG}.gold.dim_policy_vs_index"
EMBEDDING_MODEL = "databricks-bge-large-en"

print(f"Catalog: {CATALOG}")
print(f"Endpoint: {VECTOR_SEARCH_ENDPOINT}")
print(f"Source: {SOURCE_TABLE}")
print(f"Docs table: {DOCS_TABLE}")
print(f"Index: {INDEX_NAME}")
print(f"Embedding model: {EMBEDDING_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create policy documents table
# MAGIC
# MAGIC Converts each policy row into a natural language document.
# MAGIC This table is the source for the Vector Search index.
# MAGIC Change Data Feed is enabled so the index can sync incrementally.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {DOCS_TABLE} AS
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
""")

doc_count = spark.sql(f"SELECT COUNT(*) FROM {DOCS_TABLE}").collect()[0][0]
print(f"Created dim_policy_documents with {doc_count} rows")

spark.sql(f"ALTER TABLE {DOCS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("Change Data Feed enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Vector Search endpoint

# COMMAND ----------

# DBTITLE 1,Cell 7
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType
import time

w = WorkspaceClient()

existing_endpoints = [ep.name for ep in w.vector_search_endpoints.list_endpoints()]

if VECTOR_SEARCH_ENDPOINT in existing_endpoints:
    print(f"Endpoint '{VECTOR_SEARCH_ENDPOINT}' already exists")
else:
    print(f"Creating endpoint '{VECTOR_SEARCH_ENDPOINT}'...")
    w.vector_search_endpoints.create_endpoint(
        name=VECTOR_SEARCH_ENDPOINT,
        endpoint_type=EndpointType.STANDARD
    )
    print(f"Endpoint '{VECTOR_SEARCH_ENDPOINT}' creation initiated")

# Wait for endpoint to be ready
for i in range(30):
    try:
        ep = w.vector_search_endpoints.get_endpoint(VECTOR_SEARCH_ENDPOINT)
        # Extract state value from the enum
        if hasattr(ep, 'endpoint_status') and ep.endpoint_status and hasattr(ep.endpoint_status, 'state'):
            status = str(ep.endpoint_status.state.value) if hasattr(ep.endpoint_status.state, 'value') else str(ep.endpoint_status.state)
        else:
            status = "PROVISIONING"
    except Exception:
        status = "PROVISIONING"

    if "ONLINE" in status.upper() or "READY" in status.upper():
        print(f"Endpoint is {status}")
        break
    print(f"  Waiting for endpoint... ({status})")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Delta Sync index

# COMMAND ----------

# DBTITLE 1,Cell 9
from databricks.sdk.service.vectorsearch import DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType

existing_indexes = []
try:
    idx_list = w.vector_search_indexes.list_indexes(VECTOR_SEARCH_ENDPOINT)
    existing_indexes = [idx.name for idx in idx_list]
except Exception:
    pass

if INDEX_NAME in existing_indexes:
    print(f"Index '{INDEX_NAME}' already exists")
else:
    print(f"Creating index '{INDEX_NAME}'...")
    w.vector_search_indexes.create_index(
        name=INDEX_NAME,
        endpoint_name=VECTOR_SEARCH_ENDPOINT,
        primary_key="policy_number",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=f"{CATALOG}.gold.dim_policy_documents",
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name="policy_text",
                    embedding_model_endpoint_name=EMBEDDING_MODEL
                )
            ]
        )
    )
    print(f"Index '{INDEX_NAME}' created and syncing...")

# Wait for index to be ready
for i in range(60):
    try:
        idx = w.vector_search_indexes.get_index(INDEX_NAME)
        try:
            is_ready = idx.status.ready if hasattr(idx.status, 'ready') else False
        except Exception:
            is_ready = False
        if is_ready:
            print(f"Index is READY")
            break
        print(f"  Syncing... ({i*10}s)")
    except Exception as e:
        print(f"  Waiting... {e}")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify

# COMMAND ----------

# DBTITLE 1,Cell 11
# Check if index is ready before querying
idx = w.vector_search_indexes.get_index(INDEX_NAME)
if not idx.status.ready:
    print(f"Index '{INDEX_NAME}' is not ready yet.")
    print(f"Status: {idx.status.message}")
    print("\nWait for the index to finish syncing, then run this cell again.")
else:
    results = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["policy_number", "policy_text", "policy_csl", "policy_deductible"],
        query_text="policies with umbrella coverage",
        num_results=3
    )

    print("Test query: 'policies with umbrella coverage'")
    print(f"Results: {len(results.result.data_array)} rows\n")
    for row in results.result.data_array:
        print(f"  Policy {row[0]} | CSL: {row[2]} | Deductible: ${row[3]}")
        print(f"  {row[1][:100]}...")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC | Component | Value |
# MAGIC |-----------|-------|
# MAGIC | Endpoint | `primeins_policy_vs` |
# MAGIC | Index | `primeins.gold.dim_policy_vs_index` |
# MAGIC | Source table | `primeins.gold.dim_policy_documents` |
# MAGIC | Embedding model | `databricks-bge-large-en` (managed) |
# MAGIC | Sync mode | Delta Sync (triggered by sync notebook) |
# MAGIC
# MAGIC Next steps:
# MAGIC - **Pipeline sync**: `05_uc3_vector_store_sync.py` refreshes data after each Gold pipeline run
# MAGIC - **Query interface**: `05_uc3_vector_search_inference.py` for user queries