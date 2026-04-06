# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 Alternative: Persistent Vector Store Setup
# MAGIC
# MAGIC This notebook creates a persistent, auto-syncing vector store using
# MAGIC Databricks Vector Search. Unlike the FAISS approach (which rebuilds
# MAGIC in memory every run), this index:
# MAGIC - Persists across sessions
# MAGIC - Auto-syncs when gold.dim_policy changes (Delta Change Data Feed)
# MAGIC - Supports concurrent users
# MAGIC - Uses Databricks managed embeddings (databricks-bge-large-en)
# MAGIC
# MAGIC Run this once to set up the infrastructure. The index stays alive
# MAGIC and syncs automatically after that.
# MAGIC
# MAGIC **Part 1 of 2**: This notebook sets up the vector store.
# MAGIC **Part 2**: 05_uc3_vector_search_inference.py handles user queries.

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
INDEX_NAME = f"{CATALOG}.gold.dim_policy_vs_index"
EMBEDDING_MODEL = "databricks-bge-large-en"

print(f"Catalog: {CATALOG}")
print(f"Endpoint: {VECTOR_SEARCH_ENDPOINT}")
print(f"Source: {SOURCE_TABLE}")
print(f"Index: {INDEX_NAME}")
print(f"Embedding model: {EMBEDDING_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a text column for embedding
# MAGIC
# MAGIC Vector Search needs a text column to embed. We create a view that
# MAGIC converts each policy row into a natural language document, same
# MAGIC conversion logic as the FAISS notebook but stored as a Delta table
# MAGIC so the index can sync from it.

# COMMAND ----------

# Create a policy documents table with the text representation
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_policy_documents AS
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

doc_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.gold.dim_policy_documents").collect()[0][0]
print(f"Created dim_policy_documents with {doc_count} rows")

# Enable Change Data Feed so Vector Search can sync incrementally
spark.sql(f"ALTER TABLE {CATALOG}.gold.dim_policy_documents SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print("Change Data Feed enabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Vector Search endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Check if endpoint exists
existing_endpoints = [ep.name for ep in w.vector_search_endpoints.list_endpoints()]

if VECTOR_SEARCH_ENDPOINT in existing_endpoints:
    print(f"Endpoint '{VECTOR_SEARCH_ENDPOINT}' already exists")
else:
    print(f"Creating endpoint '{VECTOR_SEARCH_ENDPOINT}'...")
    w.vector_search_endpoints.create_endpoint(
        name=VECTOR_SEARCH_ENDPOINT,
        endpoint_type="STANDARD"
    )
    print(f"Endpoint '{VECTOR_SEARCH_ENDPOINT}' created")

# Wait for endpoint to be ready
import time
for i in range(30):
    ep = w.vector_search_endpoints.get_endpoint(VECTOR_SEARCH_ENDPOINT)
    status = ep.endpoint_status.state.value if ep.endpoint_status else "UNKNOWN"
    if status == "ONLINE":
        print(f"Endpoint is ONLINE")
        break
    print(f"  Waiting for endpoint... ({status})")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Delta Sync Vector Search Index
# MAGIC
# MAGIC This index auto-syncs with the source table. When dim_policy_documents
# MAGIC changes (new policies, updated coverage), the index updates automatically.

# COMMAND ----------

# Check if index exists
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
        index_type="DELTA_SYNC",
        delta_sync_index_spec={
            "source_table": f"{CATALOG}.gold.dim_policy_documents",
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {
                    "name": "policy_text",
                    "embedding_model_endpoint_name": EMBEDDING_MODEL
                }
            ]
        }
    )
    print(f"Index '{INDEX_NAME}' created and syncing...")

# Wait for index to be ready
for i in range(60):
    try:
        idx = w.vector_search_indexes.get_index(INDEX_NAME)
        status = idx.status.ready
        if status:
            print(f"Index is READY")
            break
        print(f"  Syncing... ({i*10}s)")
    except Exception as e:
        print(f"  Waiting... {e}")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify the index

# COMMAND ----------

# Test a simple similarity search
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
# MAGIC The vector store is now persistent and auto-syncing:
# MAGIC
# MAGIC | Component | Value |
# MAGIC |-----------|-------|
# MAGIC | Endpoint | `primeins_policy_vs` |
# MAGIC | Index | `primeins.gold.dim_policy_vs_index` |
# MAGIC | Source table | `primeins.gold.dim_policy_documents` |
# MAGIC | Embedding model | `databricks-bge-large-en` (managed) |
# MAGIC | Sync mode | Delta Sync (auto on Change Data Feed) |
# MAGIC | Policies indexed | Same as dim_policy count |
# MAGIC
# MAGIC Run `05_uc3_vector_search_inference.py` for the query interface.
