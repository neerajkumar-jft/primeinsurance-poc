# Databricks notebook source
# MAGIC %md
# MAGIC # UC3 Alternative: Vector Search Policy Assistant
# MAGIC
# MAGIC This notebook queries the persistent Vector Search index created by
# MAGIC `05_uc3_vector_store_setup.py`. Unlike the FAISS notebook, this:
# MAGIC - Doesn't rebuild any index (it's persistent and auto-synced)
# MAGIC - Can serve multiple users simultaneously
# MAGIC - Uses Databricks managed embeddings for queries
# MAGIC - Supports audit trail (every query logged to gold.rag_query_history)
# MAGIC
# MAGIC **Part 2 of 2**: This handles inference. Part 1 sets up the vector store.

# COMMAND ----------

# MAGIC %pip install openai pydantic --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Cell 4
import json
import re
from datetime import datetime
from openai import OpenAI
from pydantic import BaseModel, Field
# Handle both pydantic v1 and v2. v2 uses mode='before'; v1 uses pre=True.
try:
    from pydantic import field_validator as _field_validator  # v2
    def field_validator(*fields):
        return _field_validator(*fields, mode='before')
except ImportError:
    from pydantic import validator as _field_validator  # v1
    def field_validator(*fields):
        return _field_validator(*fields, pre=True)
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalog", "primeins")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{CATALOG}`")

# ── LLM Connection ──
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

w = WorkspaceClient()

# ── Constants ──
MODEL_NAME = "databricks-gpt-oss-20b"
INDEX_NAME = f"{CATALOG}.gold.dim_policy_vs_index"
OUTPUT_TABLE = f"`{CATALOG}`.gold.rag_query_history_vs"
TOP_K = 5

print(f"LLM: {MODEL_NAME}")
print(f"Index: {INDEX_NAME}")
print(f"Output: {OUTPUT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Cell 5
# ============================================================
# RESPONSE PARSER & GUARDRAILS
# ============================================================

def extract_text(raw_response):
    if isinstance(raw_response, list):
        parsed = raw_response
    elif isinstance(raw_response, str):
        try:
            parsed = json.loads(raw_response)
        except json.JSONDecodeError:
            return raw_response
    else:
        return str(raw_response)
    for block in parsed:
        if block.get("type") == "text":
            return block["text"]
    return str(raw_response)


def apply_guardrails(text):
    text = re.sub(r'\b[\w.-]+@[\w.-]+\.\w+\b', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[REDACTED_PHONE]', text)
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    return text


class RAGAnswer(BaseModel):
    answer: str = Field(min_length=20)
    cited_policies: str = Field(min_length=2)
    confidence_explanation: str = Field(min_length=10)

    @field_validator('answer', 'cited_policies', 'confidence_explanation')
    @classmethod
    def clean_input(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('n/a', 'none', 'null', '', '-'):
                return "[Not provided]"
        return v

# COMMAND ----------

# ============================================================
# VECTOR SEARCH RETRIEVAL
# ============================================================

def retrieve(query, top_k=TOP_K):
    """Retrieve relevant policies from Databricks Vector Search."""
    results = w.vector_search_indexes.query_index(
        index_name=INDEX_NAME,
        columns=["policy_number", "policy_text", "policy_csl",
                 "policy_deductible", "policy_annual_premium",
                 "umbrella_limit", "policy_state"],
        query_text=query,
        num_results=top_k
    )

    policies = []
    for row in results.result.data_array:
        score = row[-1] if len(row) > 7 else 0.0  # similarity score is last column
        policies.append({
            "policy_number": row[0],
            "text": row[1],
            "policy_csl": row[2],
            "deductible": row[3],
            "premium": row[4],
            "umbrella_limit": row[5],
            "policy_state": row[6],
            "similarity_score": round(float(score), 4) if score else 0.0
        })

    return policies

# COMMAND ----------

# ============================================================
# RAG QUERY ENGINE
# ============================================================

SYSTEM_PROMPT = """You are a policy intelligence assistant at PrimeInsurance. Claims adjusters ask you questions about insurance policies. You answer based ONLY on the policy documents provided below.

You must always respond in json format with exactly these 3 keys:
{
    "answer": "Direct answer to the question. Reference specific policy numbers, amounts, and coverage details.",
    "cited_policies": "Comma-separated list of policy numbers you referenced",
    "confidence_explanation": "Explain how confident you are and why"
}

<constraints>
- ONLY use information from the provided policy documents
- ALWAYS cite specific policy numbers
- If no policy matches the question, say so clearly
- Do NOT invent policy details not in the documents
</constraints>"""


def ask(question):
    """Full RAG pipeline using Databricks Vector Search."""
    start = datetime.now()

    # Step 1: Retrieve from Vector Search
    results = retrieve(question, top_k=TOP_K)

    # Step 2: Build context
    context_parts = []
    for i, r in enumerate(results[:3]):
        context_parts.append(f"[Policy {r['policy_number']}]\n{r['text']}")
    context = "\n\n".join(context_parts)

    # Step 3: Call LLM
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Retrieved policy documents:\n\n{context}\n\nQuestion: {question}"},
    ]

    response = client.chat.completions.create(
        model=MODEL_NAME, messages=messages, max_tokens=1500
    )
    raw = response.choices[0].message.content
    text = extract_text(raw)

    # Parse JSON response
    try:
        first = text.find('{')
        last = text.rfind('}')
        if first != -1 and last > first:
            data = json.loads(text[first:last+1])
            answer_obj = RAGAnswer(**data)
            answer_text = apply_guardrails(answer_obj.answer)
            cited = answer_obj.cited_policies
            confidence = answer_obj.confidence_explanation
        else:
            answer_text = text
            cited = ""
            confidence = "Could not parse structured response"
    except Exception as e:
        answer_text = text
        cited = ""
        confidence = f"Parse error: {e}"

    duration_ms = int((datetime.now() - start).total_seconds() * 1000)

    return {
        "question": question,
        "answer": answer_text,
        "cited_policies": cited,
        "confidence_explanation": confidence,
        "num_chunks_retrieved": len(results),
        "retrieved_policies": json.dumps([
            {"policy_number": r["policy_number"], "similarity": r["similarity_score"]}
            for r in results[:5]
        ]),
        "duration_ms": duration_ms,
        "model_name": MODEL_NAME,
        "index_name": INDEX_NAME,
        "retrieval_method": "databricks_vector_search",
        "generated_at": datetime.now().isoformat(),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test queries
# MAGIC Same 5 query types as the FAISS notebook for comparison.

# COMMAND ----------

test_questions = [
    "What are the details of policy 100804?",
    "Which policy has the highest annual premium and what does it cover?",
    "Which policies are in the state of Illinois?",
    "Which policies have a deductible of $500 or less?",
    "Which policies include umbrella coverage and what are the limits?",
]

all_results = []

for i, question in enumerate(test_questions):
    print(f"\n{'='*70}")
    print(f"  Q{i+1}: {question}")
    print(f"{'='*70}")

    result = ask(question)
    all_results.append(result)

    print(f"\n  Answer: {result['answer'][:300]}")
    print(f"\n  Cited: {result['cited_policies']}")
    print(f"  Duration: {result['duration_ms']}ms")
    print(f"  Method: {result['retrieval_method']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save query history

# COMMAND ----------

if all_results:
    results_df = spark.createDataFrame(all_results)

    table_exists = spark.catalog.tableExists(OUTPUT_TABLE)
    if not table_exists:
        results_df.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)
        print(f"Created {OUTPUT_TABLE} with {len(all_results)} rows")
    else:
        results_df.write.mode("append").saveAsTable(OUTPUT_TABLE)
        print(f"Appended {len(all_results)} rows to {OUTPUT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison: FAISS vs Vector Search
# MAGIC
# MAGIC | Feature | FAISS (current) | Databricks Vector Search (this) |
# MAGIC |---------|----------------|--------------------------------|
# MAGIC | Index persistence | In-memory only, lost when notebook ends | Persistent, managed service |
# MAGIC | Index rebuild | Full rebuild every run | Auto-sync via Delta Change Data Feed |
# MAGIC | Embedding | Local (sentence-transformers) | Managed (databricks-bge-large-en) |
# MAGIC | Concurrent users | Not supported | Built-in |
# MAGIC | Audit trail | Logged to Gold | Logged to Gold |
# MAGIC | Scale | Fine for 1K policies | Handles millions |
# MAGIC | Dependencies | pip install sentence-transformers faiss-cpu | None (native Databricks) |
# MAGIC | Setup | None (ephemeral) | One-time endpoint + index creation |