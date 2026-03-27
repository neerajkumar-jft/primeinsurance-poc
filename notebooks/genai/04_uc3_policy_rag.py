# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # UC3: Policy Intelligence Assistant (RAG)
# MAGIC
# MAGIC **Problem**: Claims adjusters spend 10-15 minutes per claim looking up policy details
# MAGIC (deductibles, coverage tiers, umbrella limits). With 3,000 claims per year, that is 500+
# MAGIC analyst hours on lookups that should take seconds. The data exists in `gold.dim_policy` but
# MAGIC adjusters need to ask questions in plain English and get accurate, cited answers.
# MAGIC
# MAGIC **Solution**: A RAG system that converts structured policy data into natural language
# MAGIC documents, embeds them locally with sentence-transformers, builds a FAISS vector index,
# MAGIC and answers questions by retrieving relevant policies and passing them to the LLM.
# MAGIC
# MAGIC **Architecture**:
# MAGIC - dim_policy rows -> text documents -> embeddings -> FAISS index
# MAGIC - User query -> embed -> FAISS search -> top-K chunks -> LLM -> cited answer
# MAGIC
# MAGIC **Design decisions**:
# MAGIC - One policy = one chunk, zero overlap (each row is self-contained at ~100-150 tokens)
# MAGIC - Embeddings generated locally with all-MiniLM-L6-v2 (no external API calls)
# MAGIC - Pydantic validates every LLM response before writing to Gold
# MAGIC - PII guardrails redact sensitive patterns from LLM output
# MAGIC - RAG handles semantic questions; Genie handles analytical SQL questions
# MAGIC
# MAGIC **Input**: `{catalog}.gold.dim_policy`
# MAGIC **Output**: `{catalog}.gold.rag_query_history`

# COMMAND ----------

%pip install openai mlflow tenacity pydantic sentence-transformers faiss-cpu --quiet

# COMMAND ----------

dbutils.library.restartPython()
import warnings
warnings.filterwarnings("ignore", message="Pydantic serializer warnings")

# COMMAND ----------

# ============================================================
# CATALOG CONFIGURATION (dynamic, no hardcoding)
# ============================================================

dbutils.widgets.text("catalog", "primeins")
dbutils.widgets.text("run_test_queries", "true")
CATALOG = dbutils.widgets.get("catalog")
RUN_TEST_QUERIES = dbutils.widgets.get("run_test_queries").lower() == "true"
spark.sql(f"USE CATALOG `{CATALOG}`")
print(f"Catalog: {CATALOG}")
print(f"Run test queries: {RUN_TEST_QUERIES}")

# COMMAND ----------

# ============================================================
# CONFIGURATION & SETUP
# ============================================================

import mlflow
import json
import re
import numpy as np
from datetime import datetime
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_random_exponential
from pydantic import BaseModel, Field, field_validator
from sentence_transformers import SentenceTransformer
import faiss

# ── MLflow Tracing ──
mlflow.openai.autolog()

# ── LLM Connection ──
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

# ── Constants ──
MODEL_NAME = "databricks-gpt-oss-20b"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
MAX_TOKENS = 1500
SOURCE_TABLE = f"`{CATALOG}`.gold.dim_policy"
OUTPUT_TABLE = f"`{CATALOG}`.gold.rag_query_history"
PROMPT_VERSION = "v1"
TOP_K = 5  # Number of chunks to retrieve

# ── Response parser ──
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

# ── LLM call with retry ──
@retry(
    wait=wait_random_exponential(min=2, max=60),
    stop=stop_after_attempt(5),
)
def call_llm(messages, max_tokens=MAX_TOKENS):
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        max_tokens=max_tokens,
    )
    raw = response.choices[0].message.content
    text = extract_text(raw)
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    if first_brace != -1 and last_brace > first_brace:
        return json.loads(text[first_brace:last_brace + 1])
    return json.loads(text)

# ── Output guardrails ──
def apply_guardrails(text):
    """Redacts PII patterns from LLM output."""
    text = re.sub(r'\b[\w.-]+@[\w.-]+\.\w+\b', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[REDACTED_PHONE]', text)
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    return text

print(f"LLM: {MODEL_NAME}")
print(f"Embedding: {EMBEDDING_MODEL} ({EMBEDDING_DIM}d)")
print(f"Source: {SOURCE_TABLE}")
print(f"Output: {OUTPUT_TABLE}")

# COMMAND ----------

# ============================================================
# PYDANTIC OUTPUT MODEL — RAG Answer
# ============================================================

class RAGAnswer(BaseModel):
    """Structured answer from the RAG policy assistant."""

    answer: str = Field(min_length=20, description="Direct answer to the user's question")
    cited_policies: str = Field(min_length=2, description="Comma-separated list of policy numbers used to form the answer")
    confidence_explanation: str = Field(min_length=10, description="Why you are confident or not confident in this answer")

    @field_validator('*', mode='before')
    @classmethod
    def clean_input(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('n/a', 'none', 'null', '', '-'):
                return "[Not provided]"
        return v

print("Pydantic output model initialized")

# COMMAND ----------

# ============================================================
# LOAD POLICIES & CONVERT TO NATURAL LANGUAGE DOCUMENTS
# ============================================================
# Each policy row becomes a rich English paragraph.
# This is critical for embedding quality — embeddings understand
# "the deductible is $500" far better than "deductible: 500".
#
# Key decisions:
#   - One policy = one chunk (no splitting, no overlap)
#   - Policy number embedded in text for citation retrieval
#   - CSL format explained in plain English
#   - Umbrella limit only mentioned if > 0
# ============================================================

policies_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}").toPandas()
print(f"Loaded {len(policies_df)} policies from {SOURCE_TABLE}")

# CSL decoder: "100/300" means $100K per person / $300K per accident
# Also handles 3-part format: "100/300/50" adds property damage coverage
def decode_csl(csl):
    """Convert CSL code to plain English."""
    try:
        parts = str(csl).split("/")
        if len(parts) == 3:
            return f"${parts[0]}K per person, ${parts[1]}K per accident, and ${parts[2]}K property damage"
        if len(parts) == 2:
            return f"${parts[0]}K per person and ${parts[1]}K per accident"
    except Exception:
        pass
    return str(csl)

def policy_to_document(row) -> str:
    """Convert a structured policy row into a natural language document."""
    doc = (
        f"Policy {int(row['policy_number'])} is an auto insurance policy "
        f"bound on {row['policy_bind_date']} in the state of {row['policy_state']}. "
        f"The policy has a combined single limit (CSL) of {row['policy_csl']}, "
        f"which provides bodily injury coverage of {decode_csl(row['policy_csl'])}. "
        f"The deductible is ${row['policy_deductible']:,} "
        f"and the annual premium is ${row['policy_annual_premium']:,.2f}. "
        f"This policy covers car ID {row['car_id']} "
        f"and belongs to customer ID {row['customer_id']}."
    )

    if row['umbrella_limit'] and row['umbrella_limit'] > 0:
        doc += f" The policy includes umbrella coverage with a limit of ${row['umbrella_limit']:,}."
    else:
        doc += " This policy does not include umbrella coverage."

    # Categorize by premium tier
    premium = row['policy_annual_premium']
    if premium >= 2000:
        doc += " This is a Premium tier policy with high coverage."
    elif premium >= 1000:
        doc += " This is a Standard tier policy with moderate coverage."
    else:
        doc += " This is a Basic tier policy with minimum coverage."

    return doc

# Convert all policies
documents = []
metadata = []

for _, row in policies_df.iterrows():
    doc_text = policy_to_document(row)
    documents.append(doc_text)
    metadata.append({
        "text": doc_text,
        "policy_number": str(int(row["policy_number"])),
        "policy_state": row["policy_state"],
        "policy_csl": row["policy_csl"],
        "deductible": int(row["policy_deductible"]),
        "premium": float(row["policy_annual_premium"]),
        "umbrella_limit": int(row["umbrella_limit"]) if row["umbrella_limit"] else 0,
        "customer_id": row["customer_id"],
        "car_id": row["car_id"],
    })

print(f"Converted {len(documents)} policies to documents")
print(f"\nExample document:\n{documents[0]}")

# COMMAND ----------

# ============================================================
# EMBED DOCUMENTS & BUILD FAISS INDEX
# ============================================================
# Model: all-MiniLM-L6-v2
#   - 384-dimensional embeddings
#   - 256 token max input
#   - Trained on cosine similarity
#
# Index: IndexFlatIP (inner product on normalized vectors = cosine similarity)
#   - Exact search (no approximation needed for 1000 policies)
#   - Sub-millisecond query time
#   - Wrapped in IndexIDMap for custom IDs
# ============================================================

# ── Load embedding model ──
embed_model = SentenceTransformer(EMBEDDING_MODEL)
print(f"Loaded embedding model: {EMBEDDING_MODEL}")

# ── Embed all documents ──
embeddings = embed_model.encode(
    documents,
    normalize_embeddings=True,  # Required for cosine similarity via inner product
    show_progress_bar=True,
    batch_size=64
)
embeddings = embeddings.astype(np.float32)
print(f"Embedded {len(embeddings)} documents, shape: {embeddings.shape}")

# ── Build FAISS index ──
# IndexFlatIP on normalized vectors = cosine similarity
# Scores will be in [0, 1] range — easy to interpret
base_index = faiss.IndexFlatIP(EMBEDDING_DIM)
index = faiss.IndexIDMap(base_index)

ids = np.arange(len(embeddings)).astype(np.int64)
index.add_with_ids(embeddings, ids)

print(f"FAISS index built: {index.ntotal} vectors, {EMBEDDING_DIM} dimensions")

# COMMAND ----------

# ============================================================
# RAG QUERY ENGINE
# ============================================================

def retrieve(query: str, top_k: int = TOP_K) -> list:
    """Retrieve top-k relevant policy chunks using FAISS."""
    query_embedding = embed_model.encode([query], normalize_embeddings=True).astype(np.float32)
    scores, indices = index.search(query_embedding, top_k)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
        results.append({
            **metadata[idx],
            "similarity_score": round(float(score), 4),
        })
    return results


def compute_confidence(results: list) -> dict:
    """Compute confidence level based on retrieval quality."""
    if not results:
        return {"level": "none", "score": 0.0}

    top_score = results[0]["similarity_score"]
    top3_avg = np.mean([r["similarity_score"] for r in results[:3]])
    score_gap = (results[0]["similarity_score"] - results[1]["similarity_score"]) if len(results) > 1 else 0.0

    confidence_score = round((0.5 * top_score) + (0.2 * score_gap) + (0.3 * top3_avg), 4)

    if top_score >= 0.65:
        level = "HIGH"
    elif top_score >= 0.35:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {"level": level, "score": confidence_score, "top_similarity": top_score}


SYSTEM_PROMPT = """You are a policy intelligence assistant at PrimeInsurance. Claims adjusters ask you questions about insurance policies. You answer based ONLY on the policy documents provided below.

You must always respond in json format with exactly these 3 keys:
{
    "answer": "Direct answer to the question. Reference specific policy numbers, amounts, and coverage details. If multiple policies match, compare them.",
    "cited_policies": "Comma-separated list of policy numbers you referenced (e.g., '100804, 101421')",
    "confidence_explanation": "Explain how confident you are and why. Reference the relevance of the retrieved policies."
}

<example>
Question: Which policies have umbrella coverage above $2,000,000?
Retrieved: Policy 106234 (umbrella $8M), Policy 111234 (umbrella $10M), Policy 107890 (umbrella $4M)

Response:
{{
    "answer": "Three policies have umbrella coverage above $2,000,000: Policy 111234 has the highest at $10,000,000, followed by Policy 106234 at $8,000,000 and Policy 107890 at $4,000,000. These are all Premium tier policies with annual premiums above $1,800.",
    "cited_policies": "111234, 106234, 107890",
    "confidence_explanation": "High confidence — all three policies explicitly list their umbrella limits in the retrieved documents, and the question directly matches the data available."
}}
</example>

<constraints>
- ONLY use information from the provided policy documents
- ALWAYS cite specific policy numbers
- If no policy matches the question, say so clearly
- Do NOT invent policy details not in the documents
</constraints>"""


def ask(question: str) -> dict:
    """Full RAG pipeline: retrieve, build context, call LLM, return structured answer."""

    # Step 1: Retrieve relevant policies
    results = retrieve(question, top_k=TOP_K)
    confidence = compute_confidence(results)

    # Step 2: Build context from top results
    context_parts = []
    for i, r in enumerate(results[:3]):
        context_parts.append(
            f"[Policy {r['policy_number']}] (relevance: {r['similarity_score']:.1%})\n{r['text']}"
        )
    context = "\n\n".join(context_parts)

    # Step 3: Call LLM with context
    user_message = f"""Retrieved policy documents:

{context}

Question: {question}"""

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    data = call_llm(messages)
    answer = RAGAnswer(**data)

    return {
        "question": question,
        "answer": apply_guardrails(answer.answer),
        "cited_policies": answer.cited_policies,
        "confidence_level": confidence["level"],
        "confidence_score": confidence["score"],
        "confidence_explanation": answer.confidence_explanation,
        "top_similarity": confidence["top_similarity"],
        "num_chunks_retrieved": len(results),
        "retrieved_policies": json.dumps([
            {"policy_number": r["policy_number"], "similarity": r["similarity_score"]}
            for r in results[:5]
        ]),
    }


print("RAG query engine initialized")

# COMMAND ----------

# ============================================================
# TEST QUERIES — 5 different query types
# ============================================================
# The hackathon requires at least 5 test questions covering:
#   1. Specific policy lookup
#   2. Comparative query
#   3. Filter-based question
#   4. Coverage/deductible question
#   5. Umbrella coverage question
#
# Controlled by run_test_queries parameter (default: true).
# The end-to-end pipeline job passes false to avoid duplicate
# query history rows on re-runs.
# ============================================================

if not RUN_TEST_QUERIES:
    print("Skipping test queries (run_test_queries=false)")
    print("FAISS index built and RAG engine ready. Run manually to test.")
    all_results = []
    dbutils.notebook.exit("FAISS index built. Test queries skipped.")

test_questions = [
    # Type 1: Specific policy lookup
    "What are the details of policy 100804?",

    # Type 2: Comparative query
    "Which policy has the highest annual premium and what does it cover?",

    # Type 3: Filter-based question
    "Which policies are in the state of Illinois?",

    # Type 4: Coverage/deductible question
    "Which policies have a deductible of $500 or less?",

    # Type 5: Umbrella coverage question
    "Which policies include umbrella coverage and what are the limits?",
]

current_user = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{current_user}/prime_ins_poc_rag_policy")

all_results = []

with mlflow.start_run(run_name=f"rag_policy_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:

    mlflow.set_tags({
        "model": MODEL_NAME,
        "embedding_model": EMBEDDING_MODEL,
        "pipeline": "rag_policy",
        "prompt_version": PROMPT_VERSION,
    })
    mlflow.log_params({
        "top_k": TOP_K,
        "total_policies": len(documents),
        "embedding_dim": EMBEDDING_DIM,
        "num_test_questions": len(test_questions),
    })

    for i, question in enumerate(test_questions):
        print(f"\n{'='*70}")
        print(f"  Q{i+1}: {question}")
        print(f"{'='*70}")

        start = datetime.now()
        result = ask(question)
        duration_ms = int((datetime.now() - start).total_seconds() * 1000)

        result["duration_ms"] = duration_ms
        result["model_name"] = MODEL_NAME
        result["embedding_model"] = EMBEDDING_MODEL
        result["prompt_version"] = PROMPT_VERSION
        result["generated_at"] = datetime.now().isoformat()

        all_results.append(result)

        print(f"\n  Answer: {result['answer'][:300]}")
        print(f"\n  Cited Policies: {result['cited_policies']}")
        print(f"  Confidence: {result['confidence_level']} ({result['confidence_score']:.1%})")
        print(f"  Duration: {duration_ms}ms")

    mlflow.log_metrics({
        "avg_confidence": np.mean([r["confidence_score"] for r in all_results]),
        "avg_duration_ms": np.mean([r["duration_ms"] for r in all_results]),
        "high_confidence_count": sum(1 for r in all_results if r["confidence_level"] == "HIGH"),
    })

    print(f"\n\n{'='*70}")
    print(f"  MLflow Run: {run.info.run_id}")
    print(f"{'='*70}")

# COMMAND ----------

# ============================================================
# SAVE QUERY HISTORY TO GOLD TABLE
# ============================================================

if all_results:
    # Convert numpy types to native Python types
    clean_results = []
    for r in all_results:
        clean_results.append({
            k: float(v) if isinstance(v, (np.floating, np.float32, np.float64))
            else int(v) if isinstance(v, (np.integer, np.int32, np.int64))
            else v
            for k, v in r.items()
        })

    results_df = spark.createDataFrame(clean_results)

    table_exists = spark.catalog.tableExists(OUTPUT_TABLE)

    if not table_exists:
        results_df.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)
        print(f"Created {OUTPUT_TABLE} with {len(clean_results)} rows")
    else:
        results_df.write.mode("append").saveAsTable(OUTPUT_TABLE)
        print(f"Appended {len(clean_results)} rows to {OUTPUT_TABLE}")

    total = spark.sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT_TABLE}").collect()[0]["cnt"]
    print(f"Total rows in {OUTPUT_TABLE}: {total}")

# COMMAND ----------

# ============================================================
# VERIFY FINAL OUTPUT
# ============================================================

display(spark.sql(f"""
    SELECT
        question,
        confidence_level,
        confidence_score,
        cited_policies,
        duration_ms,
        LEFT(answer, 150) as answer_preview
    FROM {OUTPUT_TABLE}
    ORDER BY generated_at DESC
    LIMIT 10
"""))

# COMMAND ----------

