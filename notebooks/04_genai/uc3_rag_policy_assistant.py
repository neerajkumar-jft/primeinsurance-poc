# Databricks notebook source
# MAGIC %md
# MAGIC # UC3: RAG Policy Q&A Assistant
# MAGIC Converts dim_policy rows into enriched natural language documents,
# MAGIC embeds them locally with sentence-transformers (all-MiniLM-L6-v2),
# MAGIC builds a FAISS index with cosine similarity, and answers natural
# MAGIC language questions with cited policy numbers.
# MAGIC
# MAGIC One-policy = one-chunk strategy with zero overlap. Each policy row
# MAGIC is a self-contained record (~100-150 tokens), so splitting would
# MAGIC break semantic meaning.
# MAGIC
# MAGIC Output: prime_insurance_jellsinki_poc.gold.rag_query_history

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu openai
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

from openai import OpenAI
import json
import numpy as np

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)
MODEL = "databricks-gpt-oss-20b"
TOP_K = 5
print(f"LLM: {MODEL}, TOP_K: {TOP_K}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load policies and convert to enriched text documents
# MAGIC
# MAGIC Raw structured rows are converted to natural language paragraphs because
# MAGIC embedding models (trained on web text) don't understand key-value formats.
# MAGIC Without conversion, semantic queries would produce very low similarity scores.
# MAGIC
# MAGIC Enrichments added during conversion:
# MAGIC - CSL decoding: "100/300" -> "$100K per person and $300K per accident"
# MAGIC - Premium tier: >= $2000 Premium, >= $1000 Standard, < $1000 Basic
# MAGIC - Conditional umbrella mention (has coverage vs no coverage)
# MAGIC - Policy number in text for citation

# COMMAND ----------

policies = spark.sql("""
    SELECT policy_key, policy_number, policy_bind_date, policy_state,
           policy_csl, policy_deductible, policy_annual_premium, umbrella_limit
    FROM prime_insurance_jellsinki_poc.gold.dim_policies
""").collect()

print(f"Loaded {len(policies)} policies")

def decode_csl(csl_str):
    """Decode CSL like '100/300' to human readable form."""
    try:
        parts = str(csl_str).split("/")
        if len(parts) == 2:
            return f"${parts[0]}K per person and ${parts[1]}K per accident"
        elif len(parts) == 3:
            return f"${parts[0]}K per person, ${parts[1]}K per accident, and ${parts[2]}K property damage"
        return str(csl_str)
    except:
        return str(csl_str)

def get_premium_tier(premium):
    """Classify premium into tiers."""
    if premium >= 2000:
        return "Premium"
    elif premium >= 1000:
        return "Standard"
    else:
        return "Basic"

# Convert each policy to a natural language document
documents = []
metadata = []

for p in policies:
    premium = float(p['policy_annual_premium'])
    umbrella = int(p['umbrella_limit'])
    tier = get_premium_tier(premium)
    csl_decoded = decode_csl(p['policy_csl'])

    # Conditional umbrella text
    if umbrella > 0:
        umbrella_text = f"The policy includes umbrella coverage with a limit of ${umbrella:,}."
    else:
        umbrella_text = "This policy does not include umbrella coverage."

    doc = (
        f"Policy {p['policy_number']} is an auto insurance policy bound on "
        f"{p['policy_bind_date']} in the state of {p['policy_state']}. "
        f"The policy has a combined single limit (CSL) of {p['policy_csl']}, "
        f"providing bodily injury coverage of {csl_decoded}. "
        f"The deductible is ${p['policy_deductible']:,} and the annual premium "
        f"is ${premium:,.2f}. {umbrella_text} "
        f"This is a {tier} tier policy."
    )

    documents.append(doc)
    metadata.append({
        "policy_number": p["policy_number"],
        "policy_key": p["policy_key"],
        "policy_state": p["policy_state"],
        "policy_csl": p["policy_csl"],
        "premium": premium,
        "deductible": int(p["policy_deductible"]),
        "umbrella": umbrella,
        "tier": tier,
    })

print(f"Created {len(documents)} text documents")
print(f"\nSample document:\n{documents[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate embeddings locally with sentence-transformers

# COMMAND ----------

from sentence_transformers import SentenceTransformer

# all-MiniLM-L6-v2: 384-dim embeddings, fast, runs locally on CPU
embed_model = SentenceTransformer("all-MiniLM-L6-v2")

embeddings = embed_model.encode(documents, show_progress_bar=True, batch_size=64)
embeddings = np.array(embeddings).astype("float32")

# Normalize for cosine similarity (IndexFlatIP on normalized vectors = cosine similarity)
faiss_module = __import__("faiss")
faiss_module.normalize_L2(embeddings)

print(f"Embeddings shape: {embeddings.shape} (normalized for cosine similarity)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Build FAISS vector index
# MAGIC Using IndexFlatIP (inner product) on normalized vectors = cosine similarity.

# COMMAND ----------

import faiss

dimension = embeddings.shape[1]
index = faiss.IndexFlatIP(dimension)  # Inner product on normalized vectors = cosine similarity
index.add(embeddings)

print(f"FAISS index: {index.ntotal} vectors, {dimension} dimensions, cosine similarity")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: RAG query function with confidence scoring

# COMMAND ----------

def parse_llm_response(response):
    content = response.choices[0].message.content
    try:
        parsed = json.loads(content)
        if isinstance(parsed, list):
            for block in parsed:
                if block.get("type") == "text":
                    return block.get("text", content)
        return content
    except (json.JSONDecodeError, TypeError):
        return content


def compute_confidence(scores):
    """
    Confidence from three signals:
    - Top similarity score (50% weight)
    - Score gap between #1 and #2 (20% weight) - larger gap = more decisive match
    - Average of top-3 scores (30% weight)
    All scores are cosine similarity [0, 1] since we normalized.
    """
    top_score = float(scores[0])
    gap = float(scores[0] - scores[1]) if len(scores) > 1 else 0.0
    avg_top3 = float(np.mean(scores[:3])) if len(scores) >= 3 else float(np.mean(scores))

    confidence = (top_score * 50) + (gap * 20 / 0.1) + (avg_top3 * 30)
    # Clamp to 0-100 range
    return round(max(0, min(100, confidence)), 1)


def query_policies(question, top_k=TOP_K):
    """
    1. Embed the question (same model as documents)
    2. Search FAISS for top_k most similar policy chunks
    3. Pass retrieved context + question to LLM
    4. Return answer with cited policy numbers and confidence
    """
    # Embed and normalize the question
    q_embedding = embed_model.encode([question]).astype("float32")
    faiss_module.normalize_L2(q_embedding)

    # Search FAISS - returns cosine similarity scores (higher = more similar)
    scores, indices = index.search(q_embedding, top_k)

    # Collect retrieved documents and metadata
    retrieved_docs = []
    source_policies = []
    for i, idx in enumerate(indices[0]):
        retrieved_docs.append(documents[idx])
        source_policies.append(metadata[idx]["policy_number"])

    context = "\n\n".join(retrieved_docs)
    confidence = compute_confidence(scores[0])

    # LLM prompt with retrieved context
    prompt = f"""You are a policy assistant at PrimeInsurance. Answer the question
using ONLY the policy information provided below. Always cite specific policy
numbers in your answer. If the information is not in the context, say so.

Policy context:
{context}

Question: {question}

Answer concisely, citing policy numbers."""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300,
        temperature=0.2
    )

    answer = parse_llm_response(response)

    return {
        "question": question,
        "answer": answer,
        "confidence_score": confidence,
        "source_policies": ", ".join([str(p) for p in source_policies]),
        "num_sources": len(source_policies),
        "top_similarity": round(float(scores[0][0]), 4),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test with 5 questions covering different query types

# COMMAND ----------

test_questions = [
    # Specific policy lookup
    "What is the deductible and coverage tier for policy 100804?",

    # Comparative query
    "Which policies have the highest annual premiums? Compare the top 3.",

    # Filter-based / umbrella coverage
    "Which policies include umbrella coverage and what are the limits?",

    # Coverage type question
    "What does a 250/500 coverage tier mean, and which policies have it?",

    # Analytical
    "Show me policies in Illinois with Premium tier coverage.",
]

results = []

for i, q in enumerate(test_questions):
    print(f"\n{'='*60}")
    print(f"Q{i+1}: {q}")
    print(f"{'='*60}")

    result = query_policies(q)
    results.append(result)

    print(f"Answer: {result['answer'][:200]}...")
    print(f"Confidence: {result['confidence_score']}, Top similarity: {result['top_similarity']}")
    print(f"Sources: {result['source_policies']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write query history to gold.rag_query_history

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime

schema = StructType([
    StructField("question", StringType(), False),
    StructField("answer", StringType(), True),
    StructField("confidence_score", DoubleType(), True),
    StructField("source_policies", StringType(), True),
    StructField("num_sources", IntegerType(), True),
    StructField("queried_at", TimestampType(), False),
])

rows = [{
    "question": r["question"],
    "answer": r["answer"],
    "confidence_score": r["confidence_score"],
    "source_policies": r["source_policies"],
    "num_sources": r["num_sources"],
    "queried_at": datetime.now(),
} for r in results]

df = spark.createDataFrame(rows, schema)

df.write.mode("overwrite").saveAsTable(
    "prime_insurance_jellsinki_poc.gold.rag_query_history"
)
print("Written to prime_insurance_jellsinki_poc.gold.rag_query_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Verify output

# COMMAND ----------

# MAGIC %sql
SELECT question, answer, confidence_score, source_policies, queried_at
FROM prime_insurance_jellsinki_poc.gold.rag_query_history
ORDER BY queried_at;
