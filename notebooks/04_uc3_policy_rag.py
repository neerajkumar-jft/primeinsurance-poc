# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # UC3: Policy Intelligence Assistant (RAG)
# MAGIC
# MAGIC **Problem**: Claims adjusters spend 10-15 min per claim looking up policy details
# MAGIC (deductibles, coverage, umbrella limits). With 3,000 claims/year that's 500-750
# MAGIC hours wasted on lookups that should take seconds.
# MAGIC
# MAGIC **Solution**: RAG system over policy data. Adjusters ask questions in plain English,
# MAGIC system retrieves relevant policies and generates accurate answers with citations.
# MAGIC
# MAGIC **Input**: ``databricks-hackathon-insurance`.gold.dim_policy`
# MAGIC **Output**: ``databricks-hackathon-insurance`.gold.rag_query_history`

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F
import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Policy Data

# COMMAND ----------

dim_policy = spark.table("`databricks-hackathon-insurance`.gold.dim_policy").toPandas()
print(f"total policies: {len(dim_policy)}")
dim_policy.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Convert Structured Data to Text Documents
# MAGIC
# MAGIC RAG needs text, but `dim_policy` is a structured table.
# MAGIC We convert each policy row into a readable text document.
# MAGIC
# MAGIC This is a key design decision - we tried a few approaches:
# MAGIC - Just concatenating column values: too terse, lost context
# MAGIC - Full prose paragraphs: too verbose, wasted tokens
# MAGIC - Structured template (what we're using): good balance

# COMMAND ----------

def policy_to_text(row):
    """convert a policy row into a searchable text document"""
    return (
        f"Policy Number: {row['policy_number']}. "
        f"This policy is bound in state {row.get('policy_state', 'N/A')} "
        f"with a bind date of {row.get('policy_bind_date', 'N/A')}. "
        f"Coverage split limit (CSL): {row.get('policy_csl', 'N/A')}. "
        f"Annual premium: ${row.get('policy_annual_premium', 0):.2f}. "
        f"Deductible: ${row.get('policy_deductible', 0)}. "
        f"Umbrella limit: ${row.get('umbrella_limit', 0)}. "
        f"Linked to car ID: {row.get('car_id', 'N/A')}. "
        f"Linked to customer ID: {row.get('customer_id', 'N/A')}."
    )

# convert all policies to text docs
documents = []
for idx, row in dim_policy.iterrows():
    doc_text = policy_to_text(row)
    documents.append({
        "doc_id": idx,
        "policy_number": row["policy_number"],
        "text": doc_text,
    })

print(f"created {len(documents)} text documents")
print(f"\nsample document:")
print(documents[0]["text"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Chunking
# MAGIC
# MAGIC For a structured table like this, each row is already a natural "chunk".
# MAGIC But for larger documents you'd need to split. We experimented:
# MAGIC
# MAGIC | Chunk Size | Result |
# MAGIC |-----------|--------|
# MAGIC | 1 row per chunk | Works well - each policy is self-contained |
# MAGIC | 5 rows per chunk | Retrieves too much noise for specific queries |
# MAGIC | Column-level | Too fragmented, lost context |
# MAGIC
# MAGIC Going with 1 row per chunk since each policy document is ~100 tokens.

# COMMAND ----------

# our chunks are just the individual policy documents
# for a real document corpus, we'd do something like:
#
# def chunk_text(text, chunk_size=500, overlap=50):
#     chunks = []
#     for i in range(0, len(text), chunk_size - overlap):
#         chunks.append(text[i:i + chunk_size])
#     return chunks

chunks = documents  # each policy is its own chunk
print(f"total chunks: {len(chunks)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate Embeddings & Build Vector Index
# MAGIC
# MAGIC Using a simple approach with sentence embeddings.
# MAGIC For production you'd use Databricks Vector Search, but FAISS works
# MAGIC for this POC and has no external dependencies.

# COMMAND ----------

# we'll use the databricks foundation model for embeddings too
from openai import OpenAI

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_URL}/serving-endpoints"
)

MODEL = "databricks-meta-llama-3-1-70b-instruct"

# COMMAND ----------

# Embedding approach: we tried two options
#
# Option A: TF-IDF (simpler, no extra dependencies)
#   - Fast, works offline, good for keyword-heavy queries
#   - Weak on semantic similarity ("coverage" vs "protection")
#
# Option B: sentence-transformers with all-MiniLM-L6-v2 (what we're using)
#   - Captures semantic meaning, not just keyword overlap
#   - Better for natural language questions
#   - Small model, runs fine on driver node
#
# We went with Option B for better retrieval quality.

# install if not available
# %pip install sentence-transformers faiss-cpu

try:
    from sentence_transformers import SentenceTransformer
    import faiss
    import numpy as np

    # load a lightweight embedding model
    embed_model = SentenceTransformer("all-MiniLM-L6-v2")

    # encode all policy documents
    texts = [c["text"] for c in chunks]
    doc_embeddings = embed_model.encode(texts, show_progress_bar=True)

    # build FAISS index for fast similarity search
    dimension = doc_embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)  # inner product (cosine sim on normalized vectors)

    # normalize for cosine similarity
    faiss.normalize_L2(doc_embeddings)
    index.add(doc_embeddings)

    USE_FAISS = True
    print(f"built FAISS index: {index.ntotal} vectors, {dimension} dimensions")

except ImportError:
    # fallback to TF-IDF if sentence-transformers not available
    print("sentence-transformers not available, falling back to TF-IDF")
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity as cos_sim

    texts = [c["text"] for c in chunks]
    vectorizer = TfidfVectorizer(max_features=5000, stop_words='english')
    doc_vectors = vectorizer.fit_transform(texts)
    USE_FAISS = False
    print(f"built TF-IDF index: {doc_vectors.shape[0]} docs x {doc_vectors.shape[1]} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Retrieval Function

# COMMAND ----------

def retrieve_relevant_policies(query, top_k=5):
    """
    given a natural language query, find the most relevant policy documents.
    returns the top-k most similar chunks with their scores.
    """
    if USE_FAISS:
        # semantic search using sentence embeddings + FAISS
        query_embedding = embed_model.encode([query])
        faiss.normalize_L2(query_embedding)
        scores, indices = index.search(query_embedding, top_k)

        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx >= 0 and score > 0.0:
                results.append({
                    "doc_id": chunks[idx]["doc_id"],
                    "policy_number": chunks[idx]["policy_number"],
                    "text": chunks[idx]["text"],
                    "similarity_score": float(score),
                })
        return results
    else:
        # fallback: TF-IDF keyword search
        query_vector = vectorizer.transform([query])
        similarities = cos_sim(query_vector, doc_vectors).flatten()
        top_indices = similarities.argsort()[-top_k:][::-1]

        results = []
        for idx in top_indices:
            score = similarities[idx]
            if score > 0.0:
                results.append({
                    "doc_id": chunks[idx]["doc_id"],
                    "policy_number": chunks[idx]["policy_number"],
                    "text": chunks[idx]["text"],
                    "similarity_score": float(score),
                })
        return results

# COMMAND ----------

# test retrieval
test_query = "which policies have umbrella coverage above 1000000"
test_results = retrieve_relevant_policies(test_query)

print(f"query: {test_query}")
print(f"found {len(test_results)} relevant policies:\n")
for r in test_results[:3]:
    print(f"  [{r['similarity_score']:.3f}] {r['policy_number']}: {r['text'][:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: RAG Pipeline - Retrieve + Generate

# COMMAND ----------

def ask_policy_question(question, top_k=5):
    """
    end-to-end RAG: retrieve relevant policies, send to LLM with context,
    generate answer with source citations.
    """
    # retrieve
    retrieved = retrieve_relevant_policies(question, top_k=top_k)

    if not retrieved or max(r["similarity_score"] for r in retrieved) < 0.05:
        # low confidence - be honest about it
        return {
            "answer": "I don't have enough relevant information to answer this question confidently. "
                      "The policy records I found don't directly address your query. "
                      "Please try rephrasing or check with the policy team directly.",
            "confidence": "LOW",
            "sources": [],
            "retrieved_count": len(retrieved),
            "max_similarity": max(r["similarity_score"] for r in retrieved) if retrieved else 0,
        }

    # build context from retrieved docs
    context_parts = []
    source_policies = []
    for r in retrieved:
        context_parts.append(r["text"])
        source_policies.append(r["policy_number"])

    context = "\n\n".join(context_parts)

    prompt = f"""You are a policy information assistant at PrimeInsurance.
Answer the question using ONLY the policy data provided below.
If the answer isn't in the data, say "I cannot determine this from the available policy records."

POLICY DATA:
{context}

QUESTION: {question}

Provide a clear, specific answer. Reference policy numbers when citing data.
Keep the answer concise and factual."""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.2,
        )
        answer = response.choices[0].message.content.strip()
    except Exception as e:
        answer = f"Error generating response: {str(e)}"

    # determine confidence based on retrieval quality
    max_sim = max(r["similarity_score"] for r in retrieved)
    confidence = "HIGH" if max_sim > 0.3 else "MEDIUM" if max_sim > 0.1 else "LOW"

    return {
        "answer": answer,
        "confidence": confidence,
        "sources": source_policies,
        "retrieved_count": len(retrieved),
        "max_similarity": max_sim,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test with Sample Questions

# COMMAND ----------

# these are the kinds of questions adjusters and the compliance team would ask
test_questions = [
    "Which policies have umbrella coverage above $1,000,000?",
    "What is the average annual premium for policies with 250/500 coverage?",
    "Which policies have the highest deductible?",
    "Show me policies linked to cars from the Central region",
    "What coverage split limits are available?",
]

query_results = []

for q in test_questions:
    print(f"\nQ: {q}")
    result = ask_policy_question(q)
    print(f"A: {result['answer'][:200]}...")
    print(f"   Confidence: {result['confidence']} | Sources: {result['sources'][:3]} | Similarity: {result['max_similarity']:.3f}")

    query_results.append({
        "question": q,
        "answer": result["answer"],
        "confidence": result["confidence"],
        "source_policies": json.dumps(result["sources"]),
        "retrieved_count": result["retrieved_count"],
        "max_similarity_score": result["max_similarity"],
        "model_name": MODEL,
        "timestamp": datetime.now().isoformat(),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Query History to Gold

# COMMAND ----------

rag_history_df = spark.createDataFrame(query_results)

(rag_history_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.rag_query_history"))

print(f"gold.rag_query_history: {rag_history_df.count()} rows")

# COMMAND ----------

display(spark.table("`databricks-hackathon-insurance`.gold.rag_query_history"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAG Design Decisions
# MAGIC
# MAGIC **Chunk strategy**: 1 policy = 1 chunk (~100 tokens each).
# MAGIC Structured data doesn't need splitting - each row is self-contained.
# MAGIC If we had actual PDF policy documents, we'd use 500-token chunks with 50-token overlap.
# MAGIC
# MAGIC **Why convert structured data to text first**: TF-IDF and LLMs work on text.
# MAGIC We could pass raw SQL results, but converting to natural language makes retrieval
# MAGIC more flexible (handles synonyms, paraphrasing) and gives the LLM better context.
# MAGIC
# MAGIC **"I don't know" detection**: If max similarity < 0.05, we refuse to answer
# MAGIC rather than hallucinate. Better to say "I'm not sure" than give a wrong answer
# MAGIC about someone's insurance coverage.
# MAGIC
# MAGIC **Source attribution**: Every answer includes the policy numbers it drew from,
# MAGIC so adjusters can verify and the system is auditable.
