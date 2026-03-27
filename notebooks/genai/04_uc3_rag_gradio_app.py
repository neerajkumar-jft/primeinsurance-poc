# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Policy Intelligence Assistant
# MAGIC
# MAGIC Interactive RAG interface for PrimeInsurance policy lookups.
# MAGIC Builds a FAISS index on startup, then serves a Gradio UI for
# MAGIC natural language questions with cited policy numbers.
# MAGIC
# MAGIC Run this notebook to launch the app. Keep the session open
# MAGIC while the app is in use.

# COMMAND ----------

%pip install openai sentence-transformers faiss-cpu gradio --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ============================================================
# CONFIGURATION
# ============================================================

import json
import numpy as np
from openai import OpenAI
from sentence_transformers import SentenceTransformer
import faiss
import gradio as gr
import time

dbutils.widgets.text("catalog", "primeins")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{CATALOG}`")

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

MODEL_NAME = "databricks-gpt-oss-20b"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
TOP_K = 5
SOURCE_TABLE = f"`{CATALOG}`.gold.dim_policy"

print(f"Catalog: {CATALOG}")
print(f"LLM: {MODEL_NAME}")
print(f"Source: {SOURCE_TABLE}")

# COMMAND ----------

# ============================================================
# BUILD RAG INDEX (runs once on startup)
# ============================================================

print("Loading policies...")
policies_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}").toPandas()
print(f"Loaded {len(policies_df)} policies")

# CSL decoder
def decode_csl(csl):
    try:
        parts = str(csl).split("/")
        if len(parts) == 3:
            return f"${parts[0]}K per person, ${parts[1]}K per accident, and ${parts[2]}K property damage"
        if len(parts) == 2:
            return f"${parts[0]}K per person and ${parts[1]}K per accident"
    except Exception:
        pass
    return str(csl)

# Convert each policy to a document
documents = []
metadata = []

for _, row in policies_df.iterrows():
    premium = row['policy_annual_premium']
    umbrella = row['umbrella_limit']

    if premium >= 2000:
        tier = "Premium"
    elif premium >= 1000:
        tier = "Standard"
    else:
        tier = "Basic"

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

    if umbrella and umbrella > 0:
        doc += f" The policy includes umbrella coverage with a limit of ${int(umbrella):,}."
    else:
        doc += " This policy does not include umbrella coverage."

    doc += f" This is a {tier} tier policy."

    documents.append(doc)
    metadata.append({
        "text": doc,
        "policy_number": str(int(row["policy_number"])),
        "policy_state": row["policy_state"],
        "policy_csl": row["policy_csl"],
        "deductible": int(row["policy_deductible"]),
        "premium": float(row["policy_annual_premium"]),
        "umbrella_limit": int(umbrella) if umbrella else 0,
        "tier": tier,
    })

print(f"Converted {len(documents)} documents")

# Embed
print("Embedding documents...")
embed_model = SentenceTransformer(EMBEDDING_MODEL)
embeddings = embed_model.encode(documents, normalize_embeddings=True, show_progress_bar=True, batch_size=64)
embeddings = embeddings.astype(np.float32)

# Build FAISS index
base_index = faiss.IndexFlatIP(EMBEDDING_DIM)
index = faiss.IndexIDMap(base_index)
ids = np.arange(len(embeddings)).astype(np.int64)
index.add_with_ids(embeddings, ids)
print(f"FAISS index ready: {index.ntotal} vectors")

# COMMAND ----------

# ============================================================
# RAG FUNCTIONS
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


def retrieve(query, top_k=TOP_K):
    query_emb = embed_model.encode([query], normalize_embeddings=True).astype(np.float32)
    scores, indices = index.search(query_emb, top_k)
    results = []
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
        results.append({
            **metadata[idx],
            "similarity": round(float(score), 4),
        })
    return results


def compute_confidence(results):
    if not results:
        return "LOW", 0.0
    top = results[0]["similarity"]
    gap = (results[0]["similarity"] - results[1]["similarity"]) if len(results) > 1 else 0.0
    avg3 = float(np.mean([r["similarity"] for r in results[:3]])) if len(results) >= 3 else float(np.mean([r["similarity"] for r in results]))
    score = round((0.5 * top) + (0.2 * gap) + (0.3 * avg3), 4)
    if top >= 0.65:
        level = "HIGH"
    elif top >= 0.35:
        level = "MEDIUM"
    else:
        level = "LOW"
    return level, score


SYSTEM_PROMPT = """You are a policy assistant at PrimeInsurance. Answer using ONLY the policy documents below. Cite specific policy numbers. If the information is not in the context, say so.

Respond in this JSON format:
{
    "answer": "Your answer with cited policy numbers",
    "cited_policies": "Comma-separated policy numbers"
}

Do NOT add extra keys. Do NOT wrap in markdown fences."""


def ask(question):
    start = time.time()

    # Retrieve
    results = retrieve(question)
    level, conf_score = compute_confidence(results)

    # Build context
    context_parts = []
    for r in results[:3]:
        context_parts.append(f"[Policy {r['policy_number']}] (similarity: {r['similarity']:.1%})\n{r['text']}")
    context = "\n\n".join(context_parts)

    # Call LLM
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Policy documents:\n\n{context}\n\nQuestion: {question}"},
            ],
            max_tokens=500,
        )
        raw = response.choices[0].message.content
        text = extract_text(raw)

        # Try to parse JSON answer
        try:
            first = text.find('{')
            last = text.rfind('}')
            if first != -1 and last > first:
                data = json.loads(text[first:last+1])
                answer = data.get("answer", text)
                cited = data.get("cited_policies", "")
            else:
                answer = text
                cited = ""
        except json.JSONDecodeError:
            answer = text
            cited = ""

    except Exception as e:
        answer = f"Error: {str(e)}"
        cited = ""

    duration = round(time.time() - start, 2)

    # Format retrieved policies for display
    retrieved_display = ""
    for i, r in enumerate(results):
        umb = f"${r['umbrella_limit']:,}" if r['umbrella_limit'] > 0 else "None"
        retrieved_display += (
            f"{i+1}. Policy {r['policy_number']} | "
            f"State: {r['policy_state']} | "
            f"CSL: {r['policy_csl']} | "
            f"Deductible: ${r['deductible']:,} | "
            f"Premium: ${r['premium']:,.0f} | "
            f"Umbrella: {umb} | "
            f"Tier: {r['tier']} | "
            f"Similarity: {r['similarity']:.1%}\n"
        )

    # Confidence color
    if level == "HIGH":
        conf_display = f"HIGH ({conf_score:.1%})"
    elif level == "MEDIUM":
        conf_display = f"MEDIUM ({conf_score:.1%})"
    else:
        conf_display = f"LOW ({conf_score:.1%})"

    meta = f"Confidence: {conf_display}  |  Cited: {cited if cited else 'None'}  |  Duration: {duration}s  |  Model: {MODEL_NAME}"

    return answer, meta, retrieved_display


# COMMAND ----------

# ============================================================
# GRADIO UI
# ============================================================

EXAMPLES = [
    "What are the details of policy 100804?",
    "Which policy has the highest annual premium?",
    "Which policies are in the state of Illinois?",
    "Which policies have a deductible of $500 or less?",
    "Which policies include umbrella coverage and what are the limits?",
    "Compare the coverage tiers for policies in Ohio.",
    "What does a 250/500 CSL mean?",
]

with gr.Blocks(
    title="PrimeInsurance Policy Assistant",
    theme=gr.themes.Soft(),
    css="""
        .main-header { text-align: center; margin-bottom: 10px; }
        .meta-box { background: #f0f4f8; padding: 10px; border-radius: 6px; font-family: monospace; font-size: 13px; }
        .retrieved-box { background: #fafafa; padding: 10px; border-radius: 6px; font-family: monospace; font-size: 12px; white-space: pre-wrap; }
    """
) as app:

    gr.Markdown(
        """
        # PrimeInsurance Policy Assistant
        Ask questions about insurance policies in plain English. Answers are grounded in policy data from Unity Catalog.
        """,
        elem_classes=["main-header"]
    )

    with gr.Row():
        with gr.Column(scale=4):
            question_input = gr.Textbox(
                label="Your question",
                placeholder="e.g. What is the deductible for policy 100804?",
                lines=2,
            )
        with gr.Column(scale=1):
            ask_btn = gr.Button("Ask", variant="primary", size="lg")

    gr.Markdown("**Try an example:**")
    with gr.Row():
        example_btns = []
        for ex in EXAMPLES[:4]:
            btn = gr.Button(ex[:45] + "..." if len(ex) > 45 else ex, size="sm", variant="secondary")
            example_btns.append((btn, ex))

    with gr.Row():
        for ex in EXAMPLES[4:]:
            btn = gr.Button(ex[:45] + "..." if len(ex) > 45 else ex, size="sm", variant="secondary")
            example_btns.append((btn, ex))

    gr.Markdown("---")

    answer_output = gr.Textbox(label="Answer", lines=6, interactive=False)
    meta_output = gr.Textbox(label="", lines=1, interactive=False, elem_classes=["meta-box"])

    with gr.Accordion("Retrieved policies (click to expand)", open=False):
        retrieved_output = gr.Textbox(lines=8, interactive=False, show_label=False, elem_classes=["retrieved-box"])

    gr.Markdown(
        f"""
        ---
        **{len(documents)} policies indexed** from `{SOURCE_TABLE}` | Embedding: {EMBEDDING_MODEL} ({EMBEDDING_DIM}d) | LLM: {MODEL_NAME} | Top-K: {TOP_K}
        """,
        elem_classes=["meta-box"]
    )

    # Wire up the ask button
    ask_btn.click(
        fn=ask,
        inputs=[question_input],
        outputs=[answer_output, meta_output, retrieved_output],
    )

    # Wire up enter key
    question_input.submit(
        fn=ask,
        inputs=[question_input],
        outputs=[answer_output, meta_output, retrieved_output],
    )

    # Wire up example buttons
    for btn, ex_text in example_btns:
        btn.click(
            fn=lambda t=ex_text: (t,),
            outputs=[question_input],
        ).then(
            fn=ask,
            inputs=[question_input],
            outputs=[answer_output, meta_output, retrieved_output],
        )

# Launch
app.launch()
