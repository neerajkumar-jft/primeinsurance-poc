# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # UC1: Data Quality Explanation Engine
# MAGIC
# MAGIC **Problem**: The Silver layer pipeline logs every data quality issue it catches -- bad values, schema
# MAGIC mismatches, null violations. These logs land in `silver.dq_issues`. The table is accurate but
# MAGIC completely unreadable to the compliance team. They need to understand what went wrong, what the
# MAGIC business impact is, and what should be done -- in plain English.
# MAGIC
# MAGIC **Solution**: A notebook that reads every row from `silver.dq_issues`, sends each issue to the LLM
# MAGIC with a structured prompt, and writes the AI-generated business explanation back to
# MAGIC `gold.dq_explanation_report`.
# MAGIC
# MAGIC **Architecture decisions**:
# MAGIC - Uses `databricks-gpt-oss-20b` via OpenAI-compatible API
# MAGIC - Pydantic model validates all LLM responses before writing to Gold
# MAGIC - Hash-based incremental processing skips unchanged issues on re-runs
# MAGIC - MLflow tracing captures every LLM call for observability
# MAGIC - PII guardrails redact sensitive patterns from LLM output
# MAGIC
# MAGIC **Input**: `{catalog}.silver.dq_issues`
# MAGIC **Output**: `{catalog}.gold.dq_explanation_report`

# COMMAND ----------

%pip install openai mlflow tenacity pydantic --quiet

# COMMAND ----------

dbutils.library.restartPython()
import warnings
warnings.filterwarnings("ignore", message="Pydantic serializer warnings")

# COMMAND ----------

# ============================================================
# CONFIGURATION & SETUP
# ============================================================

import mlflow
import json
import hashlib
import re
from datetime import datetime
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_random_exponential
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional

# ── MLflow Tracing ──
mlflow.openai.autolog()

# ── LLM Connection ──
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

# ── Catalog configuration (dynamic, no hardcoding) ──
dbutils.widgets.text("catalog", "primeins")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{CATALOG}`")

# ── Constants ──
MODEL_NAME = "databricks-gpt-oss-20b"
MAX_TOKENS = 1500
SOURCE_TABLE = f"`{CATALOG}`.silver.dq_issues"
OUTPUT_TABLE = f"`{CATALOG}`.gold.dq_explanation_report"
PROMPT_VERSION = "v2"

print(f"Connected to {WORKSPACE_URL}")
print(f"Model: {MODEL_NAME}")
print(f"Source: {SOURCE_TABLE}")
print(f"Output: {OUTPUT_TABLE}")

# COMMAND ----------

# ── Response parser for databricks-gpt-oss-20b ──
def extract_text(raw_response):
    """Extracts the text block from the model's structured response format."""
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


# ── LLM call with retry + JSON mode enforced ──
@retry(
    wait=wait_random_exponential(min=2, max=60),
    stop=stop_after_attempt(5),
)
def call_llm(messages, max_tokens=MAX_TOKENS):
    """Calls LLM with JSON mode enforced. Returns parsed dict."""
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        max_tokens=max_tokens,
    )
    raw = response.choices[0].message.content
    text = extract_text(raw)
    return json.loads(text)


# ── Hash function for incremental change detection ──
def hash_issue(issue):
    """Creates MD5 hash of issue content to detect changes."""
    content = f"{issue['rule_name']}|{issue['severity']}|{issue['affected_records']}|{issue['affected_ratio']}|{issue['suggested_fix']}"
    return hashlib.md5(content.encode()).hexdigest()


# ── Output guardrails ──
def apply_guardrails(text):
    """Redacts PII patterns from LLM output."""
    text = re.sub(r'\b[\w.-]+@[\w.-]+\.\w+\b', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[REDACTED_PHONE]', text)
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    return text


print("Helper functions initialized")

# COMMAND ----------

class DQExplanation(BaseModel):
    """Validated, structured explanation for a data quality issue."""

    what_was_found: str = Field(min_length=20)
    why_it_matters: str = Field(min_length=20)
    what_caused_it: str = Field(min_length=20)
    what_was_done: str = Field(min_length=20)
    how_to_prevent: str = Field(min_length=20)

    @field_validator('*', mode='before')
    @classmethod
    def clean_input(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('n/a', 'none', 'null', '', '-'):
                return "[Not provided by model — requires manual review]"
        return v

    def to_formatted_text(self) -> str:
        return (
            f"1. WHAT WAS FOUND:\n{self.what_was_found}\n\n"
            f"2. WHY IT MATTERS:\n{self.why_it_matters}\n\n"
            f"3. WHAT CAUSED IT:\n{self.what_caused_it}\n\n"
            f"4. WHAT WAS DONE:\n{self.what_was_done}\n\n"
            f"5. HOW TO PREVENT RECURRENCE:\n{self.how_to_prevent}"
        )

print("Pydantic output model initialized")

# COMMAND ----------

# ============================================================
# PROMPT DEFINITION
# ============================================================

SYSTEM_PROMPT = """You are a senior data quality analyst at PrimeInsurance with 15 years of experience in P&C insurance data management. PrimeInsurance acquired 6 regional insurance
operators — each had its own databases, formats, and naming conventions. Nothing was ever unified.

Your job: translate technical DQ issues into plain business English for the compliance team.

<severity_definitions>
- CRITICAL: Data is unusable or creates regulatory risk. Immediate action required.
- HIGH: Data is unreliable for business decisions. Action needed within days.
- MEDIUM: Data has inconsistencies that could cause confusion. Should be fixed in next sprint.
- LOW: Minor formatting issue. Fix when convenient.
</severity_definitions>

You must always respond in json format with exactly these 5 keys:
{
    "what_was_found": "2-3 sentences describing the issue in plain English",
    "why_it_matters": "2-3 sentences on business impact",
    "what_caused_it": "2-3 sentences on root cause",
    "what_was_done": "2-3 sentences on what the pipeline did",
    "how_to_prevent": "2-3 sentences on prevention"
}

<constraints>
- Respond with ONLY the json object, nothing else
- Do NOT add any keys other than the 5 listed above
- Do NOT use "N/A" — write a real explanation for every field
- Do NOT invent data not in the issue details
- Reference actual numbers and file names from the issue
</constraints>"""


USER_PROMPT_TEMPLATE = """
<example>
Issue: DQ-099 | Table: customers | Column: Region | Rule: value_abbreviation | Severity: MEDIUM | Affected: 199 (5.2%) | Fix: Expand W->West | Source: customers_6.csv

Response:
{{
    "what_was_found": "In customers_6.csv, 199 customer records use single-letter region codes ('W', 'C', 'E', 'S') instead of full region names. This affects 5.2% of total customer
records.",
    "why_it_matters": "Region-based compliance reports will show mismatched categories — 'W' and 'West' appear as separate regions, inflating the region count and skewing per-region
customer totals used in regulatory filings.",
    "what_caused_it": "The regional operator acquired as Insurance 6 used an older database system with a single-character Region field. When PrimeInsurance received the data export, the
abbreviations were preserved as-is.",
    "what_was_done": "The pipeline's Silver layer detected the abbreviated values and expanded them to full region names. The original values were preserved in the Bronze layer for audit
traceability.",
    "how_to_prevent": "Add a region validation rule at the ingestion layer that rejects or auto-maps any value not in the approved list. Provide the mapping table to all regional data
providers."
}}
</example>

Now explain this issue:

- Table: {table_name}
- Column: {column_name}
- Rule: {rule_name}
- Rule Condition: {rule_condition}
- Action Taken: {action_taken}
- Severity: {severity}
- Affected Records: {affected_records} out of {total_records} ({affected_ratio:.1%} of records)
- Suggested Fix: {suggested_fix}"""


print(f"Prompt defined - system prompt: {len(SYSTEM_PROMPT)} chars")

# COMMAND ----------

# ============================================================
# INCREMENTAL LOADING - Only process new/changed issues
# ============================================================

all_issues = [row.asDict() for row in spark.sql(f"SELECT * FROM {SOURCE_TABLE} ORDER BY severity").collect()]

# Normalize severity to uppercase (source may have lowercase: medium, critical)
for issue in all_issues:
    if issue.get("severity"):
        issue["severity"] = issue["severity"].upper()
print(f"Total issues in {SOURCE_TABLE}: {len(all_issues)}")

# Load previously processed hashes
processed_map = {}
try:
    processed_rows = [row.asDict() for row in spark.sql(f"SELECT issue_id, issue_hash FROM {OUTPUT_TABLE}").collect()]
    processed_map = {r['issue_id']: r['issue_hash'] for r in processed_rows}
    print(f"Previously processed: {len(processed_map)} issues")
except Exception:
    print(f"No existing {OUTPUT_TABLE} - first run, will process all")

# Determine new/changed issues
new_issues = []
skipped = []

for issue in all_issues:
    current_hash = hash_issue(issue)
    if f"DQ-{all_issues.index(issue)+1:04d}" not in processed_map or processed_map[f"DQ-{all_issues.index(issue)+1:04d}"] != current_hash:
        new_issues.append(issue)
    else:
        skipped.append(issue)

print(f"\nIncremental Status:")
print(f"   New or changed: {len(new_issues)} (will process)")
print(f"   Unchanged:      {len(skipped)} (will skip)")

if len(new_issues) == 0:
    print("All explanations are up to date. Nothing to process.")

# COMMAND ----------

# DBTITLE 1,Cell 8
# ============================================================
# GENERATE AI EXPLANATIONS - with MLflow tracking
# ============================================================

if len(new_issues) == 0:
    print("No new issues to process.")
    results = []
else:
    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    mlflow.set_experiment(f"/Users/{current_user}/prime_ins_poc_dq_explainer")

    results = []

    with mlflow.start_run(run_name=f"dq_explainer_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:

        mlflow.set_tags({
            "model": MODEL_NAME,
            "prompt_version": PROMPT_VERSION,
            "pipeline": "dq_explainer",
            "source_table": SOURCE_TABLE,
            "output_table": OUTPUT_TABLE,
        })
        mlflow.log_params({
            "max_tokens": MAX_TOKENS,
            "total_issues": len(all_issues),
            "new_issues": len(new_issues),
            "skipped_issues": len(skipped),
            "prompt_version": PROMPT_VERSION,
            "model": MODEL_NAME,
        })

        success_count = 0
        fail_count = 0

        for idx, issue in enumerate(new_issues):
            issue_start = datetime.now()

            try:
                # Compute total_records from affected_records / affected_ratio
                issue["total_records"] = int(round(issue["affected_records"] / issue["affected_ratio"])) if issue.get("affected_ratio", 0) > 0 else 0

                messages = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": USER_PROMPT_TEMPLATE.format(**issue)},
                ]

                # Call LLM - returns dict (JSON mode enforced)
                data = call_llm(messages)

                # Validate with Pydantic
                explanation = DQExplanation(**data)

                # Apply guardrails on the formatted output
                formatted = apply_guardrails(explanation.to_formatted_text())

                duration_ms = int((datetime.now() - issue_start).total_seconds() * 1000)

                results.append({
                    "issue_id": f"DQ-{idx+1:04d}",
                    "table_name": issue["table_name"],
                    "rule_name": issue["rule_name"],
                    "severity": issue["severity"],
                    "affected_ratio": float(issue["affected_ratio"]),
                    "affected_records": int(issue["affected_records"]),
                    "total_records": int(round(issue["affected_records"] / issue["affected_ratio"])) if issue.get("affected_ratio", 0) > 0 else 0,
                    "what_was_found": explanation.what_was_found,
                    "why_it_matters": explanation.why_it_matters,
                    "what_caused_it": explanation.what_caused_it,
                    "what_was_done": explanation.what_was_done,
                    "how_to_prevent": explanation.how_to_prevent,
                    "ai_explanation": formatted,
                    "issue_hash": hash_issue(issue),
                    "model_name": MODEL_NAME,
                    "prompt_version": PROMPT_VERSION,
                    "generated_at": datetime.now().isoformat(),
                    "status": "SUCCESS",
                    "duration_ms": duration_ms,
                })

                success_count += 1
                issue_id = f"DQ-{all_issues.index(issue)+1:04d}"
                print(f"[{idx+1}/{len(new_issues)}] {issue_id} | {issue['severity']:8s} | {issue['table_name']}.{issue['rule_name']} - {duration_ms}ms")

            except Exception as e:
                fail_count += 1
                duration_ms = int((datetime.now() - issue_start).total_seconds() * 1000)

                results.append({
                    "issue_id": f"DQ-{idx+1:04d}",
                    "table_name": issue["table_name"],
                    "rule_name": issue["rule_name"],
                    "severity": issue["severity"],
                    "affected_ratio": float(issue["affected_ratio"]),
                    "affected_records": int(issue["affected_records"]),
                    "total_records": int(round(issue["affected_records"] / issue["affected_ratio"])) if issue.get("affected_ratio", 0) > 0 else 0,
                    "what_was_found": f"Generation failed: {str(e)}",
                    "why_it_matters": "",
                    "what_caused_it": "",
                    "what_was_done": "",
                    "how_to_prevent": "",
                    "ai_explanation": f"Generation failed: {str(e)}",
                    "issue_hash": hash_issue(issue),
                    "model_name": MODEL_NAME,
                    "prompt_version": PROMPT_VERSION,
                    "generated_at": datetime.now().isoformat(),
                    "status": "FAILED",
                    "duration_ms": duration_ms,
                })
                issue_id = f"DQ-{all_issues.index(issue)+1:04d}"
                print(f"[{idx+1}/{len(new_issues)}] {issue_id} | Error: {e}")

        total_duration = sum(r["duration_ms"] for r in results)
        mlflow.log_metrics({
            "success_count": success_count,
            "fail_count": fail_count,
            "success_rate": success_count / len(new_issues) if new_issues else 0,
            "total_duration_ms": total_duration,
            "avg_duration_ms": total_duration / len(results) if results else 0,
            "critical_issues": sum(1 for r in results if r["severity"] == "CRITICAL"),
            "high_issues": sum(1 for r in results if r["severity"] == "HIGH"),
        })

        print(f"\n{'='*60}")
        print(f"  RESULTS: {success_count} succeeded, {fail_count} failed")
        print(f"  Total time: {total_duration/1000:.1f}s | Avg: {total_duration/len(results)/1000:.1f}s/issue")
        print(f"  MLflow Run: {run.info.run_id}")
        print(f"{'='*60}")

# COMMAND ----------

# ============================================================
# PREVIEW EXPLANATIONS
# ============================================================

if results:
    for r in results:
        print("=" * 70)
        print(f"  [{r['severity']}] {r['issue_id']} | {r['table_name']}.{r['rule_name']}")
        print(f"  Rule: {r['rule_name']} | Status: {r['status']} | Time: {r['duration_ms']}ms")
        print("=" * 70)
        print(r["ai_explanation"])
        print()
else:
    print("No new results to preview.")

# COMMAND ----------

# ============================================================
# SAVE TO GOLD — Incremental MERGE (upsert) + orphan cleanup
# ============================================================
# Three operations:
#   1. INSERT new explanations (issue exists in Silver, not in Gold)
#   2. UPDATE changed explanations (issue exists in both, hash differs)
#   3. DELETE orphans (issue exists in Gold but was removed from Silver)
# This keeps Gold in sync with Silver on every run.
# ============================================================

table_exists = spark.catalog.tableExists(OUTPUT_TABLE)

# Step 1 & 2: MERGE new and updated results into Gold
if results:
    new_df = spark.createDataFrame(results)

    if not table_exists:
        new_df.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)
        print(f"Created {OUTPUT_TABLE} with {len(results)} rows")
        table_exists = True
    else:
        new_df.createOrReplaceTempView("new_explanations")
        spark.sql(f"""
            MERGE INTO {OUTPUT_TABLE} AS target
            USING new_explanations AS source
            ON target.issue_id = source.issue_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"Merged {len(results)} rows into {OUTPUT_TABLE}")
else:
    print("No new or updated results to merge.")

# Step 3: DELETE orphans — Gold rows whose source issue no longer exists in Silver
# Build a list of all current issue_ids from Silver (not just new ones)
if table_exists:
    current_issue_ids = [f"DQ-{all_issues.index(issue)+1:04d}" for issue in all_issues]
    current_ids_df = spark.createDataFrame([{"issue_id": iid} for iid in current_issue_ids])
    current_ids_df.createOrReplaceTempView("current_source_ids")

    orphan_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {OUTPUT_TABLE}
        WHERE issue_id NOT IN (SELECT issue_id FROM current_source_ids)
    """).collect()[0]["cnt"]

    if orphan_count > 0:
        spark.sql(f"""
            DELETE FROM {OUTPUT_TABLE}
            WHERE issue_id NOT IN (SELECT issue_id FROM current_source_ids)
        """)
        print(f"Deleted {orphan_count} orphan rows from Gold (source issues no longer exist)")
    else:
        print("No orphan rows to delete. Gold is in sync with Silver.")

    total = spark.sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT_TABLE}").collect()[0]["cnt"]
    print(f"Total rows in {OUTPUT_TABLE}: {total}")

# COMMAND ----------

# ============================================================
# VERIFY FINAL OUTPUT
# ============================================================

display(spark.sql(f"""
    SELECT
        issue_id,
        severity,
        table_name,
        rule_name,
        status,
        duration_ms,
        prompt_version,
        LEFT(what_was_found, 120) as finding_preview,
        LEFT(how_to_prevent, 120) as prevention_preview
    FROM {OUTPUT_TABLE}
    ORDER BY
        CASE severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            WHEN 'LOW' THEN 4
        END,
        issue_id
"""))

# COMMAND ----------

