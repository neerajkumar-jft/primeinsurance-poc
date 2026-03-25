# Databricks notebook source
# MAGIC %md
# MAGIC # UC1: Data Quality Explainer
# MAGIC Reads DQ issues from silver quarantine tables and the quality log,
# MAGIC sends each issue to databricks-gpt-oss-20b to get a plain-English
# MAGIC explanation that compliance can understand, then writes the results
# MAGIC to prime_insurance_jellsinki_poc.gold.dq_explanation_report.

# COMMAND ----------

# MAGIC %pip install openai
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set up the LLM client

# COMMAND ----------

from openai import OpenAI
import json

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

MODEL = "databricks-gpt-oss-20b"
print(f"Connected to {WORKSPACE_URL}, model: {MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read DQ issues from silver quality log and quarantine tables

# COMMAND ----------

# Get the summary counts from silver_quality_log
dq_summary = spark.sql("""
    SELECT entity, quarantine_reason, record_count
    FROM prime_insurance_jellsinki_poc.silver.silver_quality_log
    ORDER BY entity, record_count DESC
""").collect()

# Get sample quarantined records for context
quarantine_samples = {}

# Customers
try:
    quarantine_samples["customers"] = spark.sql("""
        SELECT customer_id, raw_region, city, quarantine_reason, _source_file
        FROM prime_insurance_jellsinki_poc.silver.quarantine_customers
        LIMIT 5
    """).collect()
except:
    quarantine_samples["customers"] = []

# Claims
try:
    quarantine_samples["claims"] = spark.sql("""
        SELECT claim_id, policy_id, raw_incident_date, quarantine_reason, _source_file
        FROM prime_insurance_jellsinki_poc.silver.quarantine_claims
        LIMIT 5
    """).collect()
except:
    quarantine_samples["claims"] = []

# Sales
try:
    quarantine_samples["sales"] = spark.sql("""
        SELECT sales_id, car_id, raw_ad_placed_on, quarantine_reason, _source_file
        FROM prime_insurance_jellsinki_poc.silver.quarantine_sales
        LIMIT 5
    """).collect()
except:
    quarantine_samples["sales"] = []

print(f"DQ summary rows: {len(dq_summary)}")
for entity in quarantine_samples:
    print(f"  {entity} quarantine samples: {len(quarantine_samples[entity])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build prompts and call the LLM for each DQ issue

# COMMAND ----------

def parse_llm_response(response):
    """
    databricks-gpt-oss-20b returns structured JSON:
    [{"type": "reasoning", ...}, {"type": "text", "text": "actual answer"}]
    We extract only the text block.
    """
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


def explain_dq_issue(entity, reason, count, sample_records):
    """
    Ask the LLM to explain a DQ issue in plain English for compliance.
    """
    # Build sample context
    samples_text = ""
    if sample_records:
        samples_text = "\n\nSample affected records:\n"
        for r in sample_records[:3]:
            samples_text += f"  {dict(r.asDict())}\n"

    prompt = f"""You are a data quality analyst at PrimeInsurance, an auto insurance company.
The compliance team needs to understand a data quality issue found during pipeline processing.
Write a short, clear explanation in plain English. No technical jargon. Explain what happened,
why it matters for the business, and what the recommended action is.

Issue details:
- Entity: {entity}
- Problem: {reason}
- Records affected: {count}
{samples_text}

Write 3-4 sentences. Be specific about the business impact."""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300,
        temperature=0.3
    )

    return parse_llm_response(response)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate explanations for each DQ issue

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

results = []

for row in dq_summary:
    entity = row["entity"]
    reason = row["quarantine_reason"]
    count = row["record_count"]

    # Get matching sample records
    samples = [r for r in quarantine_samples.get(entity, [])
               if r["quarantine_reason"] == reason]

    print(f"Explaining: {entity} / {reason} ({count} records)...")

    try:
        explanation = explain_dq_issue(entity, reason, count, samples)
    except Exception as e:
        explanation = f"Unable to generate explanation: {str(e)}"

    results.append({
        "entity": entity,
        "issue": reason,
        "records_affected": count,
        "explanation": explanation,
        "generated_at": datetime.now()
    })

    print(f"  Done: {explanation[:80]}...")

print(f"\nGenerated {len(results)} explanations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write results to gold.dq_explanation_report

# COMMAND ----------

schema = StructType([
    StructField("entity", StringType(), False),
    StructField("issue", StringType(), False),
    StructField("records_affected", IntegerType(), False),
    StructField("explanation", StringType(), True),
    StructField("generated_at", TimestampType(), False)
])

df = spark.createDataFrame(results, schema)

# Write to gold schema
df.write.mode("overwrite").saveAsTable(
    "prime_insurance_jellsinki_poc.gold.dq_explanation_report"
)

print("Written to prime_insurance_jellsinki_poc.gold.dq_explanation_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify the output

# COMMAND ----------

# MAGIC %sql
SELECT entity, issue, records_affected, explanation, generated_at
FROM prime_insurance_jellsinki_poc.gold.dq_explanation_report
ORDER BY entity, records_affected DESC;
