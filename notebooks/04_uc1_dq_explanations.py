# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # UC1: Data Quality Explanation Report
# MAGIC
# MAGIC **Problem**: Silver layer catches quality issues but outputs technical fields
# MAGIC like `rule_name`, `affected_ratio`, `suggested_fix`. The compliance team
# MAGIC can't interpret these - they need plain English.
# MAGIC
# MAGIC **Solution**: Read each DQ issue, send to LLM with business context,
# MAGIC generate a structured explanation a compliance officer can act on.
# MAGIC
# MAGIC **Input**: ``databricks-hackathon-insurance`.silver.dq_issues`
# MAGIC **Output**: ``databricks-hackathon-insurance`.gold.dq_explanation_report`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup LLM Connection

# COMMAND ----------

from openai import OpenAI

# databricks foundation model endpoint
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_URL}/serving-endpoints"
)

MODEL = "databricks-meta-llama-3-1-70b-instruct"

# COMMAND ----------

# quick test to make sure the connection works
test_response = client.chat.completions.create(
    model=MODEL,
    messages=[{"role": "user", "content": "Say 'connection working' in 3 words or less"}],
    max_tokens=20,
)
print(test_response.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load DQ Issues

# COMMAND ----------

dq_issues = spark.table("`databricks-hackathon-insurance`.silver.dq_issues").toPandas()
print(f"total DQ issues to explain: {len(dq_issues)}")
display(spark.table("`databricks-hackathon-insurance`.silver.dq_issues"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt Design
# MAGIC
# MAGIC Went through a few iterations to get the output right.

# COMMAND ----------

# === PROMPT V1 (first attempt) ===
# just sent the raw fields to the model. output was generic paragraphs,
# not structured enough for compliance use. no business impact.

prompt_v1 = """Explain this data quality issue: {rule_name} affecting {records_affected} records in {entity}."""

# tried it - got stuff like "This issue means some records have problems."
# not useful at all for a compliance officer.

# COMMAND ----------

# === PROMPT V2 (added structure + context) ===
# told the model about PrimeInsurance and asked for JSON output.
# better but still missed business impact and prioritization.

prompt_v2 = """You are a data quality analyst at PrimeInsurance, an auto insurance company.
Explain this issue in plain English and return JSON:
Entity: {entity}
Rule: {rule_name}
Records affected: {records_affected} out of {total_records}
Severity: {severity}

Return JSON with fields: explanation, business_impact, recommended_action"""

# this was better - got structured output most of the time
# but the explanations were still too technical

# COMMAND ----------

# === PROMPT V3 (final - added persona, examples, strict format) ===
# added few-shot example, compliance officer persona, and priority field.
# this is what we're using in production.

def build_prompt(row):
    return f"""You are a senior data quality analyst at PrimeInsurance, an auto insurance company
that recently merged 6 regional systems. You are writing explanations for the compliance team
who need to understand data issues in plain, non-technical language.

Here is a data quality issue that was detected by our automated pipeline:

Entity: {row['entity']}
Quality Rule: {row['rule_name']}
Records Affected: {row['records_affected']} out of {row['total_records']} total ({row['affected_ratio']*100:.1f}%)
Severity: {row['severity']}
Suggested Fix: {row['suggested_fix']}

Write a structured explanation. Return ONLY valid JSON with these exact fields:
{{
  "plain_english_explanation": "What happened in simple terms a non-technical person understands",
  "business_impact": "How this affects PrimeInsurance's operations, compliance, or revenue",
  "recommended_action": "Specific steps to fix this issue",
  "priority": "P1 (fix immediately) or P2 (fix this week) or P3 (fix this quarter)",
  "estimated_effort": "Quick fix (hours) or Medium (days) or Complex (weeks)"
}}

Example for a similar issue:
{{
  "plain_english_explanation": "47 customer records from the East region are missing their unique identifier. Without an ID, these customers are invisible in our regulatory count and cannot be linked to their policies.",
  "business_impact": "These ghost records could cause us to undercount policyholders in the next regulatory filing. If flagged during audit, this creates a compliance finding.",
  "recommended_action": "Cross-reference these 47 records against the East region legacy system using name and address matching to recover their IDs. Escalate any unresolvable records to the regional office.",
  "priority": "P1",
  "estimated_effort": "Medium"
}}

Now generate the explanation for the issue above. Return ONLY the JSON object, nothing else."""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Explanations

# COMMAND ----------

def call_llm(prompt, retries=3):
    """call the LLM with retry logic for JSON parsing"""
    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
                temperature=0.3,  # lower temp for more consistent output
            )
            content = response.choices[0].message.content.strip()

            # sometimes model wraps json in markdown code blocks
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            parsed = json.loads(content)
            return parsed

        except json.JSONDecodeError:
            if attempt < retries - 1:
                print(f"  json parse failed on attempt {attempt+1}, retrying...")
                continue
            else:
                return {
                    "plain_english_explanation": content,
                    "business_impact": "Unable to parse structured response",
                    "recommended_action": "Manual review needed",
                    "priority": "P2",
                    "estimated_effort": "Unknown",
                }
        except Exception as e:
            print(f"  LLM call failed: {e}")
            if attempt < retries - 1:
                continue
            return {
                "plain_english_explanation": f"Error generating explanation: {str(e)}",
                "business_impact": "N/A",
                "recommended_action": "Retry or review manually",
                "priority": "P3",
                "estimated_effort": "Unknown",
            }

# COMMAND ----------

# generate explanations for each DQ issue
results = []

for idx, row in dq_issues.iterrows():
    print(f"processing {idx+1}/{len(dq_issues)}: {row['entity']}.{row['rule_name']}...")

    prompt = build_prompt(row)
    explanation = call_llm(prompt)

    result = {
        "issue_id": f"DQ-{idx+1:04d}",
        "entity": row["entity"],
        "rule_name": row["rule_name"],
        "records_affected": int(row["records_affected"]),
        "total_records": int(row["total_records"]),
        "affected_ratio": float(row["affected_ratio"]),
        "severity": row["severity"],
        "plain_english_explanation": explanation.get("plain_english_explanation", ""),
        "business_impact": explanation.get("business_impact", ""),
        "recommended_action": explanation.get("recommended_action", ""),
        "priority": explanation.get("priority", "P3"),
        "estimated_effort": explanation.get("estimated_effort", "Unknown"),
        "model_name": MODEL,
        "generated_at": datetime.now().isoformat(),
    }
    results.append(result)

print(f"\ngenerated {len(results)} explanations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

report_df = spark.createDataFrame(results)

(report_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.dq_explanation_report"))

print(f"gold.dq_explanation_report: {report_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Output

# COMMAND ----------

display(spark.table("`databricks-hackathon-insurance`.gold.dq_explanation_report"))

# COMMAND ----------

# show a few nicely formatted
for row in results[:3]:
    print(f"\n{'='*60}")
    print(f"Issue: {row['issue_id']} | {row['entity']}.{row['rule_name']}")
    print(f"Severity: {row['severity']} | Priority: {row['priority']}")
    print(f"Affected: {row['records_affected']}/{row['total_records']} ({row['affected_ratio']*100:.1f}%)")
    print(f"\nExplanation: {row['plain_english_explanation']}")
    print(f"\nBusiness Impact: {row['business_impact']}")
    print(f"\nAction: {row['recommended_action']}")
    print(f"Effort: {row['estimated_effort']}")
