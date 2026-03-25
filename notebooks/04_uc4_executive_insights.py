# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # UC4: Executive Business Insights
# MAGIC
# MAGIC **Problem**: Executives get dashboards with numbers but no narrative.
# MAGIC "Ohio has 312 claims averaging $28,400" doesn't tell them if that's alarming,
# MAGIC expected, or an opportunity. Turning numbers into narrative requires an analyst.
# MAGIC
# MAGIC **Solution**: Aggregate KPIs from all Gold tables (never raw rows),
# MAGIC send summary statistics to LLM, generate executive briefings for 3 domains.
# MAGIC Also demonstrates `ai_query()` — Databricks SQL-native LLM function.
# MAGIC
# MAGIC **Key principle**: Never pass raw rows to the LLM. Aggregate first
# MAGIC (COUNT, AVG, SUM, distribution by key), then pass the summary statistics.
# MAGIC
# MAGIC **Input**: All Gold tables (aggregated KPIs only)
# MAGIC **Output**: `databricks-hackathon-insurance`.gold.ai_business_insights

# COMMAND ----------

%pip install openai mlflow tenacity pydantic --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ============================================================
# CONFIGURATION & SETUP
# ============================================================

import mlflow
import json
import re
from datetime import datetime
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_random_exponential
from pydantic import BaseModel, Field, field_validator
from pyspark.sql import functions as F

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
MAX_TOKENS = 1500
OUTPUT_TABLE = "`databricks-hackathon-insurance`.gold.ai_business_insights"
PROMPT_VERSION = "v2"

# ── Response parser for databricks-gpt-oss-20b ──
def extract_text(raw_response):
    """Extracts text block from gpt-oss-20b structured response."""
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
    """Calls LLM and returns parsed dict."""
    response = client.chat.completions.create(
        model=MODEL_NAME, messages=messages, max_tokens=max_tokens,
    )
    raw = response.choices[0].message.content
    text = extract_text(raw)
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    if first_brace != -1 and last_brace > first_brace:
        return json.loads(text[first_brace:last_brace + 1])
    return json.loads(text)

# ── Guardrails ──
def apply_guardrails(text):
    """Redacts PII patterns from LLM output."""
    text = re.sub(r'\b[\w.-]+@[\w.-]+\.\w+\b', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[REDACTED_PHONE]', text)
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    return text

print(f"Connected to {WORKSPACE_URL}")
print(f"Model: {MODEL_NAME}")

# COMMAND ----------

# ============================================================
# PYDANTIC OUTPUT MODEL — Executive Summary
# ============================================================

class ExecutiveSummary(BaseModel):
    """Validated executive summary for a business domain."""

    headline: str = Field(min_length=20, description="One sentence capturing the most important takeaway")
    key_findings: str = Field(min_length=20, description="3-4 bullet points separated by |")
    alerts: str = Field(min_length=10, description="1-2 items needing immediate attention separated by |")
    recommendations: str = Field(min_length=20, description="2-3 actionable recommendations separated by |")
    estimated_impact: str = Field(min_length=5, description="Estimated dollar or percentage impact")

    @field_validator('*', mode='before')
    @classmethod
    def clean_input(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('n/a', 'none', 'null', '', '-'):
                return "[Not provided by model]"
        if isinstance(v, list):
            return " | ".join(str(item) for item in v)
        return v

print("Pydantic model ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gather KPIs from Gold Layer
# MAGIC
# MAGIC **Key principle**: Never pass raw rows to the LLM.
# MAGIC Aggregate first (COUNT, AVG, SUM, distribution by key),
# MAGIC then pass the summary statistics.

# COMMAND ----------

# ============================================================
# CLAIMS PERFORMANCE KPIs
# ============================================================

claims_kpis = spark.sql("""
    SELECT
        COUNT(*) as total_claims,
        ROUND(AVG(total_claim_amount), 2) as avg_claim_amount,
        ROUND(SUM(total_claim_amount), 2) as total_claim_value,
        ROUND(AVG(processing_days), 1) as avg_processing_days,
        SUM(CASE WHEN processing_days > 7 THEN 1 ELSE 0 END) as sla_breaches,
        ROUND(SUM(CASE WHEN processing_days > 7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sla_breach_pct,
        SUM(CASE WHEN claim_rejected = 'Y' THEN 1 ELSE 0 END) as rejected_claims,
        ROUND(SUM(CASE WHEN claim_rejected = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rejection_rate_pct,
        COUNT(DISTINCT incident_state) as regions_with_claims
    FROM `databricks-hackathon-insurance`.gold.fact_claims
""").collect()[0]

claims_by_region = spark.sql("""
    SELECT
        incident_state as region,
        COUNT(*) as claims,
        ROUND(AVG(processing_days), 1) as avg_days,
        ROUND(SUM(CASE WHEN claim_rejected = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as reject_pct
    FROM `databricks-hackathon-insurance`.gold.fact_claims
    GROUP BY incident_state
    ORDER BY claims DESC
""").toPandas()

print("=== Claims KPIs ===")
print(f"  Total claims: {claims_kpis['total_claims']}")
print(f"  Avg amount: ${claims_kpis['avg_claim_amount']}")
print(f"  Avg processing: {claims_kpis['avg_processing_days']} days")
print(f"  SLA breaches: {claims_kpis['sla_breach_pct']}%")

# COMMAND ----------

# ============================================================
# CUSTOMER & REGULATORY KPIs
# ============================================================

customer_kpis = spark.sql("""
    SELECT COUNT(*) as unique_customers
    FROM `databricks-hackathon-insurance`.gold.dim_customer
""").collect()[0]

# customer_resolution_audit doesn't exist in this pipeline version
# Compute dedup stats from silver_customers vs dim_customer
_raw_count = spark.sql("SELECT COUNT(*) as cnt FROM `databricks-hackathon-insurance`.silver.silver_customers").collect()[0]["cnt"]
_unique_count = spark.sql("SELECT COUNT(*) as cnt FROM `databricks-hackathon-insurance`.gold.dim_customer").collect()[0]["cnt"]
dedup_stats = {
    "total_raw_records": _raw_count,
    "duplicates_found": _raw_count - _unique_count,
    "unique_identities": _unique_count,
}

customer_by_region = spark.sql("""
    SELECT region, COUNT(*) as customers
    FROM `databricks-hackathon-insurance`.gold.dim_customer
    GROUP BY region ORDER BY customers DESC
""").toPandas()

print("=== Customer KPIs ===")
print(f"  Unique customers: {customer_kpis['unique_customers']}")
print(f"  Duplicates resolved: {dedup_stats['duplicates_found']}")

# COMMAND ----------

# ============================================================
# SALES & INVENTORY KPIs
# ============================================================

sales_kpis = spark.sql("""
    SELECT
        COUNT(*) as total_listings,
        SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold_count,
        SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold_count,
        ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_rate,
        ROUND(AVG(CASE WHEN is_sold THEN days_to_sell END), 1) as avg_days_to_sell,
        ROUND(AVG(selling_price), 2) as avg_price,
        SUM(CASE WHEN days_since_listing > 60 THEN 1 ELSE 0 END) as aging_inventory
    FROM `databricks-hackathon-insurance`.gold.fact_sales
""").collect()[0]

print("=== Sales KPIs ===")
print(f"  Sell-through rate: {sales_kpis['sell_through_rate']}%")
print(f"  Avg days to sell: {sales_kpis['avg_days_to_sell']}")
print(f"  Aging inventory: {sales_kpis['aging_inventory']} units")

# COMMAND ----------

# ============================================================
# POLICY KPIs
# ============================================================

policy_kpis = spark.sql("""
    SELECT
        COUNT(*) as total_policies,
        ROUND(AVG(policy_annual_premium), 2) as avg_premium,
        ROUND(SUM(policy_annual_premium), 2) as total_premium_revenue,
        COUNT(DISTINCT policy_csl) as csl_types
    FROM `databricks-hackathon-insurance`.gold.dim_policy
""").collect()[0]

print("=== Policy KPIs ===")
print(f"  Total policies: {policy_kpis['total_policies']}")
print(f"  Avg premium: ${policy_kpis['avg_premium']}")

# COMMAND ----------

# ============================================================
# PROMPT & DOMAIN DEFINITIONS
# ============================================================

SYSTEM_PROMPT = """You are a senior data analyst preparing executive briefings for PrimeInsurance leadership. PrimeInsurance acquired 6 regional auto insurance operators and is unifying their data platforms.

You must always respond in json format with exactly these 5 keys:
{
    "headline": "One sentence capturing the most important takeaway for leadership",
    "key_findings": ["3-4 bullet points of the most important findings using specific numbers"],
    "alerts": ["1-2 items needing IMMEDIATE attention with specific numbers"],
    "recommendations": ["2-3 actionable recommendations tied to specific KPIs"],
    "estimated_impact": "Estimated dollar or percentage impact if recommendations are followed"
}

<example>
KPIs: Total Claims: 500, Avg Processing: 12 days (benchmark: 7), Rejection Rate: 28%

Response:
{{
    "headline": "Claims processing is 71% slower than industry benchmark with a 28% rejection rate, costing PrimeInsurance an estimated $2.1M in operational overhead annually.",
    "key_findings": ["Average processing time of 12 days is 71% above the 7-day industry benchmark", "28% rejection rate suggests systemic issues in claims validation or policy matching", "500 total claims processed across all regions with significant variance in regional performance"],
    "alerts": ["Processing backlog: 12-day average means customer escalations are likely increasing — check NPS scores", "28% rejection rate needs immediate root cause analysis — could indicate policy data mismatch from regional system merger"],
    "recommendations": ["Implement automated claims triage to route simple claims (<$5K, single vehicle) to fast-track processing", "Audit the top 3 rejection reasons and fix the underlying data mapping issues from the regional merger"],
    "estimated_impact": "Reducing processing to 8 days and rejection to 15% could save $1.4M annually in operational costs and reduce customer churn by ~5%"
}}
</example>

<constraints>
- Use ACTUAL numbers from the KPIs — no generic statements
- Focus on what an executive needs to decide or act on TODAY
- Compare to industry benchmarks where relevant
- Be specific about dollar amounts and percentages
- Do NOT invent numbers not present in the KPIs
</constraints>"""

domains = [
    {
        "domain": "claims_performance",
        "title": "Claims Operations & Processing",
        "kpi_data": f"""
Total Claims: {claims_kpis['total_claims']}
Average Claim Amount: ${claims_kpis['avg_claim_amount']}
Total Claim Value: ${claims_kpis['total_claim_value']}
Average Processing Time: {claims_kpis['avg_processing_days']} days (industry benchmark: 7 days)
SLA Breaches (>7 days): {claims_kpis['sla_breach_pct']}%
Rejection Rate: {claims_kpis['rejection_rate_pct']}%
Claims by Region:
{claims_by_region.to_string(index=False)}
"""
    },
    {
        "domain": "customer_regulatory",
        "title": "Customer Registry & Regulatory Compliance",
        "kpi_data": f"""
Unique Customers (after deduplication): {customer_kpis['unique_customers']}
Total Raw Records (before dedup): {dedup_stats['total_raw_records']}
Duplicates Identified and Resolved: {dedup_stats['duplicates_found']}
Duplicate Rate: {round(dedup_stats['duplicates_found']/dedup_stats['total_raw_records']*100, 1)}%
Customers by Region:
{customer_by_region.to_string(index=False)}
Regulatory Deadline: 90 days to produce auditable customer registry
"""
    },
    {
        "domain": "inventory_revenue",
        "title": "Vehicle Inventory & Revenue",
        "kpi_data": f"""
Total Listings: {sales_kpis['total_listings']}
Sold: {sales_kpis['sold_count']} | Unsold: {sales_kpis['unsold_count']}
Sell-Through Rate: {sales_kpis['sell_through_rate']}%
Average Days to Sell: {sales_kpis['avg_days_to_sell']} days
Average Selling Price: ${sales_kpis['avg_price']}
Aging Inventory (>60 days unsold): {sales_kpis['aging_inventory']} units
Total Policies: {policy_kpis['total_policies']}
Average Annual Premium: ${policy_kpis['avg_premium']}
Total Premium Revenue: ${policy_kpis['total_premium_revenue']}
"""
    },
]

print(f"Defined {len(domains)} business domains")

# COMMAND ----------

# ============================================================
# GENERATE EXECUTIVE SUMMARIES — with MLflow tracking
# ============================================================

mlflow.set_experiment("/Users/abhinav.sarkar@jellyfishtechnologies.com/primeinsurance-hackathon/executive_insights")

results = []

with mlflow.start_run(run_name=f"exec_insights_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:

    mlflow.set_tags({
        "model": MODEL_NAME,
        "prompt_version": PROMPT_VERSION,
        "pipeline": "executive_insights",
    })
    mlflow.log_params({
        "num_domains": len(domains),
        "model": MODEL_NAME,
        "prompt_version": PROMPT_VERSION,
    })

    success_count = 0
    fail_count = 0

    for idx, domain in enumerate(domains):
        issue_start = datetime.now()
        print(f"Generating summary for: {domain['title']}...")

        try:
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Generate an executive intelligence summary for: {domain['title']}\n\nKPIs:\n{domain['kpi_data']}"},
            ]

            data = call_llm(messages)
            summary = ExecutiveSummary(**data)
            duration_ms = int((datetime.now() - issue_start).total_seconds() * 1000)

            results.append({
                "domain": domain["domain"],
                "title": domain["title"],
                "headline": apply_guardrails(summary.headline),
                "key_findings": apply_guardrails(summary.key_findings),
                "alerts": apply_guardrails(summary.alerts),
                "recommendations": apply_guardrails(summary.recommendations),
                "estimated_impact": summary.estimated_impact,
                "kpi_data_snapshot": domain["kpi_data"],
                "model_name": MODEL_NAME,
                "prompt_version": PROMPT_VERSION,
                "generated_at": datetime.now().isoformat(),
                "generation_status": "SUCCESS",
                "duration_ms": duration_ms,
            })

            success_count += 1
            print(f"  [{idx+1}/{len(domains)}] {domain['title']} — {duration_ms}ms")

        except Exception as e:
            fail_count += 1
            duration_ms = int((datetime.now() - issue_start).total_seconds() * 1000)

            results.append({
                "domain": domain["domain"],
                "title": domain["title"],
                "headline": f"Generation failed: {str(e)}",
                "key_findings": "",
                "alerts": "",
                "recommendations": "",
                "estimated_impact": "",
                "kpi_data_snapshot": domain["kpi_data"],
                "model_name": MODEL_NAME,
                "prompt_version": PROMPT_VERSION,
                "generated_at": datetime.now().isoformat(),
                "generation_status": "FAILED",
                "duration_ms": duration_ms,
            })
            print(f"  [{idx+1}/{len(domains)}] {domain['title']} — FAILED: {e}")

    total_duration = sum(r["duration_ms"] for r in results)
    mlflow.log_metrics({
        "success_count": success_count,
        "fail_count": fail_count,
        "total_duration_ms": total_duration,
    })

    print(f"\n{'='*60}")
    print(f"  RESULTS: {success_count} succeeded, {fail_count} failed")
    print(f"  Total time: {total_duration/1000:.1f}s")
    print(f"  MLflow Run: {run.info.run_id}")
    print(f"{'='*60}")

# COMMAND ----------

# ============================================================
# PREVIEW EXECUTIVE SUMMARIES
# ============================================================

for r in results:
    print(f"\n{'='*60}")
    print(f"DOMAIN: {r['title']}")
    print(f"Status: {r['generation_status']} | Time: {r['duration_ms']}ms")
    print(f"{'='*60}")
    print(f"\nHEADLINE: {r['headline']}")
    print(f"\nKEY FINDINGS:\n  {r['key_findings']}")
    print(f"\nALERTS:\n  {r['alerts']}")
    print(f"\nRECOMMENDATIONS:\n  {r['recommendations']}")
    print(f"\nESTIMATED IMPACT: {r['estimated_impact']}")

# COMMAND ----------

# ============================================================
# WRITE TO GOLD TABLE
# ============================================================

insights_df = spark.createDataFrame(results)

(insights_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.ai_business_insights"))

print(f"gold.ai_business_insights: {insights_df.count()} rows")

# COMMAND ----------

display(spark.table("`databricks-hackathon-insurance`.gold.ai_business_insights"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ai_query() Demonstrations
# MAGIC
# MAGIC `ai_query()` is Databricks' SQL-native function for calling Foundation Models
# MAGIC directly inside a SQL query. No Python needed — pure SQL.
# MAGIC
# MAGIC **Important**: `databricks-gpt-oss-20b` returns structured JSON, so we use
# MAGIC `get_json_object()` to extract the text block.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- ai_query() Example 1: Claims Summary by Severity
# MAGIC -- Aggregates claims data and asks the LLM to interpret it
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC   incident_severity,
# MAGIC   claim_count,
# MAGIC   avg_amount,
# MAGIC   avg_processing_days,
# MAGIC   get_json_object(
# MAGIC     get_json_object(
# MAGIC       ai_query(
# MAGIC         'databricks-gpt-oss-20b',
# MAGIC         CONCAT(
# MAGIC           'You are an insurance analyst. In one sentence, interpret this claims data: ',
# MAGIC           incident_severity, ' severity: ',
# MAGIC           claim_count, ' claims, avg amount $', ROUND(avg_amount, 0),
# MAGIC           ', avg processing ', ROUND(avg_processing_days, 1), ' days. ',
# MAGIC           'Industry benchmark for processing is 7 days. Is this concerning?'
# MAGIC         )
# MAGIC       ),
# MAGIC       '$[1]'
# MAGIC     ),
# MAGIC     '$.text'
# MAGIC   ) AS ai_interpretation
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     incident_severity,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     AVG(total_claim_amount) as avg_amount,
# MAGIC     AVG(processing_days) as avg_processing_days
# MAGIC   FROM `databricks-hackathon-insurance`.gold.fact_claims
# MAGIC   GROUP BY incident_severity
# MAGIC   ORDER BY claim_count DESC
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- ai_query() Example 2: Regional Inventory Health Check
# MAGIC -- Asks the LLM to assess inventory risk per region
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC   region,
# MAGIC   total_listings,
# MAGIC   unsold_count,
# MAGIC   sell_through_pct,
# MAGIC   get_json_object(
# MAGIC     get_json_object(
# MAGIC       ai_query(
# MAGIC         'databricks-gpt-oss-20b',
# MAGIC         CONCAT(
# MAGIC           'You are an inventory analyst. In one sentence, assess the inventory health: ',
# MAGIC           'Region: ', region, '. ',
# MAGIC           total_listings, ' total listings, ',
# MAGIC           unsold_count, ' unsold (',
# MAGIC           ROUND(100 - sell_through_pct, 1), '% unsold). ',
# MAGIC           'Is this region at risk of revenue leakage from aging inventory?'
# MAGIC         )
# MAGIC       ),
# MAGIC       '$[1]'
# MAGIC     ),
# MAGIC     '$.text'
# MAGIC   ) AS ai_assessment
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     region,
# MAGIC     COUNT(*) as total_listings,
# MAGIC     SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold_count,
# MAGIC     ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct
# MAGIC   FROM `databricks-hackathon-insurance`.gold.fact_sales
# MAGIC   GROUP BY region
# MAGIC   ORDER BY unsold_count DESC
# MAGIC )
