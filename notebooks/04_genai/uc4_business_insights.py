# Databricks notebook source
# MAGIC %md
# MAGIC # UC4: AI Business Insights
# MAGIC Reads from gold tables, computes KPIs as aggregations, generates
# MAGIC executive summaries for 3 domains: policy portfolio, claims performance,
# MAGIC customer profile. Never passes raw rows to the LLM, only aggregated stats.
# MAGIC
# MAGIC Also demonstrates ai_query() for SQL-native LLM calls.
# MAGIC
# MAGIC Output: prime_insurance_jellsinki_poc.gold.ai_business_insights

# COMMAND ----------

# MAGIC %pip install openai
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

from openai import OpenAI
import json
from datetime import datetime

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)
MODEL = "databricks-gpt-oss-20b"
print(f"Model: {MODEL}")

def parse_llm_response(response):
    content = response.choices[0].message.content
    # Handle case where content is already a list/dict (not a string)
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text", ""))
        return str(content)
    if not isinstance(content, str):
        return str(content)
    try:
        parsed = json.loads(content)
        if isinstance(parsed, list):
            for block in parsed:
                if isinstance(block, dict) and block.get("type") == "text":
                    return str(block.get("text", content))
        return str(parsed) if not isinstance(parsed, str) else parsed
    except (json.JSONDecodeError, TypeError):
        return content

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Compute KPIs per domain (aggregations only, no raw rows)

# COMMAND ----------

# Domain 1: Policy Portfolio KPIs
policy_kpis = spark.sql("""
    SELECT
        COUNT(*) AS total_policies,
        COUNT(DISTINCT policy_state) AS states_covered,
        ROUND(AVG(policy_annual_premium), 2) AS avg_premium,
        ROUND(MIN(policy_annual_premium), 2) AS min_premium,
        ROUND(MAX(policy_annual_premium), 2) AS max_premium,
        ROUND(AVG(policy_deductible), 0) AS avg_deductible,
        ROUND(AVG(umbrella_limit), 0) AS avg_umbrella,
        COUNT(CASE WHEN umbrella_limit > 0 THEN 1 END) AS policies_with_umbrella,
        COUNT(CASE WHEN policy_annual_premium >= 2000 THEN 1 END) AS premium_tier,
        COUNT(CASE WHEN policy_annual_premium >= 1000 AND policy_annual_premium < 2000 THEN 1 END) AS standard_tier,
        COUNT(CASE WHEN policy_annual_premium < 1000 THEN 1 END) AS basic_tier
    FROM prime_insurance_jellsinki_poc.gold.dim_policies
""").collect()[0].asDict()

# Top CSL tiers
csl_dist = spark.sql("""
    SELECT policy_csl, COUNT(*) AS cnt,
           ROUND(AVG(policy_annual_premium), 2) AS avg_premium
    FROM prime_insurance_jellsinki_poc.gold.dim_policies
    GROUP BY policy_csl ORDER BY cnt DESC LIMIT 5
""").collect()
policy_kpis["top_csl_tiers"] = [r.asDict() for r in csl_dist]

print("Policy KPIs:")
print(json.dumps(policy_kpis, indent=2, default=str))

# COMMAND ----------

# Domain 2: Claims Performance KPIs
claims_kpis = spark.sql("""
    SELECT
        COUNT(*) AS total_claims,
        ROUND(AVG(total_claim_amount), 2) AS avg_claim_amount,
        ROUND(SUM(total_claim_amount), 2) AS total_claims_value,
        ROUND(AVG(days_to_process), 1) AS avg_processing_days,
        MAX(days_to_process) AS max_processing_days,
        COUNT(CASE WHEN is_rejected THEN 1 END) AS rejected_claims,
        ROUND(COUNT(CASE WHEN is_rejected THEN 1 END) * 100.0 / COUNT(*), 1) AS rejection_rate_pct,
        COUNT(CASE WHEN days_to_process > 7 THEN 1 END) AS claims_over_7_days,
        ROUND(COUNT(CASE WHEN days_to_process > 7 THEN 1 END) * 100.0 / NULLIF(COUNT(days_to_process), 0), 1) AS pct_over_benchmark
    FROM prime_insurance_jellsinki_poc.gold.fact_claims
""").collect()[0].asDict()

# Claims by severity
severity_dist = spark.sql("""
    SELECT incident_severity, COUNT(*) AS cnt,
           ROUND(AVG(total_claim_amount), 2) AS avg_amount,
           ROUND(AVG(days_to_process), 1) AS avg_days
    FROM prime_insurance_jellsinki_poc.gold.fact_claims
    GROUP BY incident_severity ORDER BY cnt DESC
""").collect()
claims_kpis["by_severity"] = [r.asDict() for r in severity_dist]

# Claims by region
region_dist = spark.sql("""
    SELECT dc.region, COUNT(*) AS cnt,
           ROUND(AVG(fc.total_claim_amount), 2) AS avg_amount,
           ROUND(AVG(fc.days_to_process), 1) AS avg_days
    FROM prime_insurance_jellsinki_poc.gold.fact_claims fc
    JOIN prime_insurance_jellsinki_poc.gold.dim_customers dc ON fc.customer_key = dc.customer_key
    GROUP BY dc.region ORDER BY cnt DESC
""").collect()
claims_kpis["by_region"] = [r.asDict() for r in region_dist]

print("Claims KPIs:")
print(json.dumps(claims_kpis, indent=2, default=str))

# COMMAND ----------

# Domain 3: Customer Profile KPIs
customer_kpis = spark.sql("""
    SELECT
        COUNT(*) AS total_customers,
        COUNT(DISTINCT region) AS regions,
        COUNT(CASE WHEN default_flag = 1 THEN 1 END) AS customers_in_default,
        ROUND(COUNT(CASE WHEN default_flag = 1 THEN 1 END) * 100.0 / COUNT(*), 1) AS default_rate_pct,
        ROUND(AVG(balance), 2) AS avg_balance,
        COUNT(CASE WHEN hh_insurance = 1 THEN 1 END) AS with_hh_insurance,
        COUNT(CASE WHEN car_loan = 1 THEN 1 END) AS with_car_loan
    FROM prime_insurance_jellsinki_poc.gold.dim_customers
""").collect()[0].asDict()

# By region
cust_region = spark.sql("""
    SELECT region, COUNT(*) AS cnt,
           ROUND(AVG(balance), 2) AS avg_balance,
           ROUND(COUNT(CASE WHEN default_flag = 1 THEN 1 END) * 100.0 / COUNT(*), 1) AS default_rate
    FROM prime_insurance_jellsinki_poc.gold.dim_customers
    GROUP BY region ORDER BY cnt DESC
""").collect()
customer_kpis["by_region"] = [r.asDict() for r in cust_region]

# Education distribution
edu_dist = spark.sql("""
    SELECT education, COUNT(*) AS cnt
    FROM prime_insurance_jellsinki_poc.gold.dim_customers
    WHERE education IS NOT NULL
    GROUP BY education ORDER BY cnt DESC
""").collect()
customer_kpis["education_distribution"] = [r.asDict() for r in edu_dist]

print("Customer KPIs:")
print(json.dumps(customer_kpis, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate executive summaries

# COMMAND ----------

domains = [
    {
        "domain": "policy_portfolio",
        "title": "Policy portfolio overview",
        "kpis": policy_kpis,
        "prompt_context": """You are a business analyst at PrimeInsurance writing an executive summary
for the leadership team. Based on the KPIs below, write a 3-4 paragraph summary covering:
- Portfolio size and composition (tier breakdown, CSL distribution)
- Premium analysis (average, range, what's driving it)
- Risk observations (umbrella coverage gaps, deductible patterns)
- One actionable recommendation

Be specific with numbers. No generic statements. Under 250 words."""
    },
    {
        "domain": "claims_performance",
        "title": "Claims performance and backlog analysis",
        "kpis": claims_kpis,
        "prompt_context": """You are a business analyst at PrimeInsurance writing an executive summary
on claims performance. The company benchmark is 7 days for processing. Based on the KPIs:
- Overall claims volume and financial exposure
- Processing time vs the 7-day benchmark (how bad is the backlog?)
- Rejection patterns (rate, any severity or regional correlation?)
- One actionable recommendation to reduce processing time

Be specific with numbers. Under 250 words."""
    },
    {
        "domain": "customer_profile",
        "title": "Customer base analysis post-deduplication",
        "kpis": customer_kpis,
        "prompt_context": """You are a business analyst at PrimeInsurance writing an executive summary
on the customer base. This is the first accurate count after deduplicating 7 regional systems.
Based on the KPIs:
- Actual customer count vs what was previously reported (12% inflation)
- Regional distribution and balance patterns
- Default risk exposure
- Cross-sell opportunity (who doesn't have household insurance or car loans?)

Be specific with numbers. Under 250 words."""
    },
]

results = []

for d in domains:
    print(f"\nGenerating: {d['title']}...")

    kpi_json = json.dumps(d["kpis"], indent=2, default=str)

    prompt = f"""{d['prompt_context']}

KPI Data:
{kpi_json}"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.4
        )
        summary = parse_llm_response(response)
    except Exception as e:
        summary = f"Generation failed: {str(e)}"

    results.append({
        "domain": d["domain"],
        "title": d["title"],
        "executive_summary": summary,
        "kpi_data": kpi_json,
        "model_name": MODEL,
        "generated_at": datetime.now(),
        "status": "SUCCESS" if "failed" not in str(summary).lower() else "FAILED",
    })

    print(f"  Done ({len(summary)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write to gold.ai_business_insights

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("domain", StringType(), False),
    StructField("title", StringType(), False),
    StructField("executive_summary", StringType(), True),
    StructField("kpi_data", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("generated_at", TimestampType(), False),
    StructField("status", StringType(), False),
])

df = spark.createDataFrame(results, schema)
df.write.mode("overwrite").saveAsTable(
    "prime_insurance_jellsinki_poc.gold.ai_business_insights"
)
print("Written to prime_insurance_jellsinki_poc.gold.ai_business_insights")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify output

# COMMAND ----------

# MAGIC %sql
SELECT domain, title, executive_summary, status, generated_at
FROM prime_insurance_jellsinki_poc.gold.ai_business_insights
ORDER BY domain;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: ai_query() demonstrations
# MAGIC SQL-native LLM calls using Databricks ai_query() function.
# MAGIC The model returns structured JSON, so we extract the text block.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ai_query() example 1: Summarize claims by region in natural language
# MAGIC SELECT
# MAGIC   region_summary,
# MAGIC   get_json_object(
# MAGIC     get_json_object(
# MAGIC       ai_query(
# MAGIC         'databricks-gpt-oss-20b',
# MAGIC         CONCAT('Write a one-sentence executive summary of these claims stats: ', region_summary)
# MAGIC       ),
# MAGIC       '$[1]'
# MAGIC     ),
# MAGIC     '$.text'
# MAGIC   ) AS ai_summary
# MAGIC FROM (
# MAGIC   SELECT CONCAT(
# MAGIC     dc.region, ': ', COUNT(*), ' claims, $', ROUND(AVG(fc.total_claim_amount), 0),
# MAGIC     ' avg, ', ROUND(AVG(fc.days_to_process), 1), ' days avg processing'
# MAGIC   ) AS region_summary
# MAGIC   FROM prime_insurance_jellsinki_poc.gold.fact_claims fc
# MAGIC   JOIN prime_insurance_jellsinki_poc.gold.dim_customers dc ON fc.customer_key = dc.customer_key
# MAGIC   GROUP BY dc.region
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ai_query() example 2: Generate risk assessment for top policy tiers
# MAGIC SELECT
# MAGIC   policy_stats,
# MAGIC   get_json_object(
# MAGIC     get_json_object(
# MAGIC       ai_query(
# MAGIC         'databricks-gpt-oss-20b',
# MAGIC         CONCAT('As an insurance analyst, write a one-sentence risk assessment: ', policy_stats)
# MAGIC       ),
# MAGIC       '$[1]'
# MAGIC     ),
# MAGIC     '$.text'
# MAGIC   ) AS ai_risk_assessment
# MAGIC FROM (
# MAGIC   SELECT CONCAT(
# MAGIC     dp.policy_csl, ' tier: ', COUNT(*), ' policies, $', ROUND(AVG(dp.policy_annual_premium), 0),
# MAGIC     ' avg premium, ', ROUND(COUNT(CASE WHEN fc.is_rejected THEN 1 END) * 100.0 / COUNT(*), 1),
# MAGIC     '% rejection rate'
# MAGIC   ) AS policy_stats
# MAGIC   FROM prime_insurance_jellsinki_poc.gold.fact_claims fc
# MAGIC   JOIN prime_insurance_jellsinki_poc.gold.dim_policies dp ON fc.policy_key = dp.policy_key
# MAGIC   GROUP BY dp.policy_csl
# MAGIC );
