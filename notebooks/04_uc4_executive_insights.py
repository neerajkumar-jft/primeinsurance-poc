# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # UC4: Executive Business Insights (Bonus)
# MAGIC
# MAGIC **Problem**: Executives get dashboards with numbers but no narrative.
# MAGIC They don't need raw KPIs - they need synthesized intelligence with
# MAGIC context, trends, and actionable recommendations.
# MAGIC
# MAGIC **Solution**: Aggregate KPIs from all Gold tables, send to LLM,
# MAGIC generate executive briefing summaries by business domain.
# MAGIC
# MAGIC **Input**: All Gold tables
# MAGIC **Output**: ``databricks-hackathon-insurance`.gold.ai_business_insights`

# COMMAND ----------

from pyspark.sql import functions as F
import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gather KPIs from Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims Performance KPIs

# COMMAND ----------

claims_kpis = spark.sql("""
    SELECT
        COUNT(*) as total_claims,
        ROUND(AVG(total_claim_amount), 2) as avg_claim_amount,
        ROUND(SUM(total_claim_amount), 2) as total_claim_value,
        ROUND(AVG(processing_days), 1) as avg_processing_days,
        SUM(CASE WHEN sla_breach THEN 1 ELSE 0 END) as sla_breaches,
        ROUND(SUM(CASE WHEN sla_breach THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sla_breach_pct,
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

# MAGIC %md
# MAGIC ### Customer & Regulatory KPIs

# COMMAND ----------

customer_kpis = spark.sql("""
    SELECT
        COUNT(*) as unique_customers
    FROM `databricks-hackathon-insurance`.gold.dim_customer
""").collect()[0]

# get dedup stats from resolution audit
dedup_stats = spark.sql("""
    SELECT
        COUNT(*) as total_raw_records,
        SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END) as duplicates_found,
        COUNT(DISTINCT master_customer_id) as unique_identities
    FROM `databricks-hackathon-insurance`.silver.customer_resolution_audit
""").collect()[0]

customer_by_region = spark.sql("""
    SELECT region, COUNT(*) as customers
    FROM `databricks-hackathon-insurance`.gold.dim_customer
    GROUP BY region ORDER BY customers DESC
""").toPandas()

print("=== Customer KPIs ===")
print(f"  Unique customers: {customer_kpis['unique_customers']}")
print(f"  Duplicates resolved: {dedup_stats['duplicates_found']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales & Inventory KPIs

# COMMAND ----------

sales_kpis = spark.sql("""
    SELECT
        COUNT(*) as total_listings,
        SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold_count,
        SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold_count,
        ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_rate,
        ROUND(AVG(CASE WHEN is_sold THEN days_on_lot END), 1) as avg_days_to_sell,
        ROUND(AVG(original_selling_price), 2) as avg_price,
        SUM(CASE WHEN aging_flag IN ('AGING', 'CRITICAL') THEN 1 ELSE 0 END) as aging_inventory
    FROM `databricks-hackathon-insurance`.gold.fact_sales
""").collect()[0]

print("=== Sales KPIs ===")
print(f"  Sell-through rate: {sales_kpis['sell_through_rate']}%")
print(f"  Avg days to sell: {sales_kpis['avg_days_to_sell']}")
print(f"  Aging inventory: {sales_kpis['aging_inventory']} units")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Policy KPIs

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Generate Executive Summaries

# COMMAND ----------

from openai import OpenAI

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_URL}/serving-endpoints"
)

MODEL = "databricks-meta-llama-3-1-70b-instruct"

# COMMAND ----------

# build domain-specific prompts with the actual KPIs
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

# COMMAND ----------

def generate_executive_summary(domain_info):
    prompt = f"""You are a senior data analyst preparing an executive briefing for PrimeInsurance leadership.
Generate an intelligence summary for the domain: {domain_info['title']}

Here are the current KPIs from our data platform:
{domain_info['kpi_data']}

Write an executive summary. Return ONLY valid JSON:
{{
  "headline": "One sentence capturing the most important takeaway",
  "key_findings": ["3-4 bullet points of the most important findings from the data"],
  "alerts": ["1-2 items that need immediate attention, with specific numbers"],
  "recommendations": ["2-3 actionable recommendations tied to specific KPIs"],
  "estimated_impact": "Estimated dollar or percentage impact if recommendations are followed"
}}

Be specific - use actual numbers from the KPIs. No generic statements.
Focus on what an executive needs to decide or act on TODAY."""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.3,
        )
        content = response.choices[0].message.content.strip()

        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        return json.loads(content)
    except Exception as e:
        return {
            "headline": f"Error generating summary: {str(e)[:100]}",
            "key_findings": [],
            "alerts": [],
            "recommendations": [],
            "estimated_impact": "N/A",
        }

# COMMAND ----------

# generate for each domain
results = []
for domain in domains:
    print(f"generating summary for: {domain['title']}...")
    summary = generate_executive_summary(domain)

    results.append({
        "domain": domain["domain"],
        "title": domain["title"],
        "headline": summary.get("headline", ""),
        "key_findings": json.dumps(summary.get("key_findings", [])),
        "alerts": json.dumps(summary.get("alerts", [])),
        "recommendations": json.dumps(summary.get("recommendations", [])),
        "estimated_impact": summary.get("estimated_impact", ""),
        "kpi_data_snapshot": domain["kpi_data"],
        "model_name": MODEL,
        "generated_at": datetime.now().isoformat(),
    })

print(f"\ngenerated {len(results)} executive summaries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

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

# pretty print the briefs
for r in results:
    print(f"\n{'='*60}")
    print(f"DOMAIN: {r['title']}")
    print(f"HEADLINE: {r['headline']}")
    print(f"\nKEY FINDINGS:")
    for f in json.loads(r['key_findings']):
        print(f"  - {f}")
    print(f"\nALERTS:")
    for a in json.loads(r['alerts']):
        print(f"  ! {a}")
    print(f"\nRECOMMENDATIONS:")
    for rec in json.loads(r['recommendations']):
        print(f"  > {rec}")
    print(f"\nESTIMATED IMPACT: {r['estimated_impact']}")
