# Databricks notebook source
# MAGIC %md
# MAGIC # PrimeInsurance Executive Dashboard
# MAGIC Interactive dashboard covering all three business problems:
# MAGIC 1. **Customer Identity Crisis** — Dedup stats, regional distribution
# MAGIC 2. **Claims Processing Delay** — SLA breaches, processing times, anomalies
# MAGIC 3. **Revenue Leakage** — Inventory aging, unsold car alerts

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

CATALOG = CONFIG["catalog"]
print(f"Dashboard using catalog: {CATALOG}")

def safe_display(query, label=""):
    """Run query and display results. Skip gracefully if table doesn't exist."""
    try:
        df = spark.sql(query)
        display(df)
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"⏳ {label or 'Table'} not available yet — run the full pipeline first")
        else:
            print(f"⚠️ {label}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Regulatory Readiness — Overall Health Score

# COMMAND ----------

safe_display(f"""
  SELECT * FROM `{CATALOG}`.gold.regulatory_readiness
""", "regulatory_readiness")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customer Identity Resolution

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Customer Dedup Summary

# COMMAND ----------

safe_display(f"""
  SELECT
    COUNT(*) as total_master_customers,
    SUM(regional_id_count) as total_regional_ids,
    SUM(CASE WHEN regional_id_count > 1 THEN 1 ELSE 0 END) as duplicated_customers,
    ROUND(SUM(CASE WHEN regional_id_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as dedup_pct
  FROM `{CATALOG}`.gold.regulatory_customer_registry
""", "regulatory_customer_registry")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Customer Distribution by Region

# COMMAND ----------

safe_display(f"""
  SELECT region, COUNT(*) as customer_count
  FROM `{CATALOG}`.gold.dim_customer
  GROUP BY region
  ORDER BY customer_count DESC
""", "dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Top Duplicated Customers (Most Regional IDs)

# COMMAND ----------

safe_display(f"""
  SELECT master_customer_id, regional_id_count, original_ids, source_files
  FROM `{CATALOG}`.gold.regulatory_customer_registry
  WHERE regional_id_count > 1
  ORDER BY regional_id_count DESC
  LIMIT 20
""", "regulatory_customer_registry")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Claims Performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Claims Overview KPIs

# COMMAND ----------

safe_display(f"""
  SELECT
    COUNT(*) as total_claims,
    ROUND(AVG(total_claim_amount), 2) as avg_claim_amount,
    ROUND(AVG(processing_days), 1) as avg_processing_days,
    SUM(CASE WHEN sla_breach = true THEN 1 ELSE 0 END) as sla_breaches,
    ROUND(SUM(CASE WHEN sla_breach = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as sla_breach_pct,
    SUM(CASE WHEN claim_rejected = 'Y' THEN 1 ELSE 0 END) as rejected_claims,
    ROUND(SUM(CASE WHEN claim_rejected = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as rejection_rate_pct
  FROM `{CATALOG}`.gold.fact_claims
""", "fact_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Claims SLA Monitor — By Region

# COMMAND ----------

safe_display(f"""
  SELECT
    incident_state as region, total_claims, avg_processing_days,
    sla_breaches, sla_breach_pct, rejection_rate_pct, avg_claim_amount
  FROM `{CATALOG}`.gold.claims_sla_monitor
  ORDER BY sla_breach_pct DESC
""", "claims_sla_monitor")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Claims by Severity

# COMMAND ----------

safe_display(f"""
  SELECT
    incident_severity,
    COUNT(*) as claim_count,
    ROUND(AVG(total_claim_amount), 2) as avg_amount,
    ROUND(AVG(processing_days), 1) as avg_processing_days,
    SUM(CASE WHEN sla_breach = true THEN 1 ELSE 0 END) as sla_breaches
  FROM `{CATALOG}`.gold.fact_claims
  GROUP BY incident_severity
  ORDER BY claim_count DESC
""", "fact_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3d. Claims by Policy Type (CSL)

# COMMAND ----------

safe_display(f"""
  SELECT
    dp.policy_csl,
    COUNT(*) as total_claims,
    ROUND(AVG(fc.total_claim_amount), 2) as avg_claim_amount,
    SUM(CASE WHEN fc.claim_rejected = 'Y' THEN 1 ELSE 0 END) as rejected,
    ROUND(SUM(CASE WHEN fc.claim_rejected = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as rejection_rate
  FROM `{CATALOG}`.gold.fact_claims fc
  JOIN `{CATALOG}`.gold.dim_policy dp ON fc.policy_sk = dp.policy_sk
  GROUP BY dp.policy_csl
  ORDER BY total_claims DESC
""", "fact_claims + dim_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3e. Processing Days Distribution

# COMMAND ----------

safe_display(f"""
  SELECT
    CASE
      WHEN processing_days <= 3 THEN '0-3 days'
      WHEN processing_days <= 7 THEN '4-7 days (within SLA)'
      WHEN processing_days <= 14 THEN '8-14 days (SLA breach)'
      WHEN processing_days <= 30 THEN '15-30 days (critical)'
      ELSE '30+ days (severe)'
    END as processing_bucket,
    COUNT(*) as claim_count,
    ROUND(AVG(total_claim_amount), 2) as avg_amount
  FROM `{CATALOG}`.gold.fact_claims
  WHERE processing_days IS NOT NULL
  GROUP BY
    CASE
      WHEN processing_days <= 3 THEN '0-3 days'
      WHEN processing_days <= 7 THEN '4-7 days (within SLA)'
      WHEN processing_days <= 14 THEN '8-14 days (SLA breach)'
      WHEN processing_days <= 30 THEN '15-30 days (critical)'
      ELSE '30+ days (severe)'
    END
  ORDER BY processing_bucket
""", "fact_claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3f. Top Anomalous Claims (Fraud Signals)

# COMMAND ----------

safe_display(f"""
  SELECT
    claim_id, anomaly_score, priority, triggered_rules,
    total_claim_amount, incident_severity,
    COALESCE(what_is_suspicious, '') as what_is_suspicious
  FROM `{CATALOG}`.gold.claim_anomaly_explanations
  WHERE priority IN ('HIGH', 'MEDIUM')
  ORDER BY anomaly_score DESC
  LIMIT 25
""", "claim_anomaly_explanations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Revenue & Inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Sales Overview

# COMMAND ----------

safe_display(f"""
  SELECT
    COUNT(*) as total_listings,
    SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END) as sold,
    SUM(CASE WHEN is_sold = false THEN 1 ELSE 0 END) as unsold,
    ROUND(SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as sell_through_pct,
    ROUND(AVG(original_selling_price), 2) as avg_price,
    ROUND(AVG(CASE WHEN is_sold = true THEN days_on_lot END), 1) as avg_days_to_sell
  FROM `{CATALOG}`.gold.fact_sales
""", "fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Sales by Region

# COMMAND ----------

safe_display(f"""
  SELECT
    region,
    COUNT(*) as listings,
    SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END) as sold,
    ROUND(SUM(CASE WHEN is_sold = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as sell_through_pct,
    ROUND(AVG(original_selling_price), 2) as avg_price,
    ROUND(AVG(CASE WHEN is_sold = true THEN days_on_lot END), 1) as avg_days_to_sell
  FROM `{CATALOG}`.gold.fact_sales
  GROUP BY region
  ORDER BY sell_through_pct DESC
""", "fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Inventory Aging Alerts

# COMMAND ----------

safe_display(f"""
  SELECT *
  FROM `{CATALOG}`.gold.inventory_aging_alerts
  ORDER BY days_on_lot DESC
  LIMIT 30
""", "inventory_aging_alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4d. Premium Revenue by Policy Type

# COMMAND ----------

safe_display(f"""
  SELECT
    policy_csl,
    COUNT(*) as policy_count,
    ROUND(SUM(policy_annual_premium), 2) as total_premium,
    ROUND(AVG(policy_annual_premium), 2) as avg_premium,
    ROUND(AVG(umbrella_limit), 2) as avg_umbrella_limit
  FROM `{CATALOG}`.gold.dim_policy
  GROUP BY policy_csl
  ORDER BY total_premium DESC
""", "dim_policy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Scorecard

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. DQ Issues Summary by Entity

# COMMAND ----------

safe_display(f"""
  SELECT
    entity,
    COUNT(*) as issue_count,
    SUM(records_affected) as total_records_affected,
    ROUND(AVG(affected_ratio) * 100, 2) as avg_affected_pct,
    SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_severity,
    SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) as medium_severity,
    SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) as low_severity
  FROM `{CATALOG}`.silver.dq_issues
  GROUP BY entity
  ORDER BY avg_affected_pct DESC
""", "dq_issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. DQ Issues Detail

# COMMAND ----------

safe_display(f"""
  SELECT
    entity, rule_name, severity,
    records_affected, total_records,
    ROUND(affected_ratio * 100, 2) as affected_pct,
    suggested_fix
  FROM `{CATALOG}`.silver.dq_issues
  ORDER BY affected_ratio DESC
""", "dq_issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. AI-Generated DQ Explanations (Sample)

# COMMAND ----------

safe_display(f"""
  SELECT
    entity, rule_name, severity,
    what_was_found, why_it_matters, how_to_prevent
  FROM `{CATALOG}`.gold.dq_explanation_report
  ORDER BY severity DESC
  LIMIT 10
""", "dq_explanation_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. AI Insights

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a. Executive Business Insights

# COMMAND ----------

safe_display(f"""
  SELECT *
  FROM `{CATALOG}`.gold.ai_business_insights
""", "ai_business_insights")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. RAG Query History

# COMMAND ----------

safe_display(f"""
  SELECT *
  FROM `{CATALOG}`.gold.rag_query_history
  LIMIT 10
""", "rag_query_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Dashboard Summary
# MAGIC | Section | Business Problem | Key Tables |
# MAGIC |---------|-----------------|------------|
# MAGIC | Customer Identity | ~12% inflated count across 6 regions | `dim_customer`, `regulatory_customer_registry` |
# MAGIC | Claims Performance | 18-day avg processing vs 7-day SLA | `fact_claims`, `claims_sla_monitor`, `claim_anomaly_explanations` |
# MAGIC | Revenue & Inventory | Unsold cars aging without visibility | `fact_sales`, `inventory_aging_alerts` |
# MAGIC | Data Quality | Pipeline health monitoring | `dq_issues`, `dq_explanation_report` |
# MAGIC | AI Insights | Executive briefings + policy Q&A | `ai_business_insights`, `rag_query_history` |
