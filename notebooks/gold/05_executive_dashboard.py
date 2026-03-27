# Databricks notebook source
# MAGIC %md
# MAGIC # PrimeInsurance Executive Dashboard
# MAGIC
# MAGIC This notebook provides all dashboard queries for the Gold layer.
# MAGIC Use these to create a Databricks SQL Dashboard or run directly
# MAGIC for a quick view of all metrics.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Executive overview (KPI counters)
# MAGIC 2. Claims performance (rejection rate, severity, regional breakdown)
# MAGIC 3. Customer registry (deduplication, regional distribution)
# MAGIC 4. Inventory & revenue (unsold cars, aging, sell-through)
# MAGIC 5. Data quality (DQ issues, AI explanations)
# MAGIC 6. Fraud detection (anomaly scores, flagged claims)

# COMMAND ----------

dbutils.widgets.text("catalog", "primeins")
CATALOG = dbutils.widgets.get("catalog")
spark.sql(f"USE CATALOG `{CATALOG}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Executive overview

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI counters

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT COUNT(DISTINCT customer_id) FROM `{CATALOG}`.gold.dim_customer) as unique_customers,
  (SELECT COUNT(*) FROM `{CATALOG}`.gold.dim_policy) as active_policies,
  (SELECT COUNT(*) FROM `{CATALOG}`.gold.fact_claims) as total_claims,
  (SELECT ROUND(SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) FROM `{CATALOG}`.gold.fact_claims) as rejection_rate_pct,
  (SELECT COUNT(*) FROM `{CATALOG}`.gold.fact_sales) as total_listings,
  (SELECT ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) FROM `{CATALOG}`.gold.fact_sales) as sell_through_pct,
  (SELECT SUM(CASE WHEN NOT is_sold AND days_listed > 60 THEN 1 ELSE 0 END) FROM `{CATALOG}`.gold.fact_sales) as aging_inventory,
  (SELECT COUNT(*) FROM `{CATALOG}`.gold.claim_anomaly_explanations WHERE priority = 'HIGH') as high_risk_claims
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Claims performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rejection rate by policy coverage tier

# COMMAND ----------

display(spark.sql(f"""
SELECT
  policy_csl as coverage_tier,
  total_claims,
  rejected_claims,
  ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
  ROUND(avg_claim_amount, 2) as avg_claim_amount
FROM `{CATALOG}`.gold.mv_rejection_rate_by_policy
ORDER BY rejection_rate_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims by incident severity

# COMMAND ----------

display(spark.sql(f"""
SELECT
  incident_severity,
  total_claims,
  rejected_claims,
  ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
  ROUND(avg_claim_amount, 2) as avg_claim_amount,
  ROUND(total_claim_value, 2) as total_claim_value
FROM `{CATALOG}`.gold.mv_claims_by_severity
ORDER BY total_claims DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims by region

# COMMAND ----------

display(spark.sql(f"""
SELECT
  region,
  total_claims,
  rejected_claims,
  ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
  ROUND(avg_claim_amount, 2) as avg_claim_amount,
  ROUND(total_claim_value, 2) as total_claim_value
FROM `{CATALOG}`.gold.mv_claims_by_region
ORDER BY total_claims DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 highest value claims

# COMMAND ----------

display(spark.sql(f"""
SELECT
  claim_id,
  policy_number,
  incident_severity,
  incident_type,
  ROUND(total_claim_amount, 2) as total_amount,
  CASE WHEN is_rejected THEN 'Rejected' ELSE 'Approved' END as status,
  incident_state,
  incident_city
FROM `{CATALOG}`.gold.fact_claims
ORDER BY total_claim_amount DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Customer registry

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer deduplication summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.customers) as raw_records,
  (SELECT COUNT(*) FROM `{CATALOG}`.gold.dim_customer) as unique_customers,
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.customers) -
    (SELECT COUNT(*) FROM `{CATALOG}`.gold.dim_customer) as duplicates_resolved,
  ROUND(
    ((SELECT COUNT(*) FROM `{CATALOG}`.silver.customers) -
     (SELECT COUNT(*) FROM `{CATALOG}`.gold.dim_customer)) * 100.0 /
    (SELECT COUNT(*) FROM `{CATALOG}`.silver.customers), 1
  ) as dedup_rate_pct
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers by region

# COMMAND ----------

display(spark.sql(f"""
SELECT
  region,
  COUNT(*) as customer_count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `{CATALOG}`.gold.dim_customer), 1) as pct_of_total
FROM `{CATALOG}`.gold.dim_customer
GROUP BY region
ORDER BY customer_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer profile distribution

# COMMAND ----------

display(spark.sql(f"""
SELECT
  education,
  COUNT(*) as count
FROM `{CATALOG}`.gold.dim_customer
WHERE education IS NOT NULL
GROUP BY education
ORDER BY count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inventory and revenue

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales overview

# COMMAND ----------

display(spark.sql(f"""
SELECT
  COUNT(*) as total_listings,
  SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold,
  SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold,
  ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
  ROUND(AVG(CASE WHEN is_sold THEN days_listed END), 1) as avg_days_to_sell,
  ROUND(AVG(original_selling_price), 0) as avg_price,
  SUM(CASE WHEN NOT is_sold AND days_listed > 60 THEN 1 ELSE 0 END) as aging_over_60_days,
  SUM(CASE WHEN NOT is_sold AND days_listed > 90 THEN 1 ELSE 0 END) as aging_over_90_days
FROM `{CATALOG}`.gold.fact_sales
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unsold inventory by model and region (top 15)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  model,
  region,
  unsold_count,
  avg_days_listed,
  max_days_listed,
  ROUND(avg_price, 0) as avg_price
FROM `{CATALOG}`.gold.mv_unsold_inventory
ORDER BY unsold_count DESC
LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales by region

# COMMAND ----------

display(spark.sql(f"""
SELECT
  region,
  COUNT(*) as total_listings,
  SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold,
  SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold,
  ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
  ROUND(SUM(CASE WHEN is_sold THEN original_selling_price ELSE 0 END), 0) as total_revenue
FROM `{CATALOG}`.gold.fact_sales
GROUP BY region
ORDER BY total_listings DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data quality

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline quality summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.dq_issues) as total_issues,
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.dq_issues WHERE severity = 'critical') as critical_issues,
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.dq_issues WHERE severity = 'medium') as medium_issues,
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.quarantine_customers) as quarantined_customers,
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.quarantine_claims) as quarantined_claims,
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.quarantine_policy) as quarantined_policies,
  (SELECT COUNT(*) FROM `{CATALOG}`.silver.quarantine_sales) as quarantined_sales
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ issues with AI explanations

# COMMAND ----------

display(spark.sql(f"""
SELECT
  d.table_name,
  d.rule_name,
  d.severity,
  d.affected_records,
  ROUND(d.affected_ratio * 100, 1) as affected_pct,
  e.what_was_found
FROM `{CATALOG}`.silver.dq_issues d
LEFT JOIN `{CATALOG}`.gold.dq_explanation_report e
  ON d.table_name = e.table_name AND d.rule_name = e.rule_name
ORDER BY
  CASE d.severity
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'low' THEN 4
  END
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fraud detection

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anomaly detection summary

# COMMAND ----------

display(spark.sql(f"""
SELECT
  priority,
  COUNT(*) as flagged_claims,
  ROUND(AVG(anomaly_score), 1) as avg_score,
  ROUND(AVG(total_claim_amount), 2) as avg_claim_amount,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as briefs_generated,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as briefs_failed
FROM `{CATALOG}`.gold.claim_anomaly_explanations
GROUP BY priority
ORDER BY priority
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 highest risk claims

# COMMAND ----------

display(spark.sql(f"""
SELECT
  claim_id,
  anomaly_score,
  priority,
  ROUND(total_claim_amount, 2) as total_amount,
  incident_severity,
  incident_type,
  triggered_rules,
  LEFT(what_is_suspicious, 200) as suspicious_summary
FROM `{CATALOG}`.gold.claim_anomaly_explanations
ORDER BY anomaly_score DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anomaly score distribution

# COMMAND ----------

display(spark.sql(f"""
SELECT
  CASE
    WHEN anomaly_score >= 80 THEN '80-100'
    WHEN anomaly_score >= 60 THEN '60-79'
    WHEN anomaly_score >= 40 THEN '40-59'
    WHEN anomaly_score >= 20 THEN '20-39'
    ELSE '0-19'
  END as score_range,
  COUNT(*) as claim_count
FROM `{CATALOG}`.gold.claim_anomaly_explanations
GROUP BY 1
ORDER BY 1 DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Policy portfolio

# COMMAND ----------

# MAGIC %md
# MAGIC ### Policy distribution by coverage tier

# COMMAND ----------

display(spark.sql(f"""
SELECT
  policy_csl as coverage_tier,
  COUNT(*) as policy_count,
  ROUND(AVG(policy_annual_premium), 2) as avg_premium,
  ROUND(SUM(policy_annual_premium), 2) as total_premium_revenue,
  ROUND(AVG(policy_deductible), 0) as avg_deductible,
  SUM(CASE WHEN umbrella_limit > 0 THEN 1 ELSE 0 END) as with_umbrella
FROM `{CATALOG}`.gold.dim_policy
GROUP BY policy_csl
ORDER BY policy_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Premium revenue by state (top 10)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  policy_state as state,
  COUNT(*) as policies,
  ROUND(SUM(policy_annual_premium), 2) as total_premium,
  ROUND(AVG(policy_annual_premium), 2) as avg_premium
FROM `{CATALOG}`.gold.dim_policy
GROUP BY policy_state
ORDER BY total_premium DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### Dashboard summary
# MAGIC
# MAGIC This notebook covers all 3 business failures:
# MAGIC - **Regulatory pressure**: Customer deduplication stats, auditable count by region
# MAGIC - **Claims backlog**: Rejection rates by policy type, severity, and region; fraud detection
# MAGIC - **Revenue leakage**: Unsold inventory aging, sell-through rates, regional distribution
# MAGIC
# MAGIC Plus data quality monitoring and AI-generated insights.
