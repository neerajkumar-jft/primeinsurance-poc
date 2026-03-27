# Databricks notebook source
# MAGIC %md
# MAGIC # PrimeInsurance Executive Dashboard
# MAGIC
# MAGIC All queries use hardcoded `primeins` catalog for easy copy-paste into
# MAGIC Databricks SQL Dashboard widgets.
# MAGIC
# MAGIC **Sections:**
# MAGIC 1. Executive overview (KPI counters)
# MAGIC 2. Claims performance (rejection rate, severity, regional breakdown)
# MAGIC 3. Customer registry (deduplication, regional distribution)
# MAGIC 4. Inventory & revenue (unsold cars, aging, sell-through)
# MAGIC 5. Data quality (DQ issues, AI explanations)
# MAGIC 6. Fraud detection (anomaly scores, flagged claims)
# MAGIC 7. Policy portfolio

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Executive overview

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI counters

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(DISTINCT customer_id) FROM primeins.gold.dim_customer) as unique_customers,
# MAGIC   (SELECT COUNT(*) FROM primeins.gold.dim_policy) as active_policies,
# MAGIC   (SELECT COUNT(*) FROM primeins.gold.fact_claims) as total_claims,
# MAGIC   (SELECT ROUND(SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) FROM primeins.gold.fact_claims) as rejection_rate_pct,
# MAGIC   (SELECT COUNT(*) FROM primeins.gold.fact_sales) as total_listings,
# MAGIC   (SELECT ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) FROM primeins.gold.fact_sales) as sell_through_pct,
# MAGIC   (SELECT SUM(CASE WHEN NOT is_sold AND days_listed > 60 THEN 1 ELSE 0 END) FROM primeins.gold.fact_sales) as aging_inventory,
# MAGIC   (SELECT COUNT(*) FROM primeins.gold.claim_anomaly_explanations WHERE priority = 'HIGH') as high_risk_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Claims performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rejection rate by policy coverage tier

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   policy_csl as coverage_tier,
# MAGIC   total_claims,
# MAGIC   rejected_claims,
# MAGIC   ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
# MAGIC   ROUND(avg_claim_amount, 2) as avg_claim_amount
# MAGIC FROM primeins.gold.mv_rejection_rate_by_policy
# MAGIC ORDER BY rejection_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims by incident severity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   incident_severity,
# MAGIC   total_claims,
# MAGIC   rejected_claims,
# MAGIC   ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
# MAGIC   ROUND(avg_claim_amount, 2) as avg_claim_amount,
# MAGIC   ROUND(total_claim_value, 2) as total_claim_value
# MAGIC FROM primeins.gold.mv_claims_by_severity
# MAGIC ORDER BY total_claims DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   total_claims,
# MAGIC   rejected_claims,
# MAGIC   ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
# MAGIC   ROUND(avg_claim_amount, 2) as avg_claim_amount,
# MAGIC   ROUND(total_claim_value, 2) as total_claim_value
# MAGIC FROM primeins.gold.mv_claims_by_region
# MAGIC ORDER BY total_claims DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 highest value claims

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   claim_id,
# MAGIC   policy_number,
# MAGIC   incident_severity,
# MAGIC   incident_type,
# MAGIC   ROUND(total_claim_amount, 2) as total_amount,
# MAGIC   CASE WHEN is_rejected THEN 'Rejected' ELSE 'Approved' END as status,
# MAGIC   incident_state,
# MAGIC   incident_city
# MAGIC FROM primeins.gold.fact_claims
# MAGIC ORDER BY total_claim_amount DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Customer registry

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer deduplication summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.customers) as raw_records,
# MAGIC   (SELECT COUNT(*) FROM primeins.gold.dim_customer) as unique_customers,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.customers) -
# MAGIC     (SELECT COUNT(*) FROM primeins.gold.dim_customer) as duplicates_resolved,
# MAGIC   ROUND(
# MAGIC     ((SELECT COUNT(*) FROM primeins.silver.customers) -
# MAGIC      (SELECT COUNT(*) FROM primeins.gold.dim_customer)) * 100.0 /
# MAGIC     (SELECT COUNT(*) FROM primeins.silver.customers), 1
# MAGIC   ) as dedup_rate_pct

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM primeins.gold.dim_customer), 1) as pct_of_total
# MAGIC FROM primeins.gold.dim_customer
# MAGIC GROUP BY region
# MAGIC ORDER BY customer_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer profile distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   education,
# MAGIC   COUNT(*) as count
# MAGIC FROM primeins.gold.dim_customer
# MAGIC WHERE education IS NOT NULL
# MAGIC GROUP BY education
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inventory and revenue

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales overview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_listings,
# MAGIC   SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold,
# MAGIC   SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold,
# MAGIC   ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
# MAGIC   ROUND(AVG(CASE WHEN is_sold THEN days_listed END), 1) as avg_days_to_sell,
# MAGIC   ROUND(AVG(original_selling_price), 0) as avg_price,
# MAGIC   SUM(CASE WHEN NOT is_sold AND days_listed > 60 THEN 1 ELSE 0 END) as aging_over_60_days,
# MAGIC   SUM(CASE WHEN NOT is_sold AND days_listed > 90 THEN 1 ELSE 0 END) as aging_over_90_days
# MAGIC FROM primeins.gold.fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unsold inventory by model and region (top 15)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   model,
# MAGIC   region,
# MAGIC   unsold_count,
# MAGIC   avg_days_listed,
# MAGIC   max_days_listed,
# MAGIC   ROUND(avg_price, 0) as avg_price
# MAGIC FROM primeins.gold.mv_unsold_inventory
# MAGIC ORDER BY unsold_count DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales by region

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   COUNT(*) as total_listings,
# MAGIC   SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold,
# MAGIC   SUM(CASE WHEN NOT is_sold THEN 1 ELSE 0 END) as unsold,
# MAGIC   ROUND(SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as sell_through_pct,
# MAGIC   ROUND(SUM(CASE WHEN is_sold THEN original_selling_price ELSE 0 END), 0) as total_revenue
# MAGIC FROM primeins.gold.fact_sales
# MAGIC GROUP BY region
# MAGIC ORDER BY total_listings DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data quality

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pipeline quality summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.dq_issues) as total_issues,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.dq_issues WHERE severity = 'critical') as critical_issues,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.dq_issues WHERE severity = 'medium') as medium_issues,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.quarantine_customers) as quarantined_customers,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.quarantine_claims) as quarantined_claims,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.quarantine_policy) as quarantined_policies,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.quarantine_sales) as quarantined_sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ issues with AI explanations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.table_name,
# MAGIC   d.rule_name,
# MAGIC   d.severity,
# MAGIC   d.affected_records,
# MAGIC   ROUND(d.affected_ratio * 100, 1) as affected_pct,
# MAGIC   e.what_was_found
# MAGIC FROM primeins.silver.dq_issues d
# MAGIC LEFT JOIN primeins.gold.dq_explanation_report e
# MAGIC   ON d.table_name = e.table_name AND d.rule_name = e.rule_name
# MAGIC ORDER BY
# MAGIC   CASE d.severity
# MAGIC     WHEN 'critical' THEN 1
# MAGIC     WHEN 'high' THEN 2
# MAGIC     WHEN 'medium' THEN 3
# MAGIC     WHEN 'low' THEN 4
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fraud detection

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anomaly detection summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   priority,
# MAGIC   COUNT(*) as flagged_claims,
# MAGIC   ROUND(AVG(anomaly_score), 1) as avg_score,
# MAGIC   ROUND(AVG(total_claim_amount), 2) as avg_claim_amount,
# MAGIC   SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as briefs_generated,
# MAGIC   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as briefs_failed
# MAGIC FROM primeins.gold.claim_anomaly_explanations
# MAGIC GROUP BY priority
# MAGIC ORDER BY priority

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 highest risk claims

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   claim_id,
# MAGIC   anomaly_score,
# MAGIC   priority,
# MAGIC   ROUND(total_claim_amount, 2) as total_amount,
# MAGIC   incident_severity,
# MAGIC   incident_type,
# MAGIC   triggered_rules,
# MAGIC   LEFT(what_is_suspicious, 200) as suspicious_summary
# MAGIC FROM primeins.gold.claim_anomaly_explanations
# MAGIC ORDER BY anomaly_score DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anomaly score distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN anomaly_score >= 80 THEN '80-100'
# MAGIC     WHEN anomaly_score >= 60 THEN '60-79'
# MAGIC     WHEN anomaly_score >= 40 THEN '40-59'
# MAGIC     WHEN anomaly_score >= 20 THEN '20-39'
# MAGIC     ELSE '0-19'
# MAGIC   END as score_range,
# MAGIC   COUNT(*) as claim_count
# MAGIC FROM primeins.gold.claim_anomaly_explanations
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Policy portfolio

# COMMAND ----------

# MAGIC %md
# MAGIC ### Policy distribution by coverage tier

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   policy_csl as coverage_tier,
# MAGIC   COUNT(*) as policy_count,
# MAGIC   ROUND(AVG(policy_annual_premium), 2) as avg_premium,
# MAGIC   ROUND(SUM(policy_annual_premium), 2) as total_premium_revenue,
# MAGIC   ROUND(AVG(policy_deductible), 0) as avg_deductible,
# MAGIC   SUM(CASE WHEN umbrella_limit > 0 THEN 1 ELSE 0 END) as with_umbrella
# MAGIC FROM primeins.gold.dim_policy
# MAGIC GROUP BY policy_csl
# MAGIC ORDER BY policy_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Premium revenue by state (top 10)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   policy_state as state,
# MAGIC   COUNT(*) as policies,
# MAGIC   ROUND(SUM(policy_annual_premium), 2) as total_premium,
# MAGIC   ROUND(AVG(policy_annual_premium), 2) as avg_premium
# MAGIC FROM primeins.gold.dim_policy
# MAGIC GROUP BY policy_state
# MAGIC ORDER BY total_premium DESC
# MAGIC LIMIT 10

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
