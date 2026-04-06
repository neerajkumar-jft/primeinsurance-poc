# Databricks notebook source
# MAGIC %md
# MAGIC # Claims SLA Monitoring
# MAGIC
# MAGIC The operations team checks this every morning. PrimeInsurance's average
# MAGIC claim processing time is 18 days vs a 7-day industry benchmark.
# MAGIC
# MAGIC This report shows:
# MAGIC - Claims volume and rejection patterns by region and severity
# MAGIC - High-value rejected claims that need attention
# MAGIC - Rejection rate trends across policy coverage tiers
# MAGIC - Flagged anomalous claims from the fraud detection engine
# MAGIC
# MAGIC Note: claim date fields are corrupted at source (time-only values).
# MAGIC Processing time in days cannot be calculated. We focus on rejection
# MAGIC patterns and claim volumes instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Overall claims health

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) as rejected,
# MAGIC   SUM(CASE WHEN NOT is_rejected THEN 1 ELSE 0 END) as approved,
# MAGIC   ROUND(SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rejection_rate_pct,
# MAGIC   ROUND(SUM(total_claim_amount), 2) as total_claim_value,
# MAGIC   ROUND(AVG(total_claim_amount), 2) as avg_claim_amount,
# MAGIC   ROUND(MIN(total_claim_amount), 2) as min_claim,
# MAGIC   ROUND(MAX(total_claim_amount), 2) as max_claim
# MAGIC FROM primeins.gold.fact_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rejection rate by region
# MAGIC Which regions have the worst rejection rates?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   total_claims,
# MAGIC   rejected_claims,
# MAGIC   ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
# MAGIC   ROUND(avg_claim_amount, 2) as avg_claim_amount,
# MAGIC   ROUND(total_claim_value, 2) as total_value
# MAGIC FROM primeins.gold.mv_claims_by_region
# MAGIC ORDER BY rejection_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rejection rate by severity
# MAGIC Are certain severity levels rejected more than others?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   incident_severity,
# MAGIC   total_claims,
# MAGIC   rejected_claims,
# MAGIC   ROUND(rejection_rate_pct, 1) as rejection_rate_pct,
# MAGIC   ROUND(avg_claim_amount, 2) as avg_claim_amount,
# MAGIC   ROUND(total_claim_value, 2) as total_value
# MAGIC FROM primeins.gold.mv_claims_by_severity
# MAGIC ORDER BY rejection_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Rejection rate by policy coverage tier
# MAGIC Do higher coverage tiers have more rejections?

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
# MAGIC ## 5. Rejection rate by incident type
# MAGIC Which incident types generate the most rejections?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   incident_type,
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) as rejected,
# MAGIC   ROUND(SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rejection_rate_pct,
# MAGIC   ROUND(AVG(total_claim_amount), 2) as avg_amount
# MAGIC FROM primeins.gold.fact_claims
# MAGIC GROUP BY incident_type
# MAGIC ORDER BY rejection_rate_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. High-value rejected claims
# MAGIC Claims above $30K that were rejected — these need immediate review.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   claim_id,
# MAGIC   policy_number,
# MAGIC   incident_severity,
# MAGIC   incident_type,
# MAGIC   ROUND(total_claim_amount, 2) as total_amount,
# MAGIC   ROUND(injury_amount, 2) as injury,
# MAGIC   ROUND(property_amount, 2) as property,
# MAGIC   ROUND(vehicle_amount, 2) as vehicle,
# MAGIC   incident_state,
# MAGIC   incident_city
# MAGIC FROM primeins.gold.fact_claims
# MAGIC WHERE is_rejected = true AND total_claim_amount > 30000
# MAGIC ORDER BY total_claim_amount DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Claims with no police report
# MAGIC Above-median claims without a police report — a fraud risk indicator.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   claim_id,
# MAGIC   ROUND(total_claim_amount, 2) as amount,
# MAGIC   incident_severity,
# MAGIC   incident_type,
# MAGIC   witnesses,
# MAGIC   bodily_injuries,
# MAGIC   CASE WHEN is_rejected THEN 'Rejected' ELSE 'Approved' END as status
# MAGIC FROM primeins.gold.fact_claims
# MAGIC WHERE police_report_available = 'NO'
# MAGIC   AND total_claim_amount > (SELECT AVG(total_claim_amount) FROM primeins.gold.fact_claims)
# MAGIC ORDER BY total_claim_amount DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Anomaly detection — flagged claims summary
# MAGIC Claims flagged by the statistical fraud detection engine.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   priority,
# MAGIC   COUNT(*) as flagged_claims,
# MAGIC   ROUND(AVG(anomaly_score), 1) as avg_score,
# MAGIC   ROUND(AVG(total_claim_amount), 2) as avg_amount,
# MAGIC   ROUND(SUM(total_claim_amount), 2) as total_value
# MAGIC FROM primeins.gold.claim_anomaly_explanations
# MAGIC GROUP BY priority
# MAGIC ORDER BY priority

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Top 10 highest risk claims with investigation briefs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   claim_id,
# MAGIC   anomaly_score,
# MAGIC   priority,
# MAGIC   ROUND(total_claim_amount, 2) as amount,
# MAGIC   incident_severity,
# MAGIC   triggered_rules,
# MAGIC   LEFT(what_is_suspicious, 250) as suspicious_summary
# MAGIC FROM primeins.gold.claim_anomaly_explanations
# MAGIC WHERE priority = 'HIGH'
# MAGIC ORDER BY anomaly_score DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Regional claims comparison
# MAGIC Side-by-side comparison of all regions for operations review.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.region,
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) as rejected,
# MAGIC   ROUND(SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rejection_rate_pct,
# MAGIC   ROUND(AVG(fc.total_claim_amount), 2) as avg_amount,
# MAGIC   ROUND(SUM(fc.total_claim_amount), 2) as total_value,
# MAGIC   COUNT(DISTINCT fc.policy_number) as unique_policies
# MAGIC FROM primeins.gold.fact_claims fc
# MAGIC JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
# MAGIC JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
# MAGIC GROUP BY dc.region
# MAGIC ORDER BY total_claims DESC
