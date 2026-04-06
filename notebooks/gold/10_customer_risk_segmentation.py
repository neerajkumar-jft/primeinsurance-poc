# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Risk Segmentation
# MAGIC
# MAGIC Which customers are high-risk for PrimeInsurance? The business team needs
# MAGIC this for renewal decisions, premium adjustments, and fraud investigation
# MAGIC prioritization.
# MAGIC
# MAGIC This report segments customers by:
# MAGIC - Claim frequency (how many claims they've filed)
# MAGIC - Claim value (total dollar amount across all claims)
# MAGIC - Rejection rate (what percentage of their claims were denied)
# MAGIC - Anomaly flags (whether any of their claims were flagged by fraud detection)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Customer claims summary
# MAGIC Every customer with at least one claim, ranked by total claim value.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.customer_id,
# MAGIC   dc.region,
# MAGIC   dc.state,
# MAGIC   dc.city,
# MAGIC   COUNT(fc.claim_id) as claim_count,
# MAGIC   ROUND(SUM(fc.total_claim_amount), 2) as total_claim_value,
# MAGIC   ROUND(AVG(fc.total_claim_amount), 2) as avg_claim_value,
# MAGIC   SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) as rejected_claims,
# MAGIC   ROUND(SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rejection_rate_pct
# MAGIC FROM primeins.gold.fact_claims fc
# MAGIC JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
# MAGIC JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
# MAGIC GROUP BY dc.customer_id, dc.region, dc.state, dc.city
# MAGIC ORDER BY total_claim_value DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Risk tiers
# MAGIC Segment customers into HIGH, MEDIUM, LOW risk based on claim patterns.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_claims AS (
# MAGIC   SELECT
# MAGIC     dc.customer_id,
# MAGIC     dc.region,
# MAGIC     COUNT(fc.claim_id) as claim_count,
# MAGIC     ROUND(SUM(fc.total_claim_amount), 2) as total_value,
# MAGIC     ROUND(SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rejection_rate
# MAGIC   FROM primeins.gold.fact_claims fc
# MAGIC   JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
# MAGIC   JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
# MAGIC   GROUP BY dc.customer_id, dc.region
# MAGIC )
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN claim_count >= 3 OR total_value > 50000 THEN 'HIGH'
# MAGIC     WHEN claim_count >= 2 OR total_value > 25000 THEN 'MEDIUM'
# MAGIC     ELSE 'LOW'
# MAGIC   END as risk_tier,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   ROUND(AVG(claim_count), 1) as avg_claims,
# MAGIC   ROUND(AVG(total_value), 2) as avg_total_value,
# MAGIC   ROUND(AVG(rejection_rate), 1) as avg_rejection_rate
# MAGIC FROM customer_claims
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. High-risk customers — detailed view
# MAGIC Customers with 3+ claims OR total claim value above $50K.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_claims AS (
# MAGIC   SELECT
# MAGIC     dc.customer_id,
# MAGIC     dc.region,
# MAGIC     dc.state,
# MAGIC     dc.city,
# MAGIC     dc.job,
# MAGIC     dc.marital,
# MAGIC     COUNT(fc.claim_id) as claim_count,
# MAGIC     ROUND(SUM(fc.total_claim_amount), 2) as total_value,
# MAGIC     ROUND(AVG(fc.total_claim_amount), 2) as avg_value,
# MAGIC     SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) as rejected,
# MAGIC     ROUND(SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as rejection_rate
# MAGIC   FROM primeins.gold.fact_claims fc
# MAGIC   JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
# MAGIC   JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
# MAGIC   GROUP BY dc.customer_id, dc.region, dc.state, dc.city, dc.job, dc.marital
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM customer_claims
# MAGIC WHERE claim_count >= 3 OR total_value > 50000
# MAGIC ORDER BY total_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Repeat claimants
# MAGIC Customers who have filed multiple claims — a fraud indicator.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.customer_id,
# MAGIC   dc.region,
# MAGIC   COUNT(fc.claim_id) as claim_count,
# MAGIC   ROUND(SUM(fc.total_claim_amount), 2) as total_value,
# MAGIC   SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) as rejected,
# MAGIC   COLLECT_SET(fc.incident_type) as incident_types,
# MAGIC   COLLECT_SET(fc.incident_severity) as severities
# MAGIC FROM primeins.gold.fact_claims fc
# MAGIC JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
# MAGIC JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
# MAGIC GROUP BY dc.customer_id, dc.region
# MAGIC HAVING COUNT(fc.claim_id) >= 2
# MAGIC ORDER BY claim_count DESC, total_value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Customers with anomaly-flagged claims
# MAGIC Customers whose claims were flagged by the fraud detection engine.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.customer_id,
# MAGIC   dc.region,
# MAGIC   dc.state,
# MAGIC   ca.claim_id,
# MAGIC   ca.anomaly_score,
# MAGIC   ca.priority,
# MAGIC   ROUND(ca.total_claim_amount, 2) as claim_amount,
# MAGIC   ca.triggered_rules,
# MAGIC   LEFT(ca.what_is_suspicious, 200) as suspicious_summary
# MAGIC FROM primeins.gold.claim_anomaly_explanations ca
# MAGIC JOIN primeins.gold.fact_claims fc ON ca.claim_id = fc.claim_id
# MAGIC JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
# MAGIC JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
# MAGIC WHERE ca.priority = 'HIGH'
# MAGIC ORDER BY ca.anomaly_score DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Risk distribution by region
# MAGIC Which regions have the most high-risk customers?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_risk AS (
# MAGIC   SELECT
# MAGIC     dc.customer_id,
# MAGIC     dc.region,
# MAGIC     COUNT(fc.claim_id) as claim_count,
# MAGIC     SUM(fc.total_claim_amount) as total_value,
# MAGIC     CASE
# MAGIC       WHEN COUNT(fc.claim_id) >= 3 OR SUM(fc.total_claim_amount) > 50000 THEN 'HIGH'
# MAGIC       WHEN COUNT(fc.claim_id) >= 2 OR SUM(fc.total_claim_amount) > 25000 THEN 'MEDIUM'
# MAGIC       ELSE 'LOW'
# MAGIC     END as risk_tier
# MAGIC   FROM primeins.gold.fact_claims fc
# MAGIC   JOIN primeins.gold.dim_policy dp ON fc.policy_number = dp.policy_number
# MAGIC   JOIN primeins.gold.dim_customer dc ON dp.customer_id = dc.customer_id
# MAGIC   GROUP BY dc.customer_id, dc.region
# MAGIC )
# MAGIC SELECT
# MAGIC   region,
# MAGIC   risk_tier,
# MAGIC   COUNT(*) as customer_count,
# MAGIC   ROUND(AVG(total_value), 2) as avg_claim_value
# MAGIC FROM customer_risk
# MAGIC GROUP BY region, risk_tier
# MAGIC ORDER BY region, risk_tier

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Customer value vs risk
# MAGIC Premium-paying customers who are also high-risk — retention vs risk decision.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_profile AS (
# MAGIC   SELECT
# MAGIC     dc.customer_id,
# MAGIC     dc.region,
# MAGIC     dp.policy_csl,
# MAGIC     dp.policy_annual_premium as premium,
# MAGIC     COUNT(fc.claim_id) as claim_count,
# MAGIC     ROUND(SUM(fc.total_claim_amount), 2) as total_claims_value,
# MAGIC     SUM(CASE WHEN fc.is_rejected THEN 1 ELSE 0 END) as rejected
# MAGIC   FROM primeins.gold.dim_customer dc
# MAGIC   JOIN primeins.gold.dim_policy dp ON dc.customer_id = dp.customer_id
# MAGIC   LEFT JOIN primeins.gold.fact_claims fc ON dp.policy_number = fc.policy_number
# MAGIC   GROUP BY dc.customer_id, dc.region, dp.policy_csl, dp.policy_annual_premium
# MAGIC )
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   region,
# MAGIC   policy_csl,
# MAGIC   ROUND(premium, 2) as annual_premium,
# MAGIC   claim_count,
# MAGIC   total_claims_value,
# MAGIC   rejected,
# MAGIC   CASE
# MAGIC     WHEN total_claims_value > premium * 3 THEN 'UNPROFITABLE'
# MAGIC     WHEN total_claims_value > premium THEN 'AT RISK'
# MAGIC     ELSE 'PROFITABLE'
# MAGIC   END as profitability
# MAGIC FROM customer_profile
# MAGIC WHERE claim_count > 0
# MAGIC ORDER BY total_claims_value DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — action items for the underwriting team
# MAGIC
# MAGIC | Action | Detail |
# MAGIC |--------|--------|
# MAGIC | Review HIGH risk customers | Customers with 3+ claims or $50K+ total value need underwriting review |
# MAGIC | Investigate repeat claimants | Multiple claims from one customer, especially with different incident types |
# MAGIC | Cross-reference anomaly flags | Customers whose claims were flagged by fraud detection (see section 5) |
# MAGIC | Premium adjustment candidates | Unprofitable customers where claims exceed 3x their annual premium |
# MAGIC | Regional risk patterns | Regions with higher concentrations of high-risk customers may need adjusted underwriting criteria |
