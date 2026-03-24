# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer - Fact Tables
# MAGIC
# MAGIC Two main fact tables:
# MAGIC - fact_claims: one row per claim with all dimension keys + measures
# MAGIC - fact_sales: one row per sale/listing with keys + measures
# MAGIC
# MAGIC These connect to our dimensions via surrogate keys.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime

# COMMAND ----------

# load dimensions for key lookups
dim_customer = spark.table("`databricks-hackathon-insurance`.gold.dim_customer")
dim_policy = spark.table("`databricks-hackathon-insurance`.gold.dim_policy")
dim_car = spark.table("`databricks-hackathon-insurance`.gold.dim_car")
dim_date = spark.table("`databricks-hackathon-insurance`.gold.dim_date")

# load silver tables
silver_claims = spark.table("`databricks-hackathon-insurance`.silver.claims")
silver_sales = spark.table("`databricks-hackathon-insurance`.silver.sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_claims
# MAGIC
# MAGIC Grain: one row per claim.
# MAGIC Measures: amounts (injury, property, vehicle, total), processing_days, sla_breach.
# MAGIC FKs: customer, policy, car, dates.

# COMMAND ----------

# build fact_claims step by step to avoid column ambiguity

# step 1: join claims to policy (get customer_id, car_id, policy_sk)
policy_cols = dim_policy.select(
    F.col("policy_number").alias("_pol_number"),
    F.col("policy_sk"),
    F.col("customer_id").alias("_pol_customer_id"),
    F.col("car_id").alias("_pol_car_id"),
)

claims_with_policy = silver_claims.join(
    policy_cols,
    silver_claims.policy_id == policy_cols._pol_number,
    "left"
)

# step 2: join to customer dim (get customer_sk)
cust_cols = dim_customer.select(
    F.col("customer_sk"),
    F.col("original_customer_id").alias("_cust_orig_id"),
)

claims_with_cust = claims_with_policy.join(
    cust_cols,
    F.col("_pol_customer_id") == F.col("_cust_orig_id"),
    "left"
)

# step 3: join to car dim (get car_sk)
car_cols = dim_car.select(
    F.col("car_sk"),
    F.col("car_id").alias("_car_dim_id"),
)

fact_claims = claims_with_cust.join(
    car_cols,
    F.col("_pol_car_id") == F.col("_car_dim_id"),
    "left"
)

# add date SKs
fact_claims = (fact_claims
    .withColumn("incident_date_sk", F.date_format("incident_date", "yyyyMMdd").cast("integer"))
    .withColumn("logged_date_sk", F.date_format("claim_logged_on", "yyyyMMdd").cast("integer"))
    .withColumn("processed_date_sk", F.date_format("claim_processed_on", "yyyyMMdd").cast("integer"))
)

# COMMAND ----------

# select final columns for the fact table
fact_claims_final = (fact_claims
    .withColumn("claim_sk",
        F.row_number().over(Window.orderBy("claim_id"))
    )
    .select(
        "claim_sk",
        "claim_id",
        # dimension FKs
        "customer_sk",
        "policy_sk",
        "car_sk",
        "incident_date_sk",
        "logged_date_sk",
        "processed_date_sk",
        # degenerate dimensions (stay on fact)
        "incident_state",
        "incident_city",
        "incident_type",
        "collision_type",
        "incident_severity",
        "authorities_contacted",
        "police_report_available",
        "claim_rejected",
        # measures
        "injury",
        "property",
        "vehicle",
        "total_claim_amount",
        "vehicles_involved",
        "bodily_injuries",
        "witnesses",
        "processing_days",
        "sla_breach",
    )
)

(fact_claims_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.fact_claims"))

print(f"gold.fact_claims: {fact_claims_final.count()} rows")

# COMMAND ----------

# quick verification - can we join back to dimensions?
print("=== Join verification ===")
matched_customers = fact_claims_final.filter(F.col("customer_sk").isNotNull()).count()
matched_policies = fact_claims_final.filter(F.col("policy_sk").isNotNull()).count()
total = fact_claims_final.count()
print(f"  customer match rate: {matched_customers}/{total} ({matched_customers/total*100 if total > 0 else 0:.1f}%)")
print(f"  policy match rate: {matched_policies}/{total} ({matched_policies/total*100 if total > 0 else 0:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_sales
# MAGIC
# MAGIC Grain: one row per car listing/sale.
# MAGIC Measures: selling_price, days_on_lot, is_sold.

# COMMAND ----------

sales_car_cols = dim_car.select(
    F.col("car_sk"),
    F.col("car_id").alias("_sales_car_dim_id"),
)

fact_sales = (silver_sales
    .join(
        sales_car_cols,
        silver_sales.car_id == F.col("_sales_car_dim_id"),
        "left"
    )
    # date SKs
    .withColumn("ad_date_sk",
        F.date_format("ad_placed_on", "yyyyMMdd").cast("integer")
    )
    .withColumn("sold_date_sk",
        F.date_format("sold_on", "yyyyMMdd").cast("integer")
    )
)

fact_sales_final = (fact_sales
    .withColumn("sale_sk",
        F.row_number().over(Window.orderBy("sales_id"))
    )
    .select(
        "sale_sk",
        "sales_id",
        # dimension FKs
        "car_sk",
        "ad_date_sk",
        "sold_date_sk",
        # degenerate dimensions
        "region",
        "state",
        "city",
        "seller_type",
        "owner",
        # measures
        "original_selling_price",
        "days_on_lot",
        "is_sold",
        "aging_flag",
    )
)

(fact_sales_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.fact_sales"))

print(f"gold.fact_sales: {fact_sales_final.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business-Problem-Specific Gold Tables
# MAGIC
# MAGIC These directly address the three business failures.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Regulatory Customer Registry
# MAGIC
# MAGIC THE answer to the 90-day compliance deadline.
# MAGIC Auditable count of unique customers with dedup proof.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `databricks-hackathon-insurance`.gold.regulatory_customer_registry AS
# MAGIC SELECT
# MAGIC   c.master_customer_id,
# MAGIC   c.region,
# MAGIC   c.state,
# MAGIC   c.city,
# MAGIC   -- how many regional IDs mapped to this customer
# MAGIC   a.regional_id_count,
# MAGIC   a.regional_ids,
# MAGIC   a.source_files
# MAGIC FROM `databricks-hackathon-insurance`.silver.customers c
# MAGIC LEFT JOIN (
# MAGIC   SELECT
# MAGIC     master_customer_id,
# MAGIC     COUNT(*) as regional_id_count,
# MAGIC     COLLECT_SET(original_customer_id) as regional_ids,
# MAGIC     COLLECT_SET(_source_file) as source_files
# MAGIC   FROM `databricks-hackathon-insurance`.silver.customer_resolution_audit
# MAGIC   GROUP BY master_customer_id
# MAGIC ) a ON c.master_customer_id = a.master_customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the auditable answer: how many unique customers do we actually have?
# MAGIC SELECT
# MAGIC   COUNT(*) as unique_customer_count,
# MAGIC   SUM(CASE WHEN regional_id_count > 1 THEN 1 ELSE 0 END) as multi_region_customers,
# MAGIC   ROUND(AVG(regional_id_count), 2) as avg_ids_per_customer
# MAGIC FROM `databricks-hackathon-insurance`.gold.regulatory_customer_registry;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Claims SLA Monitor
# MAGIC
# MAGIC Tracks processing times against the 7-day benchmark.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `databricks-hackathon-insurance`.gold.claims_sla_monitor AS
# MAGIC SELECT
# MAGIC   fc.incident_state as region,
# MAGIC   fc.incident_severity as severity,
# MAGIC   dp.policy_csl,
# MAGIC   COUNT(*) as total_claims,
# MAGIC   SUM(CASE WHEN fc.claim_rejected = 'Y' THEN 1 ELSE 0 END) as rejected_claims,
# MAGIC   ROUND(SUM(CASE WHEN fc.claim_rejected = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as rejection_rate_pct,
# MAGIC   ROUND(AVG(fc.processing_days), 1) as avg_processing_days,
# MAGIC   ROUND(AVG(fc.total_claim_amount), 2) as avg_claim_amount,
# MAGIC   SUM(CASE WHEN fc.sla_breach THEN 1 ELSE 0 END) as sla_breaches,
# MAGIC   ROUND(SUM(CASE WHEN fc.sla_breach THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as sla_breach_pct
# MAGIC FROM `databricks-hackathon-insurance`.gold.fact_claims fc
# MAGIC LEFT JOIN `databricks-hackathon-insurance`.gold.dim_policy dp ON fc.policy_sk = dp.policy_sk
# MAGIC GROUP BY fc.incident_state, fc.incident_severity, dp.policy_csl;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- which region/severity combos are worst?
# MAGIC SELECT * FROM `databricks-hackathon-insurance`.gold.claims_sla_monitor
# MAGIC ORDER BY avg_processing_days DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Inventory Aging Alerts
# MAGIC
# MAGIC Flags unsold cars that need attention or cross-regional redistribution.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `databricks-hackathon-insurance`.gold.inventory_aging_alerts AS
# MAGIC SELECT
# MAGIC   fs.region,
# MAGIC   dc.name as car_name,
# MAGIC   dc.model,
# MAGIC   dc.fuel,
# MAGIC   fs.original_selling_price,
# MAGIC   fs.days_on_lot,
# MAGIC   fs.aging_flag,
# MAGIC   -- check if same model sold quickly in another region
# MAGIC   fast_sales.fast_sell_region,
# MAGIC   fast_sales.avg_days_to_sell_elsewhere
# MAGIC FROM `databricks-hackathon-insurance`.gold.fact_sales fs
# MAGIC JOIN `databricks-hackathon-insurance`.gold.dim_car dc ON fs.car_sk = dc.car_sk
# MAGIC LEFT JOIN (
# MAGIC   -- find regions where the same model sells fast
# MAGIC   SELECT
# MAGIC     dc2.model,
# MAGIC     fs2.region as fast_sell_region,
# MAGIC     ROUND(AVG(fs2.days_on_lot), 1) as avg_days_to_sell_elsewhere
# MAGIC   FROM `databricks-hackathon-insurance`.gold.fact_sales fs2
# MAGIC   JOIN `databricks-hackathon-insurance`.gold.dim_car dc2 ON fs2.car_sk = dc2.car_sk
# MAGIC   WHERE fs2.is_sold = true
# MAGIC     AND fs2.days_on_lot < 30
# MAGIC   GROUP BY dc2.model, fs2.region
# MAGIC ) fast_sales ON dc.model = fast_sales.model AND fs.region != fast_sales.fast_sell_region
# MAGIC WHERE fs.is_sold = false
# MAGIC   AND fs.days_on_lot > 60
# MAGIC ORDER BY fs.days_on_lot DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT aging_flag, COUNT(*) as count, ROUND(AVG(days_on_lot), 0) as avg_days
# MAGIC FROM `databricks-hackathon-insurance`.gold.inventory_aging_alerts
# MAGIC GROUP BY aging_flag;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Regulatory Readiness Score
# MAGIC
# MAGIC Single number for leadership: are we audit-ready?

# COMMAND ----------

# calculate readiness score components
from pyspark.sql import Row

# customer dedup completeness
audit = spark.table("`databricks-hackathon-insurance`.silver.customer_resolution_audit")
total_raw = audit.count()
unique_customers = audit.filter(~F.col("is_duplicate")).count()
dedup_score = round((1 - (total_raw - unique_customers) / total_raw) * 100, 1) if total_raw > 0 else 0

# data quality pass rate
dq = spark.table("`databricks-hackathon-insurance`.silver.dq_issues")
avg_pass_rate = 100 - round(dq.select(F.avg("affected_ratio")).collect()[0][0] * 100, 1)

# claims efficiency
claims = spark.table("`databricks-hackathon-insurance`.gold.fact_claims")
avg_proc_days = claims.select(F.avg("processing_days")).collect()[0][0] or 18
efficiency_score = round(max(0, (1 - avg_proc_days / 18.0)) * 100, 1)

# overall weighted score
overall = round(dedup_score * 0.4 + avg_pass_rate * 0.3 + efficiency_score * 0.3, 1)

readiness_status = "AUDIT READY" if overall >= 80 else "NEEDS ATTENTION" if overall >= 60 else "NOT READY"

print(f"=== Regulatory Readiness Score ===")
print(f"  Customer Registry Score: {dedup_score}")
print(f"  Data Quality Score:      {avg_pass_rate}")
print(f"  Claims Efficiency Score: {efficiency_score}")
print(f"  ---")
print(f"  OVERALL READINESS:       {overall} - {readiness_status}")

# COMMAND ----------

# save as a gold table
readiness_data = [{
    "assessment_date": datetime.now().isoformat(),
    "customer_registry_score": float(dedup_score),
    "data_quality_score": float(avg_pass_rate),
    "claims_efficiency_score": float(efficiency_score),
    "overall_readiness_score": float(overall),
    "readiness_status": readiness_status,
}]

readiness_df = spark.createDataFrame(readiness_data)
(readiness_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.regulatory_readiness"))

display(readiness_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------

gold_tables = [
    "dim_customer", "dim_policy", "dim_car", "dim_date",
    "fact_claims", "fact_sales",
    "regulatory_customer_registry", "claims_sla_monitor",
    "inventory_aging_alerts", "regulatory_readiness",
]

print("=== Gold Layer Summary ===\n")
for t in gold_tables:
    try:
        count = spark.table(f"`databricks-hackathon-insurance`.gold.{t}").count()
        print(f"  gold.{t:35s} | {count:6d} rows")
    except Exception:
        print(f"  gold.{t:35s} | NOT FOUND")
