# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Dimensional Model & Aggregations
# MAGIC
# MAGIC This DLT pipeline builds the star schema from cleaned Silver tables.
# MAGIC 4 dimension tables, 2 fact tables, and pre-computed aggregations
# MAGIC as Materialized Views that auto-refresh when upstream data changes.
# MAGIC
# MAGIC **Tables created in primeins.gold:**
# MAGIC - dim_customer, dim_policy, dim_car, dim_region
# MAGIC - fact_claims, fact_sales
# MAGIC - mv_rejection_rate_by_policy (Materialized View)
# MAGIC - mv_claims_by_severity (Materialized View)
# MAGIC - mv_unsold_inventory (Materialized View)
# MAGIC - mv_claims_by_region (Materialized View)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== DIMENSION TABLES ==========

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customer
# MAGIC Source: silver.customers (harmonized from 7 CSV files).
# MAGIC Deduplicated on customer_id. Contains all customer attributes
# MAGIC for slicing claims and policy data by region, state, job, etc.

# COMMAND ----------

@dlt.table(
    name="dim_customer",
    comment="Customer dimension. Deduplicated, harmonized from 7 regional files.",
    table_properties={"quality": "gold"},
)
def dim_customer():
    df = spark.read.table("primeins.silver.customers")

    # Deduplicate: if same customer_id appears multiple times,
    # keep the first occurrence (by load timestamp)
    from pyspark.sql.window import Window
    w = Window.partitionBy("customer_id").orderBy("_load_timestamp")
    df = df.withColumn("_row_num", F.row_number().over(w))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    return df.select(
        "customer_id",
        "region",
        "state",
        "city",
        "job",
        "marital",
        "education",
        "default_flag",
        "balance",
        "hh_insurance",
        "car_loan"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_policy
# MAGIC Source: silver.policy. Contains coverage details and the foreign keys
# MAGIC (customer_id, car_id) that connect customers to claims.
# MAGIC This is the bridge table: fact_claims -> dim_policy -> dim_customer.

# COMMAND ----------

@dlt.table(
    name="dim_policy",
    comment="Policy dimension. Links customers to claims via policy_number. Contains coverage details.",
    table_properties={"quality": "gold"},
)
def dim_policy():
    df = spark.read.table("primeins.silver.policy")

    return df.select(
        "policy_number",
        "policy_bind_date",
        "policy_state",
        "policy_csl",
        "policy_deductible",
        "policy_annual_premium",
        "umbrella_limit",
        F.col("car_id"),
        F.col("customer_id")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_car
# MAGIC Source: silver.cars. Vehicle reference data.
# MAGIC Referenced by both dim_policy (insurance side) and fact_sales (business side).
# MAGIC This bridges the insurance and sales domains.

# COMMAND ----------

@dlt.table(
    name="dim_car",
    comment="Car dimension. Bridges insurance (policy/claims) and business (sales) sides.",
    table_properties={"quality": "gold"},
)
def dim_car():
    df = spark.read.table("primeins.silver.cars")

    return df.select(
        "car_id",
        "name",
        "model",
        "fuel",
        "transmission",
        "km_driven",
        "mileage",
        "mileage_unit",
        "engine",
        "max_power",
        "torque",
        "seats"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_region
# MAGIC Small lookup table for the 5 standard regions.
# MAGIC Hardcoded since regions are fixed for PrimeInsurance.

# COMMAND ----------

@dlt.table(
    name="dim_region",
    comment="Region lookup. 5 standard PrimeInsurance regions.",
    table_properties={"quality": "gold"},
)
def dim_region():
    data = [
        ("East", "E"),
        ("West", "W"),
        ("Central", "C"),
        ("South", "S"),
        ("North", "N"),
    ]
    return spark.createDataFrame(data, ["region", "region_code"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== FACT TABLES ==========

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_claims
# MAGIC Grain: one row per claim.
# MAGIC Measures: claim amounts (injury, property, vehicle, total),
# MAGIC rejection status, severity, incident details.
# MAGIC
# MAGIC Note on dates: incident_date, claim_logged_on, claim_processed_on
# MAGIC are corrupted at source (only time portions like "27:00.0" survived).
# MAGIC We include them for completeness but processing_time_days cannot
# MAGIC be calculated from this data.

# COMMAND ----------

@dlt.table(
    name="fact_claims",
    comment="Claim facts. 1 row per claim. Amounts, rejection status, severity. Join to dim_policy on policy_number.",
    table_properties={"quality": "gold"},
)
def fact_claims():
    claims = spark.read.table("primeins.silver.claims")

    df = claims.select(
        F.col("claim_id"),
        # policy_id in Silver claims maps to policy_number in dim_policy
        F.col("policy_id").alias("policy_number"),
        F.col("incident_date"),
        F.col("incident_state"),
        F.col("incident_city"),
        F.col("incident_type"),
        F.col("collision_type"),
        F.col("incident_severity"),
        F.col("injury_amount"),
        F.col("property_amount"),
        F.col("vehicle_amount"),
        # Total claim amount = sum of all three components
        (
            F.coalesce(F.col("injury_amount"), F.lit(0)) +
            F.coalesce(F.col("property_amount"), F.lit(0)) +
            F.coalesce(F.col("vehicle_amount"), F.lit(0))
        ).alias("total_claim_amount"),
        F.col("vehicles_involved"),
        F.col("bodily_injuries"),
        F.col("witnesses"),
        F.col("authorities_contacted"),
        F.col("property_damage"),
        F.col("police_report_available"),
        # Convert Y/N to boolean for easier aggregation
        F.when(F.col("claim_rejected") == "Y", True)
         .otherwise(False)
         .alias("is_rejected"),
        F.col("claim_logged_on"),
        F.col("claim_processed_on"),
        F.col("_source_file"),
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_sales
# MAGIC Grain: one row per car listing.
# MAGIC Measures: selling price, days_listed, is_sold flag.
# MAGIC
# MAGIC days_listed: if sold, days between ad_placed_on and sold_on.
# MAGIC If unsold, days between ad_placed_on and today.
# MAGIC is_sold: true if sold_on is not null.

# COMMAND ----------

@dlt.table(
    name="fact_sales",
    comment="Sales facts. 1 row per listing. Price, days_listed, is_sold. Join to dim_car on car_id.",
    table_properties={"quality": "gold"},
)
def fact_sales():
    sales = spark.read.table("primeins.silver.sales")

    df = sales.select(
        F.col("sales_id"),
        F.col("car_id"),
        F.col("ad_placed_on"),
        F.col("sold_on"),
        F.col("original_selling_price"),
        # days_listed: sold -> days between ad and sale; unsold -> days since ad
        F.when(
            F.col("sold_on").isNotNull(),
            F.datediff(F.col("sold_on"), F.col("ad_placed_on"))
        ).otherwise(
            F.datediff(F.current_date(), F.col("ad_placed_on"))
        ).alias("days_listed"),
        # is_sold flag
        F.col("sold_on").isNotNull().alias("is_sold"),
        F.col("region"),
        F.col("state"),
        F.col("city"),
        F.col("seller_type"),
        F.col("owner"),
        F.col("_source_file"),
    )

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== MATERIALIZED VIEWS (Pre-computed Aggregations) ==========
# MAGIC
# MAGIC These auto-refresh when upstream data changes.
# MAGIC The SQL Warehouse serves them to business users.
# MAGIC Each one answers a specific business question.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rejection rate by policy type
# MAGIC Answers: "Which policy type (by coverage limit) has the highest claim rejection rate?"
# MAGIC Addresses: Claims backlog / regulatory pressure

# COMMAND ----------

@dlt.table(
    name="mv_rejection_rate_by_policy",
    comment="Rejection rate by policy_csl. Answers: which policy type has the highest rejection rate?",
    table_properties={"quality": "gold"},
)
def mv_rejection_rate_by_policy():
    claims = dlt.read("fact_claims")
    policy = dlt.read("dim_policy")

    df = claims.join(policy, "policy_number", "inner")

    return df.groupBy("policy_csl").agg(
        F.count("*").alias("total_claims"),
        F.sum(F.when(F.col("is_rejected"), 1).otherwise(0)).alias("rejected_claims"),
        (
            F.sum(F.when(F.col("is_rejected"), 1).otherwise(0)) * 100.0 / F.count("*")
        ).alias("rejection_rate_pct"),
        F.avg("total_claim_amount").alias("avg_claim_amount"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims by severity
# MAGIC Answers: "What is the average claim value by incident severity?"
# MAGIC Note: processing time cannot be calculated due to corrupted dates.

# COMMAND ----------

@dlt.table(
    name="mv_claims_by_severity",
    comment="Claim stats by severity. Count, rejection rate, avg claim value.",
    table_properties={"quality": "gold"},
)
def mv_claims_by_severity():
    claims = dlt.read("fact_claims")

    return claims.groupBy("incident_severity").agg(
        F.count("*").alias("total_claims"),
        F.sum(F.when(F.col("is_rejected"), 1).otherwise(0)).alias("rejected_claims"),
        (
            F.sum(F.when(F.col("is_rejected"), 1).otherwise(0)) * 100.0 / F.count("*")
        ).alias("rejection_rate_pct"),
        F.avg("total_claim_amount").alias("avg_claim_amount"),
        F.sum("total_claim_amount").alias("total_claim_value"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unsold inventory
# MAGIC Answers: "Which car models are sitting unsold and for how long?"
# MAGIC Addresses: Revenue leakage problem

# COMMAND ----------

@dlt.table(
    name="mv_unsold_inventory",
    comment="Unsold cars by model. Days listed, count, avg price. Addresses revenue leakage.",
    table_properties={"quality": "gold"},
)
def mv_unsold_inventory():
    sales = dlt.read("fact_sales")
    cars = dlt.read("dim_car")

    unsold = sales.filter(F.col("is_sold") == False)
    df = unsold.join(cars, "car_id", "inner")

    return df.groupBy("model", "region").agg(
        F.count("*").alias("unsold_count"),
        F.avg("days_listed").cast("int").alias("avg_days_listed"),
        F.max("days_listed").alias("max_days_listed"),
        F.avg("original_selling_price").alias("avg_price"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims by region
# MAGIC Answers: "How does claim volume compare across regions?"
# MAGIC Join path: fact_claims -> dim_policy -> dim_customer (for region)

# COMMAND ----------

@dlt.table(
    name="mv_claims_by_region",
    comment="Claim volume and value by region. Join: claims -> policy -> customer.",
    table_properties={"quality": "gold"},
)
def mv_claims_by_region():
    claims = dlt.read("fact_claims")
    policy = dlt.read("dim_policy")
    customer = dlt.read("dim_customer")

    # Join chain: claims -> policy -> customer (for region)
    df = (
        claims
        .join(policy, "policy_number", "inner")
        .join(customer, "customer_id", "inner")
    )

    return df.groupBy("region").agg(
        F.count("*").alias("total_claims"),
        F.sum(F.when(F.col("is_rejected"), 1).otherwise(0)).alias("rejected_claims"),
        (
            F.sum(F.when(F.col("is_rejected"), 1).otherwise(0)) * 100.0 / F.count("*")
        ).alias("rejection_rate_pct"),
        F.avg("total_claim_amount").alias("avg_claim_amount"),
        F.sum("total_claim_amount").alias("total_claim_value"),
    )
