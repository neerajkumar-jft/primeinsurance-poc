# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — DLT Pipeline
# MAGIC Builds the star schema dimensional model from Silver data.
# MAGIC
# MAGIC ## Star Schema:
# MAGIC - **Fact Tables**: fact_claims, fact_sales
# MAGIC - **Dimension Tables**: dim_customer, dim_policy, dim_car, dim_region, dim_date
# MAGIC - **Materialized Views**: Pre-computed aggregations for the 3 business failures
# MAGIC
# MAGIC ## Business Problems Addressed:
# MAGIC 1. Regulatory: Deduplicated customer count by region
# MAGIC 2. Claims backlog: Processing time, rejection rate by region/policy type
# MAGIC 3. Revenue leakage: Unsold inventory by model/region with days since listing

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, coalesce, lit, current_date,
    datediff, countDistinct, sum, avg, count,
    year, quarter, month, dayofmonth, dayofweek,
    expr, round
)
from pyspark.sql.types import IntegerType, DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dimension Tables
# MAGIC Dimensions provide the "who, what, where, when" context for facts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_customer
# MAGIC Deduplicated customer master. One row per unique customer_id.
# MAGIC This solves Business Problem #1: regulatory pressure from duplicate customers.

# COMMAND ----------

@dlt.table(
    name="dim_customer",
    comment="Deduplicated customer dimension. One row per unique customer_id, resolving duplicates across 7 source files.",
    table_properties={"quality": "gold"}
)
def dim_customer():
    df = spark.read.table("`databricks-hackathon-insurance`.silver.silver_customers")

    # Deduplicate: keep the most complete record per customer_id
    # Use row_number partitioned by customer_id, prefer rows with more non-null fields
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, coalesce, lit

    # Score each row by completeness (count of non-null fields)
    df = df.withColumn("_completeness",
        (when(col("region").isNotNull(), 1).otherwise(0) +
         when(col("state").isNotNull(), 1).otherwise(0) +
         when(col("city").isNotNull(), 1).otherwise(0) +
         when(col("job").isNotNull(), 1).otherwise(0) +
         when(col("marital_status").isNotNull(), 1).otherwise(0) +
         when(col("education").isNotNull(), 1).otherwise(0))
    )

    window = Window.partitionBy("customer_id").orderBy(col("_completeness").desc())

    return (
        df
        .withColumn("_rank", row_number().over(window))
        .filter(col("_rank") == 1)
        .select(
            "customer_id",
            "region",
            "state",
            "city",
            "job",
            "marital_status",
            "education",
            "default_flag",
            "balance",
            "hh_insurance",
            "car_loan"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_policy
# MAGIC One row per policy. Links customers to cars.

# COMMAND ----------

@dlt.table(
    name="dim_policy",
    comment="Policy dimension. Links customers to their insured cars with coverage details.",
    table_properties={"quality": "gold"}
)
def dim_policy():
    return (
        spark.read.table("`databricks-hackathon-insurance`.silver.silver_policy")
        .select(
            "policy_number",
            "policy_bind_date",
            "policy_state",
            "policy_csl",
            "policy_deductible",
            "policy_annual_premium",
            "umbrella_limit",
            "car_id",
            "customer_id"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_car
# MAGIC Vehicle catalogue dimension. One row per car.

# COMMAND ----------

@dlt.table(
    name="dim_car",
    comment="Vehicle catalogue dimension with cleaned specs.",
    table_properties={"quality": "gold"}
)
def dim_car():
    return (
        spark.read.table("`databricks-hackathon-insurance`.silver.silver_cars")
        .select(
            "car_id",
            "name",
            "model",
            "fuel",
            "transmission",
            "km_driven",
            "mileage_kmpl",
            "engine_cc",
            "max_power_bhp",
            "torque",
            "seats"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_region
# MAGIC Simple region dimension derived from customer data.

# COMMAND ----------

@dlt.table(
    name="dim_region",
    comment="Region dimension - the 4 operating regions of PrimeInsurance.",
    table_properties={"quality": "gold"}
)
def dim_region():
    return (
        spark.read.table("`databricks-hackathon-insurance`.silver.silver_customers")
        .select("region")
        .distinct()
        .filter(col("region").isNotNull())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date
# MAGIC Date dimension generated from the range of dates in our data.

# COMMAND ----------

@dlt.table(
    name="dim_date",
    comment="Date dimension covering the full range of dates in the dataset.",
    table_properties={"quality": "gold"}
)
def dim_date():
    # Generate date range from 2015-01-01 to 2027-12-31
    dates = (
        spark.sql("""
            SELECT explode(sequence(
                to_date('2015-01-01'),
                to_date('2027-12-31'),
                interval 1 day
            )) as date
        """)
    )

    return (
        dates
        .withColumn("year", year("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("day_of_week", dayofweek("date"))
        .withColumn("day_name",
            when(dayofweek("date") == 1, "Sunday")
            .when(dayofweek("date") == 2, "Monday")
            .when(dayofweek("date") == 3, "Tuesday")
            .when(dayofweek("date") == 4, "Wednesday")
            .when(dayofweek("date") == 5, "Thursday")
            .when(dayofweek("date") == 6, "Friday")
            .when(dayofweek("date") == 7, "Saturday")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Fact Tables
# MAGIC Facts record business events with measurable values.

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_claims
# MAGIC One row per insurance claim. Joins claims with policy and customer data.
# MAGIC This solves Business Problem #2: claims backlog (18-day avg processing).
# MAGIC
# MAGIC **Grain**: One row = one claim event
# MAGIC **Measures**: injury_amount, property_amount, vehicle_amount, total_claim_amount, processing_days
# MAGIC **Foreign Keys**: policy_id → dim_policy, customer_id → dim_customer, car_id → dim_car

# COMMAND ----------

@dlt.table(
    name="fact_claims",
    comment="Claims fact table. One row per claim with amounts, processing time, and links to all dimensions.",
    table_properties={"quality": "gold"}
)
def fact_claims():
    claims = spark.read.table("`databricks-hackathon-insurance`.silver.silver_claims")
    policy = spark.read.table("`databricks-hackathon-insurance`.silver.silver_policy")
    customers = spark.read.table("`databricks-hackathon-insurance`.silver.silver_customers")

    # Join claims → policy (to get customer_id, car_id, policy details)
    # Then join → customers (to get region)
    return (
        claims
        .join(
            policy.select("policy_number", "customer_id", "car_id", "policy_csl", "policy_annual_premium"),
            claims.policy_id == policy.policy_number,
            "left"
        )
        .join(
            customers.select("customer_id", "region").dropDuplicates(["customer_id"]),
            on="customer_id",
            how="left"
        )
        .withColumn("total_claim_amount",
            coalesce(col("injury_amount"), lit(0)) +
            coalesce(col("property_amount"), lit(0)) +
            coalesce(col("vehicle_amount"), lit(0))
        )
        .withColumn("processing_days",
            when(
                col("claim_processed_on").isNotNull() & col("claim_logged_on").isNotNull(),
                datediff(col("claim_processed_on"), col("claim_logged_on"))
            ).otherwise(None)
        )
        .withColumn("is_rejected",
            when(col("claim_rejected") == "Y", True).otherwise(False)
        )
        .select(
            "claim_id",
            "policy_id",
            "customer_id",
            "car_id",
            "region",
            "policy_csl",
            "incident_date",
            "claim_logged_on",
            "claim_processed_on",
            "processing_days",
            "incident_type",
            "collision_type",
            "incident_severity",
            "incident_state",
            "incident_city",
            "injury_amount",
            "property_amount",
            "vehicle_amount",
            "total_claim_amount",
            "is_rejected",
            "claim_rejected",
            "bodily_injuries",
            "witnesses",
            "num_vehicles_involved",
            "authorities_contacted",
            "property_damage",
            "police_report_available"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### fact_sales
# MAGIC One row per car listing/sale. Calculates days to sell and days since listing.
# MAGIC This solves Business Problem #3: revenue leakage from unsold inventory.
# MAGIC
# MAGIC **Grain**: One row = one car listing
# MAGIC **Measures**: selling_price, days_to_sell, days_since_listing
# MAGIC **Foreign Keys**: car_id → dim_car

# COMMAND ----------

@dlt.table(
    name="fact_sales",
    comment="Sales fact table. One row per listing with pricing, sale timing, and inventory aging.",
    table_properties={"quality": "gold"}
)
def fact_sales():
    sales = spark.read.table("`databricks-hackathon-insurance`.silver.silver_sales")
    cars = spark.read.table("`databricks-hackathon-insurance`.silver.silver_cars")

    return (
        sales
        .join(
            cars.select("car_id", "model", "name", "fuel"),
            on="car_id",
            how="left"
        )
        # Days to sell (for sold cars)
        .withColumn("days_to_sell",
            when(
                col("is_sold") == True,
                datediff(col("sold_on").cast(DateType()), col("ad_placed_on").cast(DateType()))
            ).otherwise(None)
        )
        # Days since listing (for unsold cars — how long it's been sitting)
        .withColumn("days_since_listing",
            when(
                col("is_sold") == False,
                datediff(current_date(), col("ad_placed_on").cast(DateType()))
            ).otherwise(None)
        )
        .select(
            "sales_id",
            "car_id",
            "model",
            "name",
            "fuel",
            "ad_placed_on",
            "sold_on",
            "is_sold",
            "selling_price",
            "days_to_sell",
            "days_since_listing",
            "region",
            "state",
            "city",
            "seller_type",
            "owner"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Materialized Views (Pre-computed Aggregations)
# MAGIC These serve the 3 business problems directly. They auto-refresh when the pipeline runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### mv_claims_performance
# MAGIC **Business Problem #2**: Claims backlog
# MAGIC - Rejection rate by region and policy type
# MAGIC - Average processing time
# MAGIC - Total claim value

# COMMAND ----------

@dlt.table(
    name="mv_claims_performance",
    comment="Pre-aggregated claims metrics by region and policy type. Addresses claims backlog problem.",
    table_properties={"quality": "gold"}
)
def mv_claims_performance():
    claims = spark.read.table("`databricks-hackathon-insurance`.silver.silver_claims")
    policy = spark.read.table("`databricks-hackathon-insurance`.silver.silver_policy")
    customers = spark.read.table("`databricks-hackathon-insurance`.silver.silver_customers")

    df = (
        claims
        .join(
            policy.select("policy_number", "customer_id", "policy_csl"),
            claims.policy_id == policy.policy_number,
            "left"
        )
        .join(
            customers.select("customer_id", "region").dropDuplicates(["customer_id"]),
            on="customer_id",
            how="left"
        )
        .withColumn("total_claim_amount",
            coalesce(col("injury_amount"), lit(0)) +
            coalesce(col("property_amount"), lit(0)) +
            coalesce(col("vehicle_amount"), lit(0))
        )
        .withColumn("processing_days",
            when(
                col("claim_processed_on").isNotNull() & col("claim_logged_on").isNotNull(),
                datediff(col("claim_processed_on"), col("claim_logged_on"))
            )
        )
        .withColumn("is_rejected", when(col("claim_rejected") == "Y", 1).otherwise(0))
    )

    return (
        df.groupBy("region", "policy_csl")
        .agg(
            count("*").alias("total_claims"),
            sum("is_rejected").alias("rejected_claims"),
            round(avg("is_rejected") * 100, 2).alias("rejection_rate_pct"),
            round(avg("processing_days"), 1).alias("avg_processing_days"),
            sum("total_claim_amount").alias("total_claim_value"),
            round(avg("total_claim_amount"), 0).alias("avg_claim_value")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### mv_customer_count_by_region
# MAGIC **Business Problem #1**: Regulatory pressure — deduplicated customer count.
# MAGIC Shows the TRUE customer count per region after removing duplicates.

# COMMAND ----------

@dlt.table(
    name="mv_customer_count_by_region",
    comment="Deduplicated customer count by region. Addresses regulatory compliance problem.",
    table_properties={"quality": "gold"}
)
def mv_customer_count_by_region():
    customers = spark.read.table("`databricks-hackathon-insurance`.silver.silver_customers")

    return (
        customers
        .dropDuplicates(["customer_id"])
        .groupBy("region")
        .agg(
            countDistinct("customer_id").alias("unique_customers"),
            count("*").alias("total_records_before_dedup")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### mv_unsold_inventory
# MAGIC **Business Problem #3**: Revenue leakage — unsold car inventory by model and region.
# MAGIC Shows which cars have been sitting unsold the longest.

# COMMAND ----------

@dlt.table(
    name="mv_unsold_inventory",
    comment="Unsold car inventory by model and region with aging analysis. Addresses revenue leakage problem.",
    table_properties={"quality": "gold"}
)
def mv_unsold_inventory():
    sales = spark.read.table("`databricks-hackathon-insurance`.silver.silver_sales")
    cars = spark.read.table("`databricks-hackathon-insurance`.silver.silver_cars")

    unsold = (
        sales
        .filter(col("is_sold") == False)
        .join(
            cars.select("car_id", "model", "fuel"),
            on="car_id",
            how="left"
        )
        .withColumn("days_since_listing",
            datediff(current_date(), col("ad_placed_on").cast(DateType()))
        )
    )

    return (
        unsold.groupBy("model", "region")
        .agg(
            count("*").alias("unsold_count"),
            round(avg("days_since_listing"), 0).alias("avg_days_since_listing"),
            round(avg("selling_price"), 0).alias("avg_listing_price")
        )
    )
