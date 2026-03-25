# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer — DLT Pipeline
# MAGIC Cleans, standardizes, and applies quality rules to Bronze data.
# MAGIC
# MAGIC ## What Silver Does:
# MAGIC - Unifies inconsistent column names across regional files
# MAGIC - Normalizes values (region abbreviations, education typos)
# MAGIC - Fixes corrupted data (Excel serial dates in claims)
# MAGIC - Replaces sentinel values ("NULL", "?") with actual nulls
# MAGIC - Casts string columns to proper types (int, double, date)
# MAGIC - Applies DLT Expectations (quality rules) — bad records are dropped or flagged
# MAGIC - Quarantine tables preserve failed records for compliance review
# MAGIC - Adds business metrics: sla_breach, processing_days, days_on_lot, aging_flag

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    col, when, coalesce, trim, lower, upper,
    current_timestamp, lit, regexp_replace, regexp_extract,
    to_date, to_timestamp, expr, concat, date_add, datediff,
    current_date, initcap
)
from pyspark.sql.types import IntegerType, DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Silver Customers
# MAGIC Unifies 7 customer files with different schemas into one clean table.
# MAGIC - Merges 3 ID columns → customer_id
# MAGIC - Merges Region/Reg + normalizes W/E/S/C → full names
# MAGIC - Merges Education/Edu + fixes typo terto → tertiary
# MAGIC - Merges Marital/Marital_status
# MAGIC - Merges City/City_in_state

# COMMAND ----------

@dlt.table(
    name="silver_customers",
    comment="Cleaned and standardized customer data. Columns unified, regions normalized, typos fixed.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_region", "region IN ('East', 'West', 'North', 'South', 'Central')")
@dlt.expect("valid_education", "education IS NULL OR education IN ('primary', 'secondary', 'tertiary')")
def silver_customers():
    return (
        spark.readStream.table("`databricks-hackathon-insurance`.bronze.bronze_customers")
        # 1. Unify customer ID columns (3 different names → 1)
        .withColumn("customer_id",
            coalesce(
                col("CustomerID"),
                col("Customer_ID"),
                col("cust_id")
            ).cast(IntegerType())
        )
        # 2. Unify region columns + normalize abbreviations
        .withColumn("region",
            when(coalesce(col("Region"), col("Reg")).isin("W", "w", "west"), "West")
            .when(coalesce(col("Region"), col("Reg")).isin("E", "e", "east"), "East")
            .when(coalesce(col("Region"), col("Reg")).isin("S", "s", "south"), "South")
            .when(coalesce(col("Region"), col("Reg")).isin("C", "c", "central"), "Central")
            .when(coalesce(col("Region"), col("Reg")).isin("N", "n", "north"), "North")
            .otherwise(coalesce(col("Region"), col("Reg")))
        )
        # 3. Unify education + fix typo 'terto' → 'tertiary', 'NA' → null
        .withColumn("education",
            when(coalesce(col("Education"), col("Edu")).isin("terto"), "tertiary")
            .when(coalesce(col("Education"), col("Edu")).isin("NA"), None)
            .otherwise(coalesce(col("Education"), col("Edu")))
        )
        # 4. Unify marital status
        .withColumn("marital_status",
            coalesce(col("Marital"), col("Marital_status"))
        )
        # 5. Unify city
        .withColumn("city",
            initcap(trim(coalesce(col("City"), col("City_in_state"))))
        )
        # 6. Standardize remaining columns
        .withColumn("state", upper(trim(col("State"))))
        .withColumn("job", initcap(trim(col("Job"))))
        .withColumn("default_flag", col("Default").cast(IntegerType()))
        .withColumn("balance", col("Balance").cast(IntegerType()))
        .withColumn("hh_insurance", col("HHInsurance").cast(IntegerType()))
        .withColumn("car_loan", col("CarLoan").cast(IntegerType()))
        .select(
            "customer_id", "region", "state", "city", "job",
            "marital_status", "education", "default_flag",
            "balance", "hh_insurance", "car_loan",
            "_source_file", "_ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers Quarantine

# COMMAND ----------

@dlt.table(
    name="silver_customers_quarantine",
    comment="Customer records that failed quality checks. Preserved for compliance review.",
    table_properties={"quality": "quarantine"}
)
def silver_customers_quarantine():
    return (
        spark.readStream.table("`databricks-hackathon-insurance`.bronze.bronze_customers")
        .withColumn("customer_id",
            coalesce(col("CustomerID"), col("Customer_ID"), col("cust_id")).cast(IntegerType())
        )
        .withColumn("region",
            when(coalesce(col("Region"), col("Reg")).isin("W", "w", "west"), "West")
            .when(coalesce(col("Region"), col("Reg")).isin("E", "e", "east"), "East")
            .when(coalesce(col("Region"), col("Reg")).isin("S", "s", "south"), "South")
            .when(coalesce(col("Region"), col("Reg")).isin("C", "c", "central"), "Central")
            .when(coalesce(col("Region"), col("Reg")).isin("N", "n", "north"), "North")
            .otherwise(coalesce(col("Region"), col("Reg")))
        )
        .filter(
            (col("customer_id").isNull()) |
            (~col("region").isin("East", "West", "North", "South", "Central"))
        )
        .withColumn("quarantine_reason",
            when(col("customer_id").isNull(), "missing_customer_id")
            .when(~col("region").isin("East", "West", "North", "South", "Central"), "invalid_region")
            .otherwise("unknown")
        )
        .withColumn("quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Silver Claims
# MAGIC Fixes the messiest table in the dataset:
# MAGIC - Corrupted dates (Excel serial fragments like "27:00.0") → proper dates
# MAGIC - String "NULL" / "?" → actual null
# MAGIC - Casts amounts to proper types
# MAGIC - Computes processing_days and sla_breach (>7 days)

# COMMAND ----------

@dlt.table(
    name="silver_claims",
    comment="Cleaned claims data. Dates fixed, NULLs resolved, SLA breach flag added.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect("valid_injury_amount", "injury_amount >= 0")
@dlt.expect("valid_property_amount", "property_amount >= 0")
@dlt.expect("valid_vehicle_amount", "vehicle_amount >= 0")
def silver_claims():
    df = spark.readStream.table("`databricks-hackathon-insurance`.bronze.bronze_claims")

    def fix_date_col(column_name):
        """Fix corrupted Excel serial date fragments like '27:00.0' → proper date."""
        return (
            when(
                (col(column_name).isNull()) |
                (col(column_name) == "NULL") |
                (col(column_name) == "?") |
                (trim(col(column_name)) == ""),
                None
            ).otherwise(
                coalesce(
                    expr(f"try_to_date(`{column_name}`, 'yyyy-MM-dd')"),
                    expr(f"try_to_date(`{column_name}`, 'MM/dd/yyyy')"),
                    date_add(
                        lit("1899-12-30").cast(DateType()),
                        regexp_extract(col(column_name), r"^(\d+)", 1).cast(IntegerType())
                    )
                )
            )
        )

    def clean_null(column_name):
        return when(
            (col(column_name) == "NULL") | (col(column_name) == "?") | (trim(col(column_name)) == ""),
            None
        ).otherwise(col(column_name))

    return (
        df
        .withColumn("claim_id", col("ClaimID").cast(IntegerType()))
        .withColumn("policy_id", col("PolicyID").cast(IntegerType()))
        .withColumn("incident_date", fix_date_col("incident_date"))
        .withColumn("claim_logged_on", fix_date_col("Claim_Logged_On"))
        .withColumn("claim_processed_on", fix_date_col("Claim_Processed_On"))
        .withColumn("injury_amount", col("injury").cast(DoubleType()))
        .withColumn("property_amount", col("property").cast(DoubleType()))
        .withColumn("vehicle_amount", col("vehicle").cast(DoubleType()))
        .withColumn("total_claim_amount",
            coalesce(col("injury_amount"), lit(0.0)) +
            coalesce(col("property_amount"), lit(0.0)) +
            coalesce(col("vehicle_amount"), lit(0.0))
        )
        # Business metrics
        .withColumn("processing_days",
            datediff(col("claim_processed_on"), col("claim_logged_on"))
        )
        .withColumn("sla_breach",
            when(col("processing_days") > 7, True).otherwise(False)
        )
        .withColumn("bodily_injuries", col("bodily_injuries").cast(IntegerType()))
        .withColumn("witnesses", col("witnesses").cast(IntegerType()))
        .withColumn("num_vehicles_involved", col("number_of_vehicles_involved").cast(IntegerType()))
        .withColumn("property_damage", clean_null("property_damage"))
        .withColumn("police_report_available", clean_null("police_report_available"))
        .withColumn("collision_type", clean_null("collision_type"))
        .withColumn("authorities_contacted", clean_null("authorities_contacted"))
        .withColumn("incident_severity", initcap(trim(col("incident_severity"))))
        .withColumn("incident_type", initcap(trim(col("incident_type"))))
        .withColumn("claim_rejected",
            when(upper(trim(col("Claim_Rejected"))).isin("Y", "YES", "TRUE"), "Y")
            .when(upper(trim(col("Claim_Rejected"))).isin("N", "NO", "FALSE"), "N")
            .otherwise(col("Claim_Rejected"))
        )
        .select(
            "claim_id", "policy_id",
            "incident_date", "claim_logged_on", "claim_processed_on",
            "processing_days", "sla_breach",
            "incident_state", "incident_city", "incident_location",
            "incident_type", "collision_type", "incident_severity",
            "authorities_contacted", "property_damage", "police_report_available",
            "injury_amount", "property_amount", "vehicle_amount", "total_claim_amount",
            "bodily_injuries", "witnesses", "num_vehicles_involved",
            "claim_rejected",
            "_source_file", "_ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims Quarantine

# COMMAND ----------

@dlt.table(
    name="silver_claims_quarantine",
    comment="Claims records that failed quality checks. Preserved for compliance review.",
    table_properties={"quality": "quarantine"}
)
def silver_claims_quarantine():
    return (
        spark.readStream.table("`databricks-hackathon-insurance`.bronze.bronze_claims")
        .withColumn("claim_id", col("ClaimID").cast(IntegerType()))
        .withColumn("policy_id", col("PolicyID").cast(IntegerType()))
        .withColumn("injury_amount", col("injury").cast(DoubleType()))
        .withColumn("property_amount", col("property").cast(DoubleType()))
        .withColumn("vehicle_amount", col("vehicle").cast(DoubleType()))
        .filter(
            (col("claim_id").isNull()) |
            (col("policy_id").isNull()) |
            (col("injury_amount") < 0) |
            (col("property_amount") < 0) |
            (col("vehicle_amount") < 0)
        )
        .withColumn("quarantine_reason",
            when(col("claim_id").isNull(), "missing_claim_id")
            .when(col("policy_id").isNull(), "missing_policy_id")
            .when(col("injury_amount") < 0, "negative_injury")
            .when(col("property_amount") < 0, "negative_property")
            .when(col("vehicle_amount") < 0, "negative_vehicle")
            .otherwise("unknown")
        )
        .withColumn("quarantine_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Silver Policy

# COMMAND ----------

@dlt.table(
    name="silver_policy",
    comment="Cleaned policy data. Column names standardized, types cast.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_policy_number", "policy_number IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_car_id", "car_id IS NOT NULL")
@dlt.expect("valid_premium", "policy_annual_premium > 0")
def silver_policy():
    return (
        spark.readStream.table("`databricks-hackathon-insurance`.bronze.bronze_policy")
        .withColumn("policy_number", col("policy_number").cast(IntegerType()))
        .withColumn("policy_bind_date",
            coalesce(
                expr("try_to_date(policy_bind_date, 'yyyy-MM-dd')"),
                expr("try_to_date(policy_bind_date, 'MM/dd/yyyy')"),
                expr("try_to_date(policy_bind_date, 'dd/MM/yyyy')"),
            )
        )
        .withColumn("policy_state", upper(trim(col("policy_state"))))
        .withColumn("policy_csl", trim(col("policy_csl")))
        .withColumn("policy_deductible", col("policy_deductable").cast(IntegerType()))
        .withColumn("policy_annual_premium", col("policy_annual_premium").cast(DoubleType()))
        .withColumn("umbrella_limit", col("umbrella_limit").cast(IntegerType()))
        .withColumn("car_id", col("car_id").cast(IntegerType()))
        .withColumn("customer_id", col("customer_id").cast(IntegerType()))
        .select(
            "policy_number", "policy_bind_date", "policy_state",
            "policy_csl", "policy_deductible", "policy_annual_premium",
            "umbrella_limit", "car_id", "customer_id",
            "_source_file", "_ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Silver Sales

# COMMAND ----------

@dlt.table(
    name="silver_sales",
    comment="Cleaned sales data. Dates parsed, days_on_lot and aging_flag added.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_sales_id", "sales_id IS NOT NULL")
@dlt.expect_or_drop("valid_car_id", "car_id IS NOT NULL")
@dlt.expect("valid_price", "selling_price > 0")
def silver_sales():
    return (
        spark.readStream.table("`databricks-hackathon-insurance`.bronze.bronze_sales")
        .withColumn("sales_id", col("sales_id").cast(IntegerType()))
        .withColumn("ad_placed_on",
            coalesce(
                expr("try_to_date(ad_placed_on, 'dd-MM-yyyy HH:mm')"),
                expr("try_to_date(ad_placed_on, 'yyyy-MM-dd')"),
                expr("try_to_date(ad_placed_on, 'MM/dd/yyyy')"),
            )
        )
        .withColumn("sold_on",
            when(
                (col("sold_on").isNull()) | (trim(col("sold_on")) == "") | (col("sold_on") == "NULL"),
                None
            ).otherwise(
                coalesce(
                    expr("try_to_date(sold_on, 'dd-MM-yyyy HH:mm')"),
                    expr("try_to_date(sold_on, 'yyyy-MM-dd')"),
                )
            )
        )
        .withColumn("selling_price", col("original_selling_price").cast(DoubleType()))
        .withColumn("region", initcap(trim(col("Region"))))
        .withColumn("state", upper(trim(col("State"))))
        .withColumn("city", initcap(trim(col("City"))))
        .withColumn("car_id", col("car_id").cast(IntegerType()))
        .withColumn("is_sold", col("sold_on").isNotNull())
        # Business metrics
        .withColumn("days_on_lot",
            when(col("sold_on").isNotNull(),
                datediff(col("sold_on"), col("ad_placed_on"))
            ).otherwise(
                datediff(current_date(), col("ad_placed_on"))
            )
        )
        .withColumn("aging_flag",
            when((col("sold_on").isNull()) & (datediff(current_date(), col("ad_placed_on")) > 90), "CRITICAL")
            .when((col("sold_on").isNull()) & (datediff(current_date(), col("ad_placed_on")) > 60), "AGING")
            .otherwise("OK")
        )
        .select(
            "sales_id", "ad_placed_on", "sold_on", "is_sold",
            "selling_price", "region", "state", "city",
            "seller_type", "owner", "car_id",
            "days_on_lot", "aging_flag",
            "_source_file", "_ingested_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Silver Cars

# COMMAND ----------

@dlt.table(
    name="silver_cars",
    comment="Cleaned vehicle catalogue. Numeric values extracted from unit strings.",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_car_id", "car_id IS NOT NULL")
@dlt.expect("valid_km_driven", "km_driven >= 0")
def silver_cars():
    return (
        spark.readStream.table("`databricks-hackathon-insurance`.bronze.bronze_cars")
        .withColumn("car_id", col("car_id").cast(IntegerType()))
        .withColumn("name", trim(col("name")))
        .withColumn("km_driven", col("km_driven").cast(IntegerType()))
        .withColumn("fuel", initcap(trim(col("fuel"))))
        .withColumn("transmission", initcap(trim(col("transmission"))))
        .withColumn("mileage_kmpl",
            regexp_extract(col("mileage"), r"([\d.]+)", 1).cast(DoubleType())
        )
        .withColumn("engine_cc",
            regexp_extract(col("engine"), r"(\d+)", 1).cast(IntegerType())
        )
        .withColumn("max_power_bhp",
            regexp_extract(col("max_power"), r"([\d.]+)", 1).cast(DoubleType())
        )
        .withColumn("torque", col("torque"))
        .withColumn("seats", col("seats").cast(IntegerType()))
        .withColumn("model", trim(col("model")))
        .select(
            "car_id", "name", "km_driven", "fuel", "transmission",
            "mileage_kmpl", "engine_cc", "max_power_bhp", "torque",
            "seats", "model",
            "_source_file", "_ingested_at"
        )
    )
