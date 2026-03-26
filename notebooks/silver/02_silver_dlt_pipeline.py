# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Data Quality, Harmonization & Quarantine Pipeline
# MAGIC
# MAGIC This DLT pipeline reads from Bronze tables, applies transformations
# MAGIC (column standardization, value normalization, type casting), enforces
# MAGIC quality rules via DLT Expectations, and routes failed records to
# MAGIC quarantine tables.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - primeins.silver.customers (harmonized from 7 CSV files)
# MAGIC - primeins.silver.claims (type-cast, nulls cleaned from 2 JSON files)
# MAGIC - primeins.silver.policy (validated)
# MAGIC - primeins.silver.sales (dates parsed, empty rows filtered)
# MAGIC - primeins.silver.cars (unit strings stripped)
# MAGIC - primeins.silver.quarantine_customers
# MAGIC - primeins.silver.quarantine_claims
# MAGIC - primeins.silver.quarantine_policy
# MAGIC - primeins.silver.quarantine_sales
# MAGIC - primeins.silver.quarantine_cars
# MAGIC - primeins.silver.dq_issues (quality issues log)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CUSTOMERS ==========
# MAGIC
# MAGIC Issues found in Bronze:
# MAGIC - 3 customer ID column names: CustomerID, Customer_ID, cust_id
# MAGIC - 2 region column names: Region, Reg; abbreviations W/C/E/S in file 5
# MAGIC - 2 marital column names: Marital_status, Marital; swapped with Education in file 6
# MAGIC - 2 education column names: Education, Edu; "terto" typo in file 5; "NA" strings
# MAGIC - 2 city column names: City, City_in_state
# MAGIC - Job missing from file 1; HHInsurance missing from file 2; Education missing from file 4

# COMMAND ----------

@dlt.view(
    name="customers_transformed",
    comment="Intermediate: harmonized columns, standardized values from 7 regional files"
)
def customers_transformed():
    df = spark.read.table("primeins.bronze.customers")

    # --- Column harmonization ---

    # 3 different customer ID columns -> customer_id
    df = df.withColumn(
        "customer_id",
        F.coalesce(
            F.col("CustomerID").cast("int"),
            F.col("Customer_ID").cast("int"),
            F.col("cust_id").cast("int")
        )
    )

    # 2 region column names -> region
    df = df.withColumn(
        "region",
        F.coalesce(F.col("Region"), F.col("Reg"))
    )

    # Standardize region abbreviations from customers_5.csv
    # W -> West, C -> Central, E -> East, S -> South, N -> North
    df = df.withColumn(
        "region",
        F.when(F.col("region") == "W", "West")
         .when(F.col("region") == "C", "Central")
         .when(F.col("region") == "E", "East")
         .when(F.col("region") == "S", "South")
         .when(F.col("region") == "N", "North")
         .otherwise(F.col("region"))
    )

    # customers_6.csv: Marital_status and Education columns are SWAPPED
    # Marital_status contains education values (primary/secondary/tertiary)
    # Education contains marital values (divorced/married/single)
    # For file 6: read Education column for marital, Marital_status for education
    df = df.withColumn(
        "marital",
        F.when(
            F.col("_source_file").contains("customers_6"),
            F.coalesce(F.col("Education"), F.col("Marital_status"))
        ).otherwise(
            F.coalesce(F.col("Marital_status"), F.col("Marital"))
        )
    )

    df = df.withColumn(
        "education",
        F.when(
            F.col("_source_file").contains("customers_6"),
            F.coalesce(F.col("Marital_status"), F.col("Education"))
        ).otherwise(
            F.coalesce(F.col("Education"), F.col("Edu"))
        )
    )

    # Fix typo: "terto" -> "tertiary" (73 rows in customers_5.csv)
    df = df.withColumn(
        "education",
        F.when(F.col("education") == "terto", "tertiary")
         .otherwise(F.col("education"))
    )

    # Convert "NA" strings to actual null (118+ rows across files)
    df = df.withColumn(
        "education",
        F.when(F.col("education") == "NA", F.lit(None))
         .otherwise(F.col("education"))
    )
    df = df.withColumn(
        "marital",
        F.when(F.col("marital") == "NA", F.lit(None))
         .otherwise(F.col("marital"))
    )

    # 2 city column names -> city
    df = df.withColumn(
        "city",
        F.coalesce(F.col("City"), F.col("City_in_state"))
    )

    # Select standardized columns with consistent names and types
    return df.select(
        F.col("customer_id"),
        F.col("region"),
        F.col("State").alias("state"),
        F.col("city"),
        F.col("Job").alias("job"),
        F.col("marital"),
        F.col("education"),
        F.col("Default").cast("int").alias("default_flag"),
        F.col("Balance").cast("int").alias("balance"),
        F.col("HHInsurance").cast("int").alias("hh_insurance"),
        F.col("CarLoan").cast("int").alias("car_loan"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


# Clean customers table: only records passing all quality rules
@dlt.table(
    name="customers",
    comment="Cleaned, harmonized customer data. Failed records in quarantine_customers.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_region", "region IN ('East', 'West', 'Central', 'South', 'North')")
def customers_clean():
    return dlt.read("customers_transformed")


# Quarantine: customer records that failed any quality rule
@dlt.table(
    name="quarantine_customers",
    comment="Customer records that failed validation. Includes _rejection_reason for compliance.",
    table_properties={"quality": "quarantine"},
)
def quarantine_customers():
    df = dlt.read("customers_transformed")

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws("; ",
            F.when(F.col("customer_id").isNull(), F.lit("customer_id IS NULL")),
            F.when(
                ~F.col("region").isin("East", "West", "Central", "South", "North"),
                F.lit("invalid region value")
            )
        )
    )

    return df.filter(
        (F.col("customer_id").isNull()) |
        (~F.col("region").isin("East", "West", "Central", "South", "North"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CLAIMS ==========
# MAGIC
# MAGIC Issues found in Bronze:
# MAGIC - All 21 columns are strings (JSON source). Need type casting.
# MAGIC - Date fields corrupted: "27:00.0" format. Dates lost, only time portion remains.
# MAGIC - String "NULL" in Claim_Processed_On (526 rows), Claim_Logged_On (11 rows), vehicle (29 rows)
# MAGIC - "?" placeholder in property_damage (360), police_report_available (343), collision_type (178)

# COMMAND ----------

@dlt.view(
    name="claims_transformed",
    comment="Intermediate: types cast, NULL/? strings cleaned, dates preserved as-is (corrupted at source)"
)
def claims_transformed():
    df = spark.read.table("primeins.bronze.claims")

    # --- Clean placeholder values ---

    # Replace string "NULL" with actual null across all columns
    for col_name in ["Claim_Processed_On", "Claim_Logged_On", "vehicle",
                      "incident_date"]:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "NULL", F.lit(None))
             .otherwise(F.col(col_name))
        )

    # Replace "?" with actual null
    for col_name in ["property_damage", "police_report_available", "collision_type"]:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "?", F.lit(None))
             .otherwise(F.col(col_name))
        )

    # --- Type casting ---
    # Numeric fields stored as strings in JSON -> proper types
    return df.select(
        F.col("ClaimID").alias("claim_id"),
        F.col("PolicyID").alias("policy_id"),
        F.col("incident_date"),          # corrupted at source, kept as string
        F.col("incident_state"),
        F.col("incident_city"),
        F.col("incident_location"),
        F.col("incident_type"),
        F.col("collision_type"),
        F.col("incident_severity"),
        F.col("injury").cast("double").alias("injury_amount"),
        F.col("property").cast("double").alias("property_amount"),
        F.col("vehicle").cast("double").alias("vehicle_amount"),
        F.col("authorities_contacted"),
        F.col("number_of_vehicles_involved").cast("int").alias("vehicles_involved"),
        F.col("property_damage"),
        F.col("bodily_injuries").cast("int"),
        F.col("witnesses").cast("int"),
        F.col("police_report_available"),
        F.col("Claim_Rejected").alias("claim_rejected"),
        F.col("Claim_Logged_On").alias("claim_logged_on"),
        F.col("Claim_Processed_On").alias("claim_processed_on"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


# Clean claims table
@dlt.table(
    name="claims",
    comment="Cleaned claims data. Types cast, NULL/? strings replaced. Failed records in quarantine_claims.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect_or_drop("valid_rejection_status", "claim_rejected IN ('Y', 'N')")
@dlt.expect("non_negative_injury", "injury_amount >= 0 OR injury_amount IS NULL")
@dlt.expect("non_negative_property", "property_amount >= 0 OR property_amount IS NULL")
@dlt.expect("non_negative_vehicle", "vehicle_amount >= 0 OR vehicle_amount IS NULL")
def claims_clean():
    return dlt.read("claims_transformed")


# Quarantine: claims that failed quality rules
@dlt.table(
    name="quarantine_claims",
    comment="Claims records that failed validation with rejection reasons.",
    table_properties={"quality": "quarantine"},
)
def quarantine_claims():
    df = dlt.read("claims_transformed")

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws("; ",
            F.when(F.col("claim_id").isNull(), F.lit("claim_id IS NULL")),
            F.when(F.col("policy_id").isNull(), F.lit("policy_id IS NULL")),
            F.when(
                ~F.col("claim_rejected").isin("Y", "N"),
                F.lit("invalid claim_rejected value")
            )
        )
    )

    return df.filter(
        (F.col("claim_id").isNull()) |
        (F.col("policy_id").isNull()) |
        (~F.col("claim_rejected").isin("Y", "N"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== POLICY ==========
# MAGIC
# MAGIC Issues found in Bronze:
# MAGIC - 1 row with negative umbrella_limit (-1,000,000)
# MAGIC - Column name misspelling: policy_deductable (preserved from source)
# MAGIC - Otherwise clean: proper types, no null keys, valid policy_csl values

# COMMAND ----------

@dlt.view(
    name="policy_transformed",
    comment="Intermediate: policy data with standardized column names"
)
def policy_transformed():
    df = spark.read.table("primeins.bronze.policy")

    return df.select(
        F.col("policy_number"),
        F.col("policy_bind_date"),
        F.col("policy_state"),
        F.col("policy_csl"),
        F.col("policy_deductable").alias("policy_deductible"),  # fix spelling
        F.col("policy_annual_premium"),
        F.col("umbrella_limit"),
        F.col("car_id").cast("int"),
        F.col("customer_id").cast("int"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


# Clean policy table
@dlt.table(
    name="policy",
    comment="Validated policy data. Deductible column spelling fixed. Failed records in quarantine_policy.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_policy_number", "policy_number IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_premium", "policy_annual_premium > 0")
@dlt.expect_or_drop("non_negative_umbrella", "umbrella_limit >= 0")
def policy_clean():
    return dlt.read("policy_transformed")


# Quarantine: policy records that failed rules
@dlt.table(
    name="quarantine_policy",
    comment="Policy records that failed validation with rejection reasons.",
    table_properties={"quality": "quarantine"},
)
def quarantine_policy():
    df = dlt.read("policy_transformed")

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws("; ",
            F.when(F.col("policy_number").isNull(), F.lit("policy_number IS NULL")),
            F.when(F.col("customer_id").isNull(), F.lit("customer_id IS NULL")),
            F.when(F.col("policy_annual_premium") <= 0, F.lit("premium <= 0")),
            F.when(F.col("umbrella_limit") < 0, F.lit("negative umbrella_limit"))
        )
    )

    return df.filter(
        (F.col("policy_number").isNull()) |
        (F.col("customer_id").isNull()) |
        (F.col("policy_annual_premium") <= 0) |
        (F.col("umbrella_limit") < 0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== SALES ==========
# MAGIC
# MAGIC Issues found in Bronze:
# MAGIC - 3,132 entirely empty rows (62.9%) with NULL sales_id (blank padding from source)
# MAGIC - Date columns stored as strings in "dd-MM-yyyy HH:mm" format
# MAGIC - NULL sold_on among valid rows = legitimate unsold inventory (not a data error)

# COMMAND ----------

@dlt.view(
    name="sales_transformed",
    comment="Intermediate: empty rows removed, dates parsed from string to timestamp"
)
def sales_transformed():
    df = spark.read.table("primeins.bronze.sales")

    # Parse date strings "dd-MM-yyyy HH:mm" to timestamps
    df = df.withColumn(
        "ad_placed_on",
        F.to_timestamp(F.col("ad_placed_on"), "dd-MM-yyyy HH:mm")
    )
    df = df.withColumn(
        "sold_on",
        F.to_timestamp(F.col("sold_on"), "dd-MM-yyyy HH:mm")
    )

    return df.select(
        F.col("sales_id").cast("int"),
        F.col("ad_placed_on"),
        F.col("sold_on"),
        F.col("original_selling_price").cast("double"),
        F.col("Region").alias("region"),
        F.col("State").alias("state"),
        F.col("City").alias("city"),
        F.col("seller_type"),
        F.col("owner"),
        F.col("car_id").cast("int"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


# Clean sales table
@dlt.table(
    name="sales",
    comment="Cleaned sales data. Empty padding rows dropped. Dates parsed. Failed records in quarantine_sales.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_sales_id", "sales_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "original_selling_price > 0")
@dlt.expect_or_drop("valid_ad_date", "ad_placed_on IS NOT NULL")
def sales_clean():
    return dlt.read("sales_transformed")


# Quarantine: sales records that failed rules
@dlt.table(
    name="quarantine_sales",
    comment="Sales records that failed validation. Includes 3132 blank padding rows.",
    table_properties={"quality": "quarantine"},
)
def quarantine_sales():
    df = dlt.read("sales_transformed")

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws("; ",
            F.when(F.col("sales_id").isNull(), F.lit("sales_id IS NULL (empty row)")),
            F.when(
                (F.col("original_selling_price") <= 0) & (F.col("sales_id").isNotNull()),
                F.lit("price <= 0")
            ),
            F.when(
                F.col("ad_placed_on").isNull() & F.col("sales_id").isNotNull(),
                F.lit("ad_placed_on is NULL")
            )
        )
    )

    return df.filter(
        (F.col("sales_id").isNull()) |
        (F.col("original_selling_price") <= 0) |
        (F.col("ad_placed_on").isNull())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CARS ==========
# MAGIC
# MAGIC Issues found in Bronze:
# MAGIC - mileage: "23.4 kmpl" / "17.3 km/kg" (unit strings embedded)
# MAGIC - engine: "1248 CC" (unit suffix)
# MAGIC - max_power: "74 bhp" (unit suffix)
# MAGIC - torque: wildly inconsistent formats ("190Nm@ 2000rpm", "22.4 kgm at 1750-2750rpm")
# MAGIC - No null car_ids, no duplicates, no negative km_driven

# COMMAND ----------

@dlt.view(
    name="cars_transformed",
    comment="Intermediate: unit strings stripped from mileage/engine/max_power, torque kept as-is"
)
def cars_transformed():
    df = spark.read.table("primeins.bronze.cars")

    # Strip unit strings from mileage: "23.4 kmpl" -> 23.4
    # Handle both "kmpl" and "km/kg" units
    df = df.withColumn(
        "mileage_value",
        F.regexp_extract(F.col("mileage"), r"^([\d.]+)", 1).cast("double")
    )
    df = df.withColumn(
        "mileage_unit",
        F.regexp_extract(F.col("mileage"), r"[\d.]+\s*(.*)", 1)
    )

    # Strip " CC" from engine: "1248 CC" -> 1248
    df = df.withColumn(
        "engine_cc",
        F.regexp_extract(F.col("engine"), r"^(\d+)", 1).cast("int")
    )

    # Strip " bhp" from max_power: "74 bhp" -> 74.0
    df = df.withColumn(
        "max_power_bhp",
        F.regexp_extract(F.col("max_power"), r"^([\d.]+)", 1).cast("double")
    )

    # Torque has too many inconsistent formats (Nm vs kgm, various separators)
    # We keep it as the original string rather than risk bad parsing
    return df.select(
        F.col("car_id").cast("int"),
        F.col("name"),
        F.col("km_driven"),
        F.col("fuel"),
        F.col("transmission"),
        F.col("mileage_value").alias("mileage"),
        F.col("mileage_unit"),
        F.col("engine_cc").alias("engine"),
        F.col("max_power_bhp").alias("max_power"),
        F.col("torque"),           # kept as original string
        F.col("seats"),
        F.col("model"),
        F.col("_source_file"),
        F.col("_load_timestamp")
    )


# Clean cars table
@dlt.table(
    name="cars",
    comment="Cleaned cars data. Unit strings stripped from mileage/engine/max_power. Failed records in quarantine_cars.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_car_id", "car_id IS NOT NULL")
@dlt.expect_or_drop("non_negative_km", "km_driven >= 0")
def cars_clean():
    return dlt.read("cars_transformed")


# Quarantine: cars records that failed rules
@dlt.table(
    name="quarantine_cars",
    comment="Car records that failed validation with rejection reasons.",
    table_properties={"quality": "quarantine"},
)
def quarantine_cars():
    df = dlt.read("cars_transformed")

    df = df.withColumn(
        "_rejection_reason",
        F.concat_ws("; ",
            F.when(F.col("car_id").isNull(), F.lit("car_id IS NULL")),
            F.when(F.col("km_driven") < 0, F.lit("negative km_driven"))
        )
    )

    return df.filter(
        (F.col("car_id").isNull()) |
        (F.col("km_driven") < 0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== DQ ISSUES LOG ==========
# MAGIC
# MAGIC Aggregated quality metrics: what was found, where, how many records affected.
# MAGIC The compliance team checks this table for a summary before diving into
# MAGIC individual quarantine records.

# COMMAND ----------

@dlt.table(
    name="dq_issues",
    comment="Quality issues log. One row per rule violation type per table, with counts and severity.",
    table_properties={"quality": "silver"},
)
def dq_issues():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

    issues = []

    # --- Customers DQ checks ---
    cust_df = dlt.read("customers_transformed")
    total_cust = cust_df.count()

    null_id = cust_df.filter(F.col("customer_id").isNull()).count()
    if null_id > 0:
        issues.append(("customers", "customer_id", "null_customer_id",
                       "customer_id IS NULL", "drop", "high",
                       null_id, round(null_id / total_cust, 4) if total_cust > 0 else 0,
                       "Coalesce CustomerID/Customer_ID/cust_id failed - record has no ID in any variant"))

    invalid_region = cust_df.filter(
        ~F.col("region").isin("East", "West", "Central", "South", "North")
    ).count()
    if invalid_region > 0:
        issues.append(("customers", "region", "invalid_region",
                       "region NOT IN (East,West,Central,South,North)", "drop", "high",
                       invalid_region, round(invalid_region / total_cust, 4) if total_cust > 0 else 0,
                       "Region value not recognized after standardizing W/C/E/S abbreviations"))

    # Count terto fixes applied
    raw_cust = spark.read.table("primeins.bronze.customers")
    terto_count = raw_cust.filter(
        F.coalesce(F.col("Education"), F.col("Edu")) == "terto"
    ).count()
    if terto_count > 0:
        issues.append(("customers", "education", "typo_terto",
                       "education == 'terto'", "transform", "medium",
                       terto_count, round(terto_count / total_cust, 4) if total_cust > 0 else 0,
                       "Typo in customers_5.csv: terto corrected to tertiary"))

    # Count NA string conversions
    na_count = raw_cust.filter(
        (F.coalesce(F.col("Education"), F.col("Edu")) == "NA") |
        (F.coalesce(F.col("Marital_status"), F.col("Marital")) == "NA")
    ).count()
    if na_count > 0:
        issues.append(("customers", "education/marital", "string_na_to_null",
                       "value == 'NA'", "transform", "medium",
                       na_count, round(na_count / total_cust, 4) if total_cust > 0 else 0,
                       "String NA converted to actual null"))

    # Count swapped columns in file 6
    swap_count = raw_cust.filter(
        F.col("_source_file").contains("customers_6")
    ).count()
    if swap_count > 0:
        issues.append(("customers", "marital/education", "swapped_columns",
                       "_source_file contains customers_6", "transform", "critical",
                       swap_count, round(swap_count / total_cust, 4) if total_cust > 0 else 0,
                       "customers_6.csv: Marital_status and Education columns swapped - corrected"))

    # --- Claims DQ checks ---
    claims_df = dlt.read("claims_transformed")
    total_claims = claims_df.count()

    null_claim_id = claims_df.filter(F.col("claim_id").isNull()).count()
    if null_claim_id > 0:
        issues.append(("claims", "claim_id", "null_claim_id",
                       "claim_id IS NULL", "drop", "high",
                       null_claim_id, round(null_claim_id / total_claims, 4) if total_claims > 0 else 0,
                       "Claim has no identifier"))

    null_policy_id = claims_df.filter(F.col("policy_id").isNull()).count()
    if null_policy_id > 0:
        issues.append(("claims", "policy_id", "null_policy_id",
                       "policy_id IS NULL", "drop", "high",
                       null_policy_id, round(null_policy_id / total_claims, 4) if total_claims > 0 else 0,
                       "Claim has no policy reference"))

    # Count NULL string and ? conversions from raw Bronze
    raw_claims = spark.read.table("primeins.bronze.claims")
    null_str = raw_claims.filter(
        (F.col("Claim_Processed_On") == "NULL") |
        (F.col("Claim_Logged_On") == "NULL") |
        (F.col("vehicle") == "NULL")
    ).count()
    if null_str > 0:
        issues.append(("claims", "multiple", "string_null_to_null",
                       "value == 'NULL'", "transform", "medium",
                       null_str, round(null_str / total_claims, 4) if total_claims > 0 else 0,
                       "String NULL converted to actual null in Claim_Processed_On, Claim_Logged_On, vehicle"))

    q_mark = raw_claims.filter(
        (F.col("property_damage") == "?") |
        (F.col("police_report_available") == "?") |
        (F.col("collision_type") == "?")
    ).count()
    if q_mark > 0:
        issues.append(("claims", "multiple", "question_mark_to_null",
                       "value == '?'", "transform", "medium",
                       q_mark, round(q_mark / total_claims, 4) if total_claims > 0 else 0,
                       "? placeholder converted to null in property_damage, police_report_available, collision_type"))

    # --- Policy DQ checks ---
    policy_df = dlt.read("policy_transformed")
    total_policy = policy_df.count()

    neg_umbrella = policy_df.filter(F.col("umbrella_limit") < 0).count()
    if neg_umbrella > 0:
        issues.append(("policy", "umbrella_limit", "negative_umbrella",
                       "umbrella_limit < 0", "drop", "medium",
                       neg_umbrella, round(neg_umbrella / total_policy, 4) if total_policy > 0 else 0,
                       "Negative umbrella limit value (-1000000) found"))

    # --- Sales DQ checks ---
    sales_df = dlt.read("sales_transformed")
    total_sales = sales_df.count()

    null_sales_id = sales_df.filter(F.col("sales_id").isNull()).count()
    if null_sales_id > 0:
        issues.append(("sales", "sales_id", "null_sales_id",
                       "sales_id IS NULL", "drop", "critical",
                       null_sales_id, round(null_sales_id / total_sales, 4) if total_sales > 0 else 0,
                       "Empty padding rows from source files - entire row is NULL"))

    # --- Build output dataframe ---
    schema = StructType([
        StructField("table_name", StringType()),
        StructField("column_name", StringType()),
        StructField("rule_name", StringType()),
        StructField("rule_condition", StringType()),
        StructField("action_taken", StringType()),
        StructField("severity", StringType()),
        StructField("affected_records", IntegerType()),
        StructField("affected_ratio", DoubleType()),
        StructField("suggested_fix", StringType()),
    ])

    df = spark.createDataFrame(issues, schema)
    df = df.withColumn("detected_at", F.current_timestamp())

    return df
