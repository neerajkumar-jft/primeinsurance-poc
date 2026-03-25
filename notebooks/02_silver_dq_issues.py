# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver DQ Issues Table
# MAGIC Scans Bronze and Silver data to catalog every data quality issue found.
# MAGIC This table feeds **Gen AI UC1 (DQ Explainer)** — the LLM translates
# MAGIC these technical findings into plain English for the compliance team.
# MAGIC
# MAGIC **Output**: ``databricks-hackathon-insurance`.silver.dq_issues`

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp, count, when, sum as _sum,
    coalesce, round as _round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the DQ Issues schema

# COMMAND ----------

schema = StructType([
    StructField("issue_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("rule_name", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("affected_records", IntegerType(), True),
    StructField("total_records", IntegerType(), True),
    StructField("affected_ratio", DoubleType(), True),
    StructField("issue_description", StringType(), False),
    StructField("suggested_fix", StringType(), True),
    StructField("detected_at", TimestampType(), False)
])

issues = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Customers — Schema Variations & Value Issues

# COMMAND ----------

bronze_customers = spark.read.table("`databricks-hackathon-insurance`.bronze.bronze_customers")
silver_customers = spark.read.table("`databricks-hackathon-insurance`.silver.silver_customers")
total_cust = bronze_customers.count()

# --- Issue 1: Schema variation — 3 different customer ID column names ---
cols_with_data = bronze_customers.select(
    _sum(when(col("CustomerID").isNotNull(), 1).otherwise(0)).alias("CustomerID_count"),
    _sum(when(col("Customer_ID").isNotNull(), 1).otherwise(0)).alias("Customer_ID_count"),
    _sum(when(col("cust_id").isNotNull(), 1).otherwise(0)).alias("cust_id_count")
).collect()[0]
issues.append(("DQ-CUST-001", "bronze_customers", "CustomerID, Customer_ID, cust_id",
    "schema_variation", "HIGH", total_cust, total_cust, 1.0,
    f"Customer ID spread across 3 different column names: CustomerID ({cols_with_data['CustomerID_count']} rows), Customer_ID ({cols_with_data['Customer_ID_count']} rows), cust_id ({cols_with_data['cust_id_count']} rows). Regional files used inconsistent naming.",
    "Unified into single 'customer_id' column using COALESCE(CustomerID, Customer_ID, cust_id)"))

# --- Issue 2: Schema variation — 2 region column names ---
reg_counts = bronze_customers.select(
    _sum(when(col("Region").isNotNull(), 1).otherwise(0)).alias("Region_count"),
    _sum(when(col("Reg").isNotNull(), 1).otherwise(0)).alias("Reg_count")
).collect()[0]
issues.append(("DQ-CUST-002", "bronze_customers", "Region, Reg",
    "schema_variation", "HIGH", total_cust, total_cust, 1.0,
    f"Region stored in 2 different column names: Region ({reg_counts['Region_count']} rows), Reg ({reg_counts['Reg_count']} rows).",
    "Unified into single 'region' column using COALESCE(Region, Reg)"))

# --- Issue 3: Region abbreviation inconsistency ---
abbrev_count = bronze_customers.filter(
    coalesce(col("Region"), col("Reg")).isin("W", "E", "S", "C")
).count()
issues.append(("DQ-CUST-003", "bronze_customers", "region",
    "value_inconsistency", "MEDIUM", abbrev_count, total_cust,
    round(abbrev_count / total_cust, 4),
    f"{abbrev_count} records used single-letter abbreviations (W, E, S, C) instead of full region names (West, East, South, Central).",
    "Normalized abbreviations to full names: W→West, E→East, S→South, C→Central"))

# --- Issue 4: Schema variation — 2 education column names ---
edu_counts = bronze_customers.select(
    _sum(when(col("Education").isNotNull(), 1).otherwise(0)).alias("Education_count"),
    _sum(when(col("Edu").isNotNull(), 1).otherwise(0)).alias("Edu_count")
).collect()[0]
issues.append(("DQ-CUST-004", "bronze_customers", "Education, Edu",
    "schema_variation", "MEDIUM", total_cust, total_cust, 1.0,
    f"Education stored in 2 different column names: Education ({edu_counts['Education_count']} rows), Edu ({edu_counts['Edu_count']} rows).",
    "Unified into single 'education' column using COALESCE(Education, Edu)"))

# --- Issue 5: Education typo — 'terto' instead of 'tertiary' ---
terto_count = bronze_customers.filter(
    coalesce(col("Education"), col("Edu")) == "terto"
).count()
issues.append(("DQ-CUST-005", "bronze_customers", "education",
    "value_typo", "MEDIUM", terto_count, total_cust,
    round(terto_count / total_cust, 4) if total_cust > 0 else 0.0,
    f"{terto_count} records had education value 'terto' — a typo for 'tertiary'.",
    "Replaced 'terto' with 'tertiary' during Silver transformation"))

# --- Issue 6: Education sentinel 'NA' instead of null ---
na_count = bronze_customers.filter(
    coalesce(col("Education"), col("Edu")) == "NA"
).count()
issues.append(("DQ-CUST-006", "bronze_customers", "education",
    "sentinel_value", "LOW", na_count, total_cust,
    round(na_count / total_cust, 4) if total_cust > 0 else 0.0,
    f"{na_count} records had education = 'NA' (string) instead of actual null.",
    "Replaced string 'NA' with null in Silver layer"))

# --- Issue 7: Schema variation — 2 marital status column names ---
issues.append(("DQ-CUST-007", "bronze_customers", "Marital, Marital_status",
    "schema_variation", "MEDIUM", total_cust, total_cust, 1.0,
    "Marital status stored in 2 different column names: 'Marital' and 'Marital_status' across regional files.",
    "Unified into single 'marital_status' column using COALESCE(Marital, Marital_status)"))

# --- Issue 8: Schema variation — 2 city column names ---
issues.append(("DQ-CUST-008", "bronze_customers", "City, City_in_state",
    "schema_variation", "MEDIUM", total_cust, total_cust, 1.0,
    "City stored in 2 different column names: 'City' and 'City_in_state' across regional files.",
    "Unified into single 'city' column using COALESCE(City, City_in_state)"))

# --- Issue 9: Duplicate customers across files ---
unique_count = silver_customers.select("customer_id").distinct().count()
dup_count = total_cust - unique_count
issues.append(("DQ-CUST-009", "bronze_customers", "customer_id",
    "duplicate_records", "HIGH", dup_count, total_cust,
    round(dup_count / total_cust, 4) if total_cust > 0 else 0.0,
    f"{dup_count} duplicate customer records found across 7 source files. {total_cust} total records but only {unique_count} unique customer_ids. Same customers appear in multiple regional files.",
    "Deduplicated in Gold dim_customer using window function — keeps most complete record per customer_id"))

print(f"Customers: {len(issues)} issues found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Claims — Corrupted Dates & Sentinel Values

# COMMAND ----------

bronze_claims = spark.read.table("`databricks-hackathon-insurance`.bronze.bronze_claims")
total_claims = bronze_claims.count()

# --- Issue 10: Corrupted Excel serial dates ---
corrupt_incident = bronze_claims.filter(col("incident_date").rlike(r"^\d+:")).count()
corrupt_logged = bronze_claims.filter(col("Claim_Logged_On").rlike(r"^\d+:")).count()
corrupt_processed = bronze_claims.filter(col("Claim_Processed_On").rlike(r"^\d+:")).count()
total_corrupt = corrupt_incident + corrupt_logged + corrupt_processed
issues.append(("DQ-CLM-001", "bronze_claims", "incident_date, Claim_Logged_On, Claim_Processed_On",
    "corrupted_date_format", "HIGH", total_corrupt, total_claims * 3,
    round(total_corrupt / (total_claims * 3), 4) if total_claims > 0 else 0.0,
    f"Date columns contain corrupted Excel serial number fragments (e.g., '27:00.0'). incident_date: {corrupt_incident}, Claim_Logged_On: {corrupt_logged}, Claim_Processed_On: {corrupt_processed} affected.",
    "Extracted day number before colon, added as offset to Excel epoch (1899-12-30) using date_add()"))

# --- Issue 11: Sentinel 'NULL' strings in claims ---
null_str_prop_dmg = bronze_claims.filter(col("property_damage") == "NULL").count()
null_str_police = bronze_claims.filter(col("police_report_available") == "NULL").count()
null_str_collision = bronze_claims.filter(col("collision_type") == "NULL").count()
null_str_auth = bronze_claims.filter(col("authorities_contacted") == "NULL").count()
total_null_str = null_str_prop_dmg + null_str_police + null_str_collision + null_str_auth
issues.append(("DQ-CLM-002", "bronze_claims", "property_damage, police_report_available, collision_type, authorities_contacted",
    "sentinel_value", "MEDIUM", total_null_str, total_claims * 4,
    round(total_null_str / (total_claims * 4), 4) if total_claims > 0 else 0.0,
    f"String 'NULL' used instead of actual null in text fields: property_damage ({null_str_prop_dmg}), police_report_available ({null_str_police}), collision_type ({null_str_collision}), authorities_contacted ({null_str_auth}).",
    "Replaced string 'NULL' with actual null in Silver transformation"))

# --- Issue 12: Sentinel '?' strings in claims ---
q_prop_dmg = bronze_claims.filter(col("property_damage") == "?").count()
q_police = bronze_claims.filter(col("police_report_available") == "?").count()
q_collision = bronze_claims.filter(col("collision_type") == "?").count()
total_q = q_prop_dmg + q_police + q_collision
issues.append(("DQ-CLM-003", "bronze_claims", "property_damage, police_report_available, collision_type",
    "sentinel_value", "MEDIUM", total_q, total_claims * 3,
    round(total_q / (total_claims * 3), 4) if total_claims > 0 else 0.0,
    f"Question mark '?' used as placeholder in text fields: property_damage ({q_prop_dmg}), police_report_available ({q_police}), collision_type ({q_collision}).",
    "Replaced '?' with actual null in Silver transformation"))

print(f"Claims: 3 issues found. Total so far: {len(issues)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Sales — Empty Rows & Date Parsing

# COMMAND ----------

bronze_sales = spark.read.table("`databricks-hackathon-insurance`.bronze.bronze_sales")
total_sales = bronze_sales.count()

# --- Issue 13: Empty/padding rows with all NULL ---
empty_rows = bronze_sales.filter(col("sales_id").isNull()).count()
issues.append(("DQ-SAL-001", "bronze_sales", "sales_id",
    "empty_rows", "HIGH", empty_rows, total_sales,
    round(empty_rows / total_sales, 4) if total_sales > 0 else 0.0,
    f"{empty_rows} out of {total_sales} rows are completely empty — sales_id and all other columns are NULL. These are padding/filler rows in the source CSVs.",
    "Dropped using DLT expect_or_drop('valid_sales_id', 'sales_id IS NOT NULL'). Silver retains only {0} valid rows.".format(total_sales - empty_rows)))

# --- Issue 14: Date format needs parsing ---
date_rows = bronze_sales.filter(col("ad_placed_on").isNotNull()).count()
issues.append(("DQ-SAL-002", "bronze_sales", "ad_placed_on, sold_on",
    "date_format_parsing", "LOW", date_rows, total_sales,
    round(date_rows / total_sales, 4) if total_sales > 0 else 0.0,
    f"Date columns stored as strings in DD-MM-YYYY HH:MM format. {date_rows} ad_placed_on values required parsing to proper timestamp type.",
    "Parsed using to_timestamp(col, 'dd-MM-yyyy HH:mm') in Silver transformation"))

print(f"Sales: 2 issues found. Total so far: {len(issues)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Cars — Embedded Units in Numeric Fields

# COMMAND ----------

bronze_cars = spark.read.table("`databricks-hackathon-insurance`.bronze.bronze_cars")
total_cars = bronze_cars.count()

# --- Issue 15: Mileage has embedded unit 'kmpl' ---
mileage_with_unit = bronze_cars.filter(col("mileage").rlike(r"kmpl|km/kg")).count()
issues.append(("DQ-CAR-001", "bronze_cars", "mileage",
    "embedded_unit", "MEDIUM", mileage_with_unit, total_cars,
    round(mileage_with_unit / total_cars, 4) if total_cars > 0 else 0.0,
    f"{mileage_with_unit} records have mileage stored as string with unit (e.g., '23.4 kmpl'). Cannot perform numeric operations without extraction.",
    "Extracted numeric value using regexp_extract and stored as mileage_kmpl (DoubleType)"))

# --- Issue 16: Engine has embedded unit 'CC' ---
engine_with_unit = bronze_cars.filter(col("engine").rlike(r"CC|cc")).count()
issues.append(("DQ-CAR-002", "bronze_cars", "engine",
    "embedded_unit", "MEDIUM", engine_with_unit, total_cars,
    round(engine_with_unit / total_cars, 4) if total_cars > 0 else 0.0,
    f"{engine_with_unit} records have engine size stored as string with unit (e.g., '1248 CC'). Cannot perform numeric operations without extraction.",
    "Extracted numeric value using regexp_extract and stored as engine_cc (IntegerType)"))

# --- Issue 17: Max power has embedded unit 'bhp' ---
power_with_unit = bronze_cars.filter(col("max_power").rlike(r"bhp")).count()
issues.append(("DQ-CAR-003", "bronze_cars", "max_power",
    "embedded_unit", "MEDIUM", power_with_unit, total_cars,
    round(power_with_unit / total_cars, 4) if total_cars > 0 else 0.0,
    f"{power_with_unit} records have max power stored as string with unit (e.g., '74 bhp'). Cannot perform numeric operations without extraction.",
    "Extracted numeric value using regexp_extract and stored as max_power_bhp (DoubleType)"))

print(f"Cars: 3 issues found. Total so far: {len(issues)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Policy — Column Typo

# COMMAND ----------

bronze_policy = spark.read.table("`databricks-hackathon-insurance`.bronze.bronze_policy")
total_policy = bronze_policy.count()

# --- Issue 18: Column name typo 'policy_deductable' ---
issues.append(("DQ-POL-001", "bronze_policy", "policy_deductable",
    "column_name_typo", "LOW", total_policy, total_policy, 1.0,
    f"Column named 'policy_deductable' — misspelling of 'deductible'. Affects all {total_policy} records.",
    "Renamed to 'policy_deductible' in Silver transformation"))

print(f"Policy: 1 issue found. Total: {len(issues)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the DQ Issues Table

# COMMAND ----------

from datetime import datetime

# Convert to DataFrame
rows = []
now = datetime.now()
for issue in issues:
    rows.append((*issue, now))

dq_df = spark.createDataFrame(rows, schema)

# Display summary
print(f"Total DQ issues cataloged: {dq_df.count()}")
dq_df.select("issue_id", "table_name", "rule_name", "severity", "affected_records").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Unity Catalog

# COMMAND ----------

(
    dq_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("`databricks-hackathon-insurance`.silver.dq_issues")
)

print("✓ Table `databricks-hackathon-insurance`.silver.dq_issues created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `databricks-hackathon-insurance`.silver.dq_issues ORDER BY issue_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT severity, COUNT(*) as issue_count
# MAGIC FROM `databricks-hackathon-insurance`.silver.dq_issues
# MAGIC GROUP BY severity
# MAGIC ORDER BY
# MAGIC   CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 END;
