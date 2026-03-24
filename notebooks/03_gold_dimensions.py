# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer - Dimension Tables
# MAGIC
# MAGIC Building the star schema dimensions from Silver data.
# MAGIC These are the "lookup" tables that give context to our facts.
# MAGIC
# MAGIC Dimensions:
# MAGIC - dim_customer: unified customer profiles
# MAGIC - dim_policy: policy details
# MAGIC - dim_car: vehicle information
# MAGIC - dim_date: calendar dimension (generated)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_customer
# MAGIC
# MAGIC Comes from our deduplicated silver.customers table.
# MAGIC Each row = one unique customer across all regions.

# COMMAND ----------

silver_customers = spark.table("`databricks-hackathon-insurance`.silver.customers")

dim_customer = (silver_customers
    .withColumn("customer_sk",
        F.row_number().over(Window.orderBy("master_customer_id"))
    )
    .select(
        "customer_sk",
        "master_customer_id",
        "original_customer_id",
        "region",
        "state",
        "city",
        "job",
        "marital",
        "education",
        "has_default",
        "balance",
        "hh_insurance",
        "car_loan",
    )
)

(dim_customer.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.dim_customer"))

print(f"gold.dim_customer: {dim_customer.count()} rows")
display(dim_customer.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_policy
# MAGIC
# MAGIC Policy dimension - one row per policy.
# MAGIC Links to customer and car through foreign keys.

# COMMAND ----------

silver_policy = spark.table("`databricks-hackathon-insurance`.silver.policy")

dim_policy = (silver_policy
    .withColumn("policy_sk",
        F.row_number().over(Window.orderBy("policy_number"))
    )
    .select(
        "policy_sk",
        "policy_number",
        "policy_bind_date",
        "policy_state",
        "policy_csl",
        "policy_deductible",
        "policy_annual_premium",
        "umbrella_limit",
        "car_id",
        "customer_id",
    )
)

(dim_policy.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.dim_policy"))

print(f"gold.dim_policy: {dim_policy.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_car
# MAGIC
# MAGIC Vehicle dimension. Used for both claims analysis and inventory/revenue tracking.

# COMMAND ----------

silver_cars = spark.table("`databricks-hackathon-insurance`.silver.cars")

dim_car = (silver_cars
    .withColumn("car_sk",
        F.row_number().over(Window.orderBy("car_id"))
    )
    .select(
        "car_sk",
        "car_id",
        "name",
        "model",
        "fuel",
        "transmission",
        "km_driven",
        "mileage",
        "engine",
        "max_power",
        "seats",
    )
)

(dim_car.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.dim_car"))

print(f"gold.dim_car: {dim_car.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date
# MAGIC
# MAGIC Calendar dimension - generated, not from source data.
# MAGIC Covers a range that spans all our data dates.

# COMMAND ----------

from pyspark.sql.types import DateType
from datetime import date, timedelta

# generate date range covering our data
start_date = date(2014, 1, 1)
end_date = date(2025, 12, 31)

dates = []
current = start_date
while current <= end_date:
    dates.append((current,))
    current += timedelta(days=1)

date_df = spark.createDataFrame(dates, ["date_value"])

dim_date = (date_df
    .withColumn("date_sk", F.date_format("date_value", "yyyyMMdd").cast("integer"))
    .withColumn("year", F.year("date_value"))
    .withColumn("quarter", F.quarter("date_value"))
    .withColumn("month", F.month("date_value"))
    .withColumn("month_name", F.date_format("date_value", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("date_value"))
    .withColumn("day_of_week", F.dayofweek("date_value"))
    .withColumn("day_name", F.date_format("date_value", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("date_value"))
    .withColumn("is_weekend",
        F.when(F.dayofweek("date_value").isin(1, 7), True).otherwise(False)
    )
    .select(
        "date_sk",
        "date_value",
        "year",
        "quarter",
        "month",
        "month_name",
        "day_of_month",
        "day_of_week",
        "day_name",
        "week_of_year",
        "is_weekend",
    )
)

(dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.dim_date"))

print(f"gold.dim_date: {dim_date.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC Dimensions are ready. Next: fact tables.
