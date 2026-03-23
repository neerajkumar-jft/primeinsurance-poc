# Databricks notebook source

# COMMAND ----------

# Central configuration for the Prime Insurance POC pipeline
CATALOG_NAME = "prime-ins-jellsinki-poc"
try:
    spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
except Exception:
    spark.sql(f"CREATE CATALOG `{CATALOG_NAME}`")
    spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

catalog = CATALOG_NAME

# ensure schemas exist
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.{schema}")

print(f"catalog: {catalog}")
print("schemas: bronze, silver, gold")
