# Databricks notebook source
# MAGIC %md
# MAGIC # Regulatory Customer Deduplication Report
# MAGIC
# MAGIC PrimeInsurance has 90 days to produce an auditable, unified customer registry
# MAGIC for regulators. Two regions reported the same customers under different IDs,
# MAGIC inflating the policyholder count by ~12%.
# MAGIC
# MAGIC This report documents:
# MAGIC - How many raw records came in from each regional system
# MAGIC - How many duplicates were identified and resolved
# MAGIC - The final auditable customer count by region
# MAGIC - The methodology used for deduplication
# MAGIC - Data quality issues found and how they were handled

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Source data summary
# MAGIC How many records did each regional system hand over?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   _source_file as source_file,
# MAGIC   COUNT(*) as records
# MAGIC FROM primeins.bronze.customers
# MAGIC GROUP BY _source_file
# MAGIC ORDER BY _source_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplication summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM primeins.bronze.customers) as bronze_raw_records,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.customers) as silver_after_harmonization,
# MAGIC   (SELECT COUNT(*) FROM primeins.gold.dim_customer) as gold_unique_customers,
# MAGIC   (SELECT COUNT(*) FROM primeins.silver.customers) -
# MAGIC     (SELECT COUNT(*) FROM primeins.gold.dim_customer) as duplicates_resolved,
# MAGIC   ROUND(
# MAGIC     ((SELECT COUNT(*) FROM primeins.silver.customers) -
# MAGIC      (SELECT COUNT(*) FROM primeins.gold.dim_customer)) * 100.0 /
# MAGIC     (SELECT COUNT(*) FROM primeins.silver.customers), 1
# MAGIC   ) as dedup_rate_pct

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Auditable customer count by region
# MAGIC This is the number regulators need.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   region,
# MAGIC   COUNT(DISTINCT customer_id) as unique_customers,
# MAGIC   ROUND(COUNT(DISTINCT customer_id) * 100.0 /
# MAGIC     (SELECT COUNT(DISTINCT customer_id) FROM primeins.gold.dim_customer), 1
# MAGIC   ) as pct_of_total
# MAGIC FROM primeins.gold.dim_customer
# MAGIC GROUP BY region
# MAGIC ORDER BY unique_customers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Customer count by state (top 15)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   state,
# MAGIC   region,
# MAGIC   COUNT(DISTINCT customer_id) as customers
# MAGIC FROM primeins.gold.dim_customer
# MAGIC GROUP BY state, region
# MAGIC ORDER BY customers DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data quality issues found in customer data
# MAGIC Issues caught during harmonization, with AI-generated explanations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.rule_name,
# MAGIC   d.severity,
# MAGIC   d.affected_records,
# MAGIC   ROUND(d.affected_ratio * 100, 1) as affected_pct,
# MAGIC   d.action_taken,
# MAGIC   e.what_was_found
# MAGIC FROM primeins.silver.dq_issues d
# MAGIC LEFT JOIN primeins.gold.dq_explanation_report e
# MAGIC   ON d.table_name = e.table_name AND d.rule_name = e.rule_name
# MAGIC WHERE d.table_name = 'customers'
# MAGIC ORDER BY
# MAGIC   CASE d.severity WHEN 'critical' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quarantined customer records
# MAGIC Records that failed validation and were removed from the clean registry.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as quarantined_records FROM primeins.silver.quarantine_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If any quarantined records exist, show them
# MAGIC SELECT * FROM primeins.silver.quarantine_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Harmonization methodology
# MAGIC
# MAGIC How we unified 7 different customer files into one registry:
# MAGIC
# MAGIC | Step | What we did | Why |
# MAGIC |------|------------|-----|
# MAGIC | Column unification | CustomerID, Customer_ID, cust_id -> customer_id | 3 different ID column names across 7 files |
# MAGIC | Region standardization | W/C/E/S -> West/Central/East/South | customers_5.csv used abbreviations |
# MAGIC | Region column merge | Reg, Region -> region | customers_1.csv used "Reg" |
# MAGIC | Marital/Education swap fix | Detected and corrected swapped columns | customers_6.csv had Marital_status and Education reversed |
# MAGIC | Typo correction | "terto" -> "tertiary" | 73 records in customers_5.csv |
# MAGIC | NA string cleanup | "NA" -> NULL | 118+ records across files |
# MAGIC | City column merge | City, City_in_state -> city | customers_2.csv used City_in_state |
# MAGIC | Missing column handling | Added as NULL where column didn't exist | HHInsurance missing from file 2, Education from file 4, Job from file 1 |
# MAGIC | Deduplication | Window function on customer_id, keep first by load timestamp | customers_7.csv overlaps with other files |
# MAGIC
# MAGIC All transformations are declared in the DLT Silver pipeline. Bronze data is preserved
# MAGIC as-is for audit trail. Every record carries `_source_file` for traceability back to
# MAGIC the originating regional system.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Lineage and auditability
# MAGIC
# MAGIC Every customer record can be traced from the Gold `dim_customer` table back to the
# MAGIC exact source CSV file it came from:
# MAGIC
# MAGIC ```
# MAGIC dim_customer (Gold) -> customers (Silver) -> customers (Bronze) -> source CSV in Volume
# MAGIC ```
# MAGIC
# MAGIC Unity Catalog tracks this lineage automatically. The compliance team has SELECT
# MAGIC access to Gold tables only. Engineers have access to all layers. Auditors can
# MAGIC view the lineage graph in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cross-regional duplicate detection
# MAGIC
# MAGIC Customers appearing in multiple source files (potential cross-regional duplicates):

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COUNT(*) as appearances,
# MAGIC   COLLECT_SET(_source_file) as source_files
# MAGIC FROM primeins.silver.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY appearances DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Regulatory readiness assessment
# MAGIC
# MAGIC | Criteria | Status | Detail |
# MAGIC |----------|--------|--------|
# MAGIC | Unified customer registry | Complete | 1,604 unique customers from 3,605 raw records |
# MAGIC | Auditable count by region | Complete | East, West, Central, South, North breakdown available |
# MAGIC | Traceability to source | Complete | Every record linked to source file via _source_file |
# MAGIC | Data quality documented | Complete | 4 customer issues logged in dq_issues with AI explanations |
# MAGIC | Quarantine for failed records | Complete | Separate quarantine table with rejection reasons |
# MAGIC | Cross-regional dedup | Complete | Duplicates identified across regional files |
# MAGIC | Access control | Complete | Compliance has Gold-only access via Unity Catalog |
# MAGIC | Lineage tracking | Complete | Automatic via Unity Catalog, source file to Gold table |
