# Architecture design -- PrimeInsurance data intelligence platform

## End-to-end data flow

```
14 Source Files (6 regional folders, CSV + JSON)
        |
        v
Unity Catalog Volumes (primeins.bronze.source_files)
        |
        v
+-------------------------------+
|   BRONZE LAYER                |
|   DLT + Auto Loader           |
|   Schema: primeins.bronze     |
|   --------------------------  |
|   customers  (7 CSV files)    |
|   claims     (2 JSON files)   |
|   policy     (1 CSV file)     |
|   sales      (3 CSV files)    |
|   cars       (1 CSV file)     |
|                               |
|   Raw data preserved as-is.   |
|   Source file + load timestamp |
|   added to every record.      |
+---------------+---------------+
                |
                v
+-------------------------------+
|   SILVER LAYER                |
|   DLT Expectations            |
|   Schema: primeins.silver     |
|   --------------------------  |
|   customers  (harmonized)     |
|   claims     (cleaned)        |
|   policy     (validated)      |
|   sales      (standardized)   |
|   cars       (normalized)     |
|   dq_issues  (quality log)    |
|   quarantine_customers        |
|   quarantine_claims           |
|   quarantine_policy           |
|   quarantine_sales            |
|   quarantine_cars             |
+---------------+---------------+
                |
                v
+---------------------------------------+
|   GOLD LAYER                          |
|   Materialized Views + Delta Tables   |
|   Schema: primeins.gold              |
|   ----------------------------------  |
|                                       |
|   DIMENSIONS:                         |
|     dim_customer  (PK: customer_id)   |
|     dim_policy    (PK: policy_number) |
|     dim_car       (PK: car_id)        |
|     dim_region    (PK: region)        |
|                                       |
|   FACTS:                              |
|     fact_claims   (1 row per claim)   |
|     fact_sales    (1 row per listing) |
|                                       |
|   AI OUTPUTS:                         |
|     dq_explanation_report             |
|     claim_anomaly_explanations        |
|     rag_query_history                 |
|     ai_business_insights              |
+-------------------+-------------------+
                    |
          +---------+---------+
          |                   |
          v                   v
+----------------+   +------------------+
| SQL Warehouse  |   | Gen AI Layer     |
| (serving)      |   | databricks-gpt-  |
|                |   | oss-20b          |
| Dashboards     |   |                  |
| Genie Space    |   | UC1: DQ Explain  |
| Ad-hoc queries |   | UC2: Anomaly     |
+----------------+   | UC3: RAG + Genie |
                      | UC4: Insights    |
                      +------------------+

Unity Catalog wraps all layers: governance, access control, lineage.
```

## Source files

| Folder | Files | Entity | Format |
|--------|-------|--------|--------|
| Insurance 1 | customers_1.csv, Sales_2.csv | customers, sales | CSV |
| Insurance 2 | customers_2.csv, sales_1.csv | customers, sales | CSV |
| Insurance 3 | customers_3.csv, sales_4.csv | customers, sales | CSV |
| Insurance 4 | customers_4.csv, cars.csv | customers, cars | CSV |
| Insurance 5 | customers_5.csv, policy.csv | customers, policy | CSV |
| Insurance 6 | customers_6.csv, claims_1.json | customers, claims | CSV, JSON |
| Root | customers_7.csv, claims_2.json | customers, claims | CSV, JSON |

14 files total. 7 customer files, 3 sales files, 2 claims files, 1 policy file, 1 cars file.

## Component map

| Component | Databricks feature | Layer | What it does |
|-----------|-------------------|-------|-------------|
| File storage | Unity Catalog Volumes | Bronze | Stores raw source files with regional folder structure |
| Ingestion | DLT + Auto Loader (cloudFiles) | Bronze | Incremental file loading, schema evolution via mergeSchema, tracks which files have been processed |
| Pipeline orchestration | Delta Live Tables | Bronze, Silver, Gold | Declarative pipeline across all three layers |
| Data quality | DLT Expectations (expect, expect_or_drop, expect_or_fail) | Silver | Inline validation rules, routes failures to quarantine |
| Quarantine | DLT + separate Delta tables | Silver | Stores records that fail quality rules with full context |
| Quality log | dq_issues table | Silver | Logs every quality issue: table, column, rule, severity, record count |
| Dimensional model | Delta tables | Gold | Star schema with fact and dimension tables |
| Aggregations | Materialized Views | Gold | Pre-computed metrics that auto-refresh when upstream data changes |
| Serving | SQL Warehouse | Gold | Fast concurrent query access for 20+ dashboard users |
| LLM | databricks-gpt-oss-20b (OpenAI-compatible API) | Gen AI | Generates explanations, investigation briefs, executive summaries |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) | Gen AI | Local embedding generation for RAG policy assistant |
| Vector index | FAISS | Gen AI | Similarity search over embedded policy documents |
| NLQ | Databricks Genie Space | Gen AI | Natural language SQL queries on Gold tables |
| SQL AI | ai_query() | Gen AI | LLM calls from within SQL queries |
| Governance | Unity Catalog | All | Access control, lineage tracking, discoverability |

## Data quality strategy

### By layer

Bronze: no validation, no cleaning. Data lands exactly as received. Auto Loader handles schema evolution (mergeSchema) so new columns in source files don't break the pipeline. Every record gets a `_source_file` and `_load_timestamp` column for traceability.

Silver: all quality enforcement happens here via DLT Expectations.

- `expect_or_drop` for records that should be removed but shouldn't stop the pipeline (null customer_id, invalid region, out-of-range amounts)
- `expect_or_fail` for catastrophic issues (empty source file, completely missing required table)
- `expect` for warnings on optional fields (missing education, missing job title)

Failed records go to quarantine tables (`quarantine_customers`, `quarantine_claims`, etc.) with the original data plus the rule that caught them. The `dq_issues` table logs every issue with table name, column, rule, severity, and affected record count.

Gold: referential integrity between fact and dimension tables. Aggregation sanity checks (no negative counts, processing times within expected range).

### Quality rules by entity

customers:
- `customer_id` must not be null
- Region must be one of East, West, Central, South, North (after standardization from W/C/E/S/N)
- No exact duplicate records

claims:
- `ClaimID` must not be null
- `PolicyID` must not be null
- Claim amounts (injury, property, vehicle) must be numeric and >= 0
- `Claim_Rejected` must be Y or N
- Date fields repaired from corrupted format ("27:00.0" etc.)

policy:
- `policy_number` must not be null
- `customer_id` must not be null
- `policy_annual_premium` must be > 0
- `policy_csl` must be a valid coverage tier

sales:
- `sales_id` must not be null
- `original_selling_price` must be > 0
- `ad_placed_on` must be a valid date

cars:
- `car_id` must not be null
- `km_driven` must be >= 0

## Silver harmonization (what gets fixed)

| Issue | Source | Fix |
|-------|--------|-----|
| Customer ID column names: CustomerID, Customer_ID, cust_id | customers_1-7 | Standardize to `customer_id` |
| Region abbreviations: W, C, E | customers_5 | Map to full names: West, Central, East |
| Region column name: Reg | customers_1 | Rename to `region` |
| Column names: Marital_status vs Marital, Education vs Edu, City vs City_in_state | Various customer files | Standardize to `marital`, `education`, `city` |
| Missing columns: HHInsurance (customers_2), Education (customers_4) | customers_2, customers_4 | Add as null columns |
| Typo: "terto" | customers_5 | Correct to "tertiary" |
| String "NULL" and "?" | claims files | Convert to actual null |
| Corrupted dates: "27:00.0", "34:00.0" | claims files | Parse and convert to proper timestamps |
| All-string fields in JSON | claims files | Cast injury, property, vehicle to float; bodily_injuries, witnesses to int |

## Star schema (Gold layer)

```
                  +---------------+
                  | dim_customer  |
                  |---------------|
                  | customer_id   | <-- PK
                  | region        |
                  | state         |
                  | city          |
                  | job           |
                  | marital       |
                  | education     |
                  | default       |
                  | balance       |
                  | hh_insurance  |
                  | car_loan      |
                  +-------+-------+
                          |
+-------------+  +--------+----------+  +--------------+
| dim_region  |  |   fact_claims      |  | dim_policy   |
|-------------|  |-------------------|  |--------------|
| region      |  | claim_id          |  | policy_number| <-- PK
| states[]    |  | policy_number (FK)|  | policy_csl   |
+-------------+  | customer_id  (FK) |  | policy_deductable |
                 | car_id       (FK) |  | policy_annual_premium |
                 | incident_date     |  | umbrella_limit |
                 | incident_type     |  | policy_state |
                 | incident_severity |  | policy_bind_date |
                 | injury_amount     |  | car_id       |
                 | property_amount   |  | customer_id  |
                 | vehicle_amount    |  +--------------+
                 | processing_time_days |
                 | is_rejected       |  +--------------+
                 | claim_logged_on   |--| dim_car      |
                 | claim_processed_on|  |--------------|
                 +-------------------+  | car_id       | <-- PK
                                        | name         |
+-------------+  +-------------------+  | model        |
| dim_region  |  |   fact_sales       |  | fuel         |
|             |--| ------------------|  | transmission |
+-------------+  | sales_id          |  | km_driven    |
                 | car_id       (FK) |  | mileage      |
                 | ad_placed_on      |  | engine       |
                 | sold_on           |  | max_power    |
                 | original_selling_price | | torque    |
                 | days_listed       |  | seats        |
                 | region            |--+--------------+
                 | state             |
                 | city              |
                 | seller_type       |
                 | owner             |
                 +-------------------+
```

### How fact tables answer the three business questions

Rejection rate by policy type: fact_claims JOIN dim_policy ON policy_number, GROUP BY policy_csl, rate = COUNT(is_rejected = true) / COUNT(*)

Processing time by severity: fact_claims alone, GROUP BY incident_severity, AVG(processing_time_days)

Unsold cars by model: fact_sales JOIN dim_car ON car_id, WHERE sold_on IS NULL, GROUP BY model, show days_listed

## Gen AI integration

Model: `databricks-gpt-oss-20b` (free on Databricks, 128K context, OpenAI-compatible API)

Connection: OpenAI SDK pointing to Databricks serving endpoint. Returns structured JSON that must be parsed to extract the text block.

```python
from openai import OpenAI

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)
```

### Use cases

UC1 -- DQ Explainer (Easy)
- Reads: `primeins.silver.dq_issues`
- Prompt: takes each issue row (table, column, rule, severity, count) and asks the LLM to explain in plain English what broke, why it matters, what caused it, what to do
- Output: `primeins.gold.dq_explanation_report`

UC2 -- Claims Anomaly Detection (Medium)
- Reads: `primeins.silver.claims` + Gold dimensions for context
- Process: PySpark statistical scoring with 5+ weighted fraud-indicator rules, then LLM generates investigation briefs for flagged claims
- Output: `primeins.gold.claim_anomaly_explanations`

UC3 -- RAG Policy Assistant + Genie NLQ (Advanced)
- Part A (RAG): reads `primeins.gold.dim_policy`, converts to natural language docs, chunks, embeds with sentence-transformers (all-MiniLM-L6-v2), indexes in FAISS, answers questions by retrieving relevant chunks and passing to LLM
- Part B (Genie): Genie Space on Gold tables (dim_policy, dim_customer, fact_claims) with context instructions explaining schema
- Output: `primeins.gold.rag_query_history`

UC4 -- AI Business Insights (Bonus)
- Reads: all Gold tables
- Process: aggregates KPIs first (never pass raw rows to LLM), then passes summaries for executive narratives. Also demonstrates ai_query() for SQL-native LLM calls
- Output: `primeins.gold.ai_business_insights`

### Where Gen AI sits

The intelligence layer sits above Gold because Gold data is clean, joined, and governed. If the LLM reads from Bronze or Silver and produces a wrong insight, you can't tell whether the data or the model caused the problem. Reading from Gold means the data has passed validation. Wrong output = prompt or model issue, not data issue.

The one exception: UC1 reads from Silver (dq_issues) because that's where quality problems are logged during Silver processing.

All AI output tables live in Gold because they are finished, business-ready artifacts.

## Access control

| Role | Bronze | Silver | Gold |
|------|--------|--------|------|
| Data Engineers | Full access | Full access | Full access |
| Compliance team | No access | No access | SELECT on Gold tables |
| Business users | No access | No access | SELECT on Gold tables via SQL Warehouse |
| Auditors | Read lineage | Read lineage | Read lineage + SELECT |

## Lineage

Unity Catalog tracks the full chain automatically:

Source file in Volume -> Bronze table -> Silver table -> Gold fact/dimension -> Gold aggregation/AI output

No manual lineage documentation required. Visible in the Unity Catalog lineage diagram.
