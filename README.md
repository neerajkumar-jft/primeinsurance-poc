# PrimeInsurance Data Platform POC

## Overview

Proof of Concept for unifying data from 6 acquired regional auto insurance systems onto the Databricks Intelligence Data Platform. Solves three business failures:

1. Regulatory compliance -- 12% inflated customer count from duplicate records across regional systems
2. Claims backlog -- 18-day average processing time against a 7-day industry benchmark
3. Revenue leakage -- no cross-regional visibility into aging unsold inventory

## Architecture

Medallion architecture on Databricks with Unity Catalog governance:

```
Raw Files (Volumes) -> Bronze (DLT) -> Silver (DLT) -> Gold (DLT) -> SQL Warehouse / GenAI
```

- Catalog: `prime_insurance_jellsinki_poc`
- Schemas: `bronze`, `silver`, `gold`
- Compute: Serverless DLT pipelines + Serverless SQL Warehouse

## Data sources

| Dataset | Files | Format | Records |
|---------|-------|--------|---------|
| Customers | 7 | CSV | ~3,610 |
| Claims | 2 | JSON | 1,000 |
| Policy | 1 | CSV | 1,000 |
| Sales | 3 | CSV | ~4,982 |
| Cars | 1 | CSV | 2,500 |

14 files total from 6 regional systems, stored in a single `raw_data` volume preserving the original folder structure (Insurance 1/ through Insurance 6/).

## Project structure

```
├── data/                              # Raw data files from regional systems
│   ├── Insurance 1/                   # customers_1.csv, Sales_2.csv
│   ├── Insurance 2/                   # customers_2.csv, sales_1.csv
│   ├── Insurance 3/                   # customers_3.csv, sales_4.csv
│   ├── Insurance 4/                   # customers_4.csv, cars.csv
│   ├── Insurance 5/                   # customers_5.csv, policy.csv
│   ├── Insurance 6/                   # customers_6.csv, claims_1.json
│   ├── customers_7.csv
│   └── claims_2.json
├── docs/
│   ├── architecture_design.md         # Full architecture document
│   ├── dimensional_model.md           # Star schema design (facts, dims, views)
│   ├── bronze_quality_findings.csv    # Data quality issues found in bronze tables
│   ├── silver_quality_rules.csv       # Quality rules applied in silver layer
│   ├── gold_layer_notes.md            # Issues, resolutions, extra work
│   ├── team_summary.html              # 1-page team summary
│   └── diagrams/                      # draw.io diagrams + PNGs
│       ├── 01_data_flow.drawio
│       ├── 02_component_map.drawio
│       ├── 03_data_quality.drawio
│       ├── 04_genai_integration.drawio
│       └── 05_star_schema.drawio
├── notebooks/
│   ├── 00_setup/
│   │   ├── 01_create_catalog_schemas.sql   # Creates catalog + bronze/silver/gold schemas
│   │   ├── 02_create_volumes.sql           # Creates raw_data volume
│   │   ├── 03_upload_data_to_volumes.py    # Copies 14 files preserving folder structure
│   │   ├── 04_create_dlt_pipelines.py      # Creates bronze, silver, gold DLT pipelines
│   │   ├── 05_run_all_pipelines.py         # Orchestrates all pipelines sequentially
│   │   └── 06_create_workflow.py           # Creates Databricks Workflow for one-click run
│   ├── 01_bronze/
│   │   ├── bronze_customers.sql            # 7 CSVs -> 1 table, schema evolution
│   │   ├── bronze_claims.sql               # 2 JSONs -> 1 table
│   │   ├── bronze_policy.sql               # 1 CSV -> 1 table
│   │   ├── bronze_sales.sql                # 3 CSVs -> 1 table, mixed case glob
│   │   └── bronze_cars.sql                 # 1 CSV -> 1 table
│   ├── 02_silver/
│   │   ├── silver_customers.sql            # Harmonize 3 ID cols, expand regions, dedup, quarantine
│   │   ├── silver_claims.sql               # Parse Excel dates, replace ? and "NULL", quarantine
│   │   ├── silver_policy.sql               # Type casting, fix deductable typo
│   │   ├── silver_sales.sql                # Parse DD-MM-YYYY dates, drop junk rows, quarantine
│   │   ├── silver_cars.sql                 # Extract units from strings, split torque
│   │   └── silver_validation.sql           # Before/after comparison queries for screenshots
│   ├── 03_gold/
│   │   ├── gold_dimensions.sql             # dim_customers, dim_policies, dim_cars, dim_date
│   │   ├── gold_facts.sql                  # fact_claims, fact_sales with derived measures
│   │   ├── gold_views.sql                  # 4 business views (claims, regulatory, revenue, policy)
│   │   └── gold_validation.sql             # Validation queries for screenshots
│   └── 04_genai/
│       ├── uc1_dq_explainer.py             # LLM explains DQ issues for compliance
│       ├── uc2_claims_anomaly.py           # MAD z-scores + IQR fraud detection + LLM briefs
│       ├── uc3_rag_policy_assistant.py     # FAISS + sentence-transformers RAG
│       └── uc4_business_insights.py        # KPI aggregation + executive summaries + ai_query()
```

## Setup instructions

1. Clone this repo into your Databricks workspace
2. Run notebooks in order:
   - `00_setup/01_create_catalog_schemas.sql` -- creates catalog and schemas
   - `00_setup/02_create_volumes.sql` -- creates the raw_data volume
   - `00_setup/03_upload_data_to_volumes.py` -- copies data files to volume
   - `00_setup/04_create_dlt_pipelines.py` -- creates bronze, silver, gold pipelines
3. Run pipelines: bronze first, then silver, then gold
4. Run GenAI notebooks (04_genai/) individually on serverless compute

Or run `00_setup/06_create_workflow.py` once to create a Databricks Workflow that chains everything, then trigger it from the Workflows UI.

## Pipelines

| Pipeline | Target schema | Tables | What it does |
|----------|---------------|--------|-------------|
| _bronze | bronze | 5 streaming tables | Raw ingestion with Auto Loader, schema evolution |
| _silver | silver | 8 views + 3 quarantine + 1 quality log | Harmonization, quality rules, dedup |
| _gold | gold | 4 dims + 2 facts + 4 views | Star schema with pre-computed aggregations |

DLT UC requires one target schema per pipeline, so they run as three separate pipelines in sequence.

## GenAI use cases

| UC | Notebook | Output table | What it does |
|----|----------|-------------|-------------|
| 1 | uc1_dq_explainer.py | gold.dq_explanation_report | Explains DQ issues in plain English for compliance |
| 2 | uc2_claims_anomaly.py | gold.claim_anomaly_explanations | Statistical fraud scoring + LLM investigation briefs |
| 3 | uc3_rag_policy_assistant.py | gold.rag_query_history | FAISS vector search + LLM policy Q&A |
| 4 | uc4_business_insights.py | gold.ai_business_insights | KPI aggregation + executive summaries |

All use databricks-gpt-oss-20b. Statistical analysis runs first, LLM generates narratives only.

## Team

| Member | Email | Responsibility |
|--------|-------|----------------|
| Neeraj Kumar | neeraj.kumar@jellyfishtechnologies.com | Infrastructure setup, Bronze layer |
| Aman Kumar Singh | aksingh@jellyfishtechnologies.com | Silver layer, Gold layer |
| Paras Dhyani | paras.dhyani@jellyfishtechnologies.com | Silver layer, Gold layer |
| Abhinav Sarkar | abhinav.sarkar@jellyfishtechnologies.com | GenAI implementation |
