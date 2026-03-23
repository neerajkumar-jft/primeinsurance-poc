# PrimeInsurance Data Intelligence Platform

End-to-end data platform on Databricks solving three critical business failures for a multi-region auto insurance company.

## Business Problems Solved

| Problem | Root Cause | Solution |
|---------|-----------|----------|
| **Customer Identity Crisis** | Same customers across 6 regions with different IDs, ~12% inflated count | Identity resolution in Silver, unified `dim_customer` in Gold |
| **Claims Processing Delay** | 18-day avg vs 7-day benchmark, no unified view | Star schema with `fact_claims`, SLA monitoring, anomaly detection |
| **Revenue Leakage** | Unsold cars aging unnoticed, no cross-regional visibility | Inventory aging alerts with cross-regional redistribution signals |

## Architecture

```
Source Files (6 regions, CSV + JSON)
    |
    v
[BRONZE] Auto Loader -> Delta tables (raw, as-is)
    |
    v
[SILVER] Quality rules + Standardization + Quarantine
    |
    v
[GOLD] Star schema (dims + facts) + Business aggregations
    |
    v
[AI LAYER] LLM explanations, Anomaly detection, RAG, Executive insights
```

**Catalog**: `databricks-hackathon-insurance`
**Schemas**: `bronze`, `silver`, `gold`

## Quick Start

### Option 1: Automated Deploy (Recommended)

```bash
# clone repo
git clone git@github.com:aksingh-jft/primeins.git
cd primeins

# authenticate with Databricks
databricks auth login --host https://dbc-23b39f08-0ca0.cloud.databricks.com

# deploy notebooks + create pipeline job
./deploy/deploy.sh

# trigger pipeline
databricks jobs run-now <JOB_ID>
```

### Option 2: Manual Deploy

1. Import all notebooks from `notebooks/` to your Databricks workspace
2. Create a workflow job using `deploy/job_config.json` (replace `{{WORKSPACE_PATH}}`)
3. Run the job — setup notebook auto-uploads data files on first run

## Pipeline DAG

```
setup ─> bronze_ingestion
              ├── silver_customers  ─┐
              ├── silver_claims      ├── silver_dq_combined ── uc1_dq_explanations
              ├── silver_policy      │
              ├── silver_sales       ├── gold_dimensions ── gold_facts
              └── silver_cars        ┘                       ├── uc2_anomaly_engine
                                                             ├── uc3_policy_rag
                                                             └── uc4_executive_insights
```

- All 5 Silver notebooks run **in parallel**
- Gold waits for all Silver to complete
- UC1 runs as soon as DQ issues are ready
- UC2, UC3, UC4 run **in parallel** after Gold

## Notebooks

| # | Notebook | Layer | Description |
|---|----------|-------|-------------|
| 1 | `00_setup` | Setup | Validates catalog/schemas/volume, auto-uploads missing data files |
| 2 | `00_data_exploration` | Explore | Profile all 5 entities, document inconsistencies |
| 3 | `01_bronze_ingestion` | Bronze | Load 14 source files as-is into Delta tables |
| 4 | `02_silver_customers` | Silver | Unify 7 different schemas, identity resolution, dedup |
| 5 | `02_silver_claims` | Silver | Parse dates, validate amounts, calculate processing time |
| 6 | `02_silver_policy` | Silver | Standardize formats, validate coverage fields |
| 7 | `02_silver_sales` | Silver | Parse datetime, flag aging inventory |
| 8 | `02_silver_cars` | Silver | Extract numeric values, standardize categoricals |
| 9 | `02_silver_dq_combined` | Silver | Merge all entity DQ issues into unified table |
| 10 | `03_gold_dimensions` | Gold | dim_customer, dim_policy, dim_car, dim_date |
| 11 | `03_gold_facts` | Gold | fact_claims, fact_sales + regulatory registry, SLA monitor, aging alerts, readiness score |
| 12 | `04_uc1_dq_explanations` | GenAI | LLM explains DQ issues in plain English |
| 13 | `04_uc2_anomaly_engine` | GenAI | 5 statistical rules + LLM investigation briefs |
| 14 | `04_uc3_policy_rag` | GenAI | RAG over policy data with FAISS + sentence-transformers |
| 15 | `04_uc4_executive_insights` | GenAI | KPI aggregation to executive briefings |

## Gold Layer Tables

| Table | Purpose |
|-------|---------|
| `dim_customer` | Deduplicated customers with master ID |
| `dim_policy` | Policy details with coverage/premium |
| `dim_car` | Vehicle information |
| `dim_date` | Calendar dimension |
| `fact_claims` | One row per claim with all dimension FKs |
| `fact_sales` | One row per listing with aging flags |
| `regulatory_customer_registry` | Auditable customer count for regulators |
| `claims_sla_monitor` | Processing times vs 7-day benchmark by region |
| `inventory_aging_alerts` | Unsold cars with cross-regional redistribution signals |
| `regulatory_readiness` | Single 0-100 readiness score |
| `dq_explanation_report` | AI-generated quality issue explanations |
| `claim_anomaly_explanations` | AI-generated fraud investigation briefs |
| `rag_query_history` | Policy Q&A with source citations |
| `ai_business_insights` | Executive briefings by business domain |

## Data Quality Strategy

| Layer | Approach | On Failure |
|-------|----------|-----------|
| Bronze | Preserve raw, `mergeSchema` for evolution | Log schema mismatch |
| Silver | Rule-based validation per entity | Route to quarantine table + log to `dq_issues` |
| Gold | Referential integrity via joins | NULLs for unmatched FKs |

## Project Structure

```
primeins/
  README.md
  deploy/
    deploy.sh             # one-command deploy script
    job_config.json       # Databricks workflow job config
  notebooks/              # all Databricks notebooks
  data/
    autoinsurancedata/    # source files (auto-uploaded to Volume)
  architecture.md         # system design
  decisions.md            # architectural decision records
  docs/
    submission_answers.md # pre-written answers for hackathon submission
```

## Key Design Decisions

See [decisions.md](decisions.md) for full rationale. Highlights:

- **Customer matching**: Hash-based on (state + city + job + marital + balance) — pragmatic for POC
- **Date handling**: SQL `try_to_date` for graceful parsing of Excel serial fragments
- **Quality enforcement**: Per-entity quarantine tables with reason codes for audit trail
- **LLM integration**: Llama 3.1 70B via OpenAI SDK, structured JSON output with retry logic
- **RAG**: sentence-transformers + FAISS for semantic search over policy data
