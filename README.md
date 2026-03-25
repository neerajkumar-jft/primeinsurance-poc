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

**Catalog**: `prime-ins-jellsinki-poc` (auto-created if not exists)
**Schemas**: `bronze`, `silver`, `gold`

## Quick Start

### Option 1: Databricks Repos (Recommended)

1. **Clone the repo in Databricks**
   - Go to **Repos** → **Add Repo**
   - URL: `https://github.com/neerajkumar-jft/primeinsurance-poc.git`
   - Branch: `main`

2. **Create the pipeline job**
   - Open `notebooks/00_create_pipeline` in the cloned repo
   - Attach any cluster and click **Run All**
   - The notebook auto-detects your repo path and creates the workflow job

3. **Run the pipeline**
   - Click the **Job URL** printed at the end of the notebook
   - Click **Run Now** on the job page
   - The pipeline runs all 16 tasks in the correct DAG order

### Option 2: CLI Deploy

```bash
# clone repo
git clone git@github.com:neerajkumar-jft/primeinsurance-poc.git
cd primeinsurance-poc

# authenticate with Databricks
databricks auth login --host https://<your-databricks-host>

# deploy notebooks + create pipeline job
./deploy/deploy.sh

# trigger pipeline
databricks jobs run-now <JOB_ID>
```

### Option 3: Manual Deploy

1. Import all notebooks from `notebooks/` to your Databricks workspace
2. Create a workflow job using `deploy/job_config.json`
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
                                                             ├── uc4_executive_insights
                                                             └── dashboard
```

- All 5 Silver notebooks run **in parallel**
- Gold waits for all Silver to complete
- UC1 runs as soon as DQ issues are ready
- UC2, UC3, UC4 run **in parallel** after Gold
- Dashboard runs after all AI use cases complete

## Notebooks

| # | Notebook | Layer | Description |
|---|----------|-------|-------------|
| 0 | `00_create_pipeline` | Setup | **Run once** — creates the workflow job automatically |
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
| 16 | `05_dashboard` | Dashboard | Interactive visualizations across all business problems |
| 17 | `06_create_dashboard` | Dashboard | Creates Lakeview dashboard via API (6 pages, charts, tables) |

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
primeinsurance-poc/
  README.md
  architecture.md           # system design with data flow diagrams
  key-facts.md              # architectural decision records (D1-D10)
  deploy/
    deploy.sh               # CLI deploy script
    job_config.json          # Databricks workflow job config (GIT source)
  notebooks/
    00_create_pipeline.py   # run once — creates job + triggers pipeline
    00_config.py            # shared catalog config (prime-ins-jellsinki-poc)
    00_setup.py             # creates catalog/schemas/volume, uploads data
    00_data_exploration.py  # profile all 5 entities, document inconsistencies
    01_bronze_ingestion.py  # load 14 source files as-is into Delta
    02_silver_*.py          # 6 notebooks: customers, claims, policy, sales, cars, dq_combined
    03_gold_dimensions.py   # dim_customer, dim_policy, dim_car, dim_date
    03_gold_facts.py        # fact_claims, fact_sales + business tables
    04_uc1_dq_explanations.py   # LLM explains DQ issues
    04_uc2_anomaly_engine.py    # statistical fraud scoring + LLM briefs
    04_uc3_policy_rag.py        # RAG over policy data (FAISS + embeddings)
    04_uc4_executive_insights.py # KPI aggregation to executive briefings
    05_dashboard.py         # notebook dashboard (display-based, safe_display)
    06_create_dashboard.py  # Lakeview dashboard (API, 6 pages, filters)
  data/
    autoinsurancedata/      # 14 source files (auto-uploaded to Volume)
  docs/
    submission_answers.md   # hackathon submission answers
```

## Dashboard Guide

The Lakeview dashboard (`06_create_dashboard`) creates a 6-page interactive dashboard:

| Page | What it Shows | Key Metrics |
|------|--------------|-------------|
| **Overview** | Executive health snapshot | Total Claims, Customers, Sell-Through %, SLA Breach %, Readiness Score |
| **Claims Performance** | Claims analysis with region/severity filters | Claims by Region (bar), Claims by Severity (bar), SLA Monitor (table) |
| **Customer Identity** | Deduplication results | Unique Customers, Duplicates Found, Dedup Rate, Customers by Region |
| **Revenue & Inventory** | Sales and aging inventory with region filter | Total Listings, Sold/Unsold, Sell-Through %, Aging Alerts |
| **Data Quality** | Pipeline health | DQ Issues by Entity (bar + table), AI-Generated Explanations |
| **AI Insights** | GenAI outputs | Executive Briefings, RAG Query History with cited policies |

**How to use:**
- **Filters**: Use dropdown filters on Claims and Revenue pages to drill down by region/severity
- **Counters**: Top-row KPI cards show aggregated metrics — click to see details
- **Tables**: Sortable data tables — click column headers to sort
- **Refresh**: Dashboard auto-refreshes when pipeline reruns. Click refresh icon for latest data.

**Readiness Score interpretation:**
- **AUDIT READY** (score >= 80): All systems green
- **NEEDS ATTENTION** (60-79): Some areas need work (typically data quality)
- **NOT READY** (< 60): Significant gaps in customer dedup, DQ, or claims processing

## Key Design Decisions

See [key-facts.md](key-facts.md) for full rationale. Highlights:

- **Customer matching**: Hash-based on (state + city + job + marital + balance) — pragmatic for POC
- **Date handling**: SQL `try_to_date` for graceful parsing of Excel serial fragments
- **Quality enforcement**: Per-entity quarantine tables with reason codes for audit trail
- **LLM integration**: `databricks-gpt-oss-20b` via OpenAI SDK, structured JSON output with retry logic
- **RAG**: sentence-transformers + FAISS for semantic search over policy data
