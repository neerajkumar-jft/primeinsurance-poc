# Architecture Design — PrimeInsurance Data Intelligence Platform

## High-Level Data Flow

```
Source Files (6 regions, CSV + JSON)
        │
        ▼
┌─────────────────────────────┐
│   BRONZE LAYER (Raw)        │
│   Auto Loader / COPY INTO   │
│   Delta Live Tables         │
│   Schema: primeins.bronze   │
│   ─────────────────────     │
│   bronze.customers          │
│   bronze.claims             │
│   bronze.policy             │
│   bronze.sales              │
│   bronze.cars               │
└─────────┬───────────────────┘
          │
          ▼
┌─────────────────────────────┐
│   SILVER LAYER (Clean)      │
│   DLT Expectations          │
│   Schema: primeins.silver   │
│   ─────────────────────     │
│   silver.customers          │
│   silver.claims             │
│   silver.policy             │
│   silver.sales              │
│   silver.cars               │
│   silver.dq_issues          │
│   silver.quarantine_*       │
└─────────┬───────────────────┘
          │
          ▼
┌─────────────────────────────────────┐
│   GOLD LAYER (Business-Ready)       │
│   Star Schema + Aggregations        │
│   Schema: primeins.gold             │
│   ───────────────────────────       │
│   DIMENSIONS:                       │
│     dim_customer                    │
│     dim_policy                      │
│     dim_car                         │
│     dim_date                        │
│     dim_region                      │
│   FACTS:                            │
│     fact_claims                     │
│     fact_sales                      │
│   AGGREGATIONS:                     │
│     agg_claims_by_region            │
│     agg_inventory_aging             │
│     agg_customer_360                │
│   AI OUTPUTS:                       │
│     dq_explanation_report           │
│     claim_anomaly_explanations      │
│     rag_query_history               │
│     ai_business_insights            │
└─────────┬───────────────────────────┘
          │
          ▼
┌─────────────────────────────┐
│   INTELLIGENCE LAYER        │
│   ─────────────────────     │
│   LLM: Llama 3.1 70B       │
│   RAG: FAISS + Embeddings   │
│   Genie Space (NL queries)  │
│   ai_query() for summaries  │
└─────────────────────────────┘
```

## Component Map

| Component | Tool/Feature | Layer | Role |
|-----------|-------------|-------|------|
| File Storage | Unity Catalog Volumes | Ingestion | Store raw source files |
| Ingestion | Auto Loader (cloudFiles) | Bronze | Incremental file loading with schema evolution |
| Pipeline Orchestration | Delta Live Tables | Bronze → Silver | Declarative pipeline with quality expectations |
| Quality Checks | DLT Expectations | Silver | Inline data validation |
| Quarantine | Custom Delta tables | Silver | Store failed/rejected records |
| Star Schema | Delta tables | Gold | Queryable dimensional model |
| Aggregations | Materialized Views / Delta | Gold | Pre-computed business KPIs |
| LLM | databricks-meta-llama-3.1-70b-instruct | Intelligence | Generate explanations, briefs, summaries |
| Vector Search | FAISS + Databricks Embeddings | Intelligence | Policy document retrieval (RAG) |
| Natural Language BI | Databricks Genie Space | Intelligence | Business user self-service queries |
| Governance | Unity Catalog | All | Lineage, access control, discoverability |

## Data Quality Strategy

| Layer | Quality Approach | What Happens on Failure |
|-------|-----------------|------------------------|
| Bronze | Schema validation only (preserve raw) | Log schema mismatch, still ingest |
| Silver | DLT Expectations — null checks, format validation, range checks, referential integrity | Record → quarantine table + `dq_issues` log |
| Gold | Referential integrity, aggregation sanity checks | Block promotion to Gold |

### Quality Rules by Entity
- **customers**: `customer_id` NOT NULL, valid region abbreviation, no exact duplicates
- **claims**: `claim_id` NOT NULL, valid date formats, amounts >= 0, valid severity values
- **policy**: `policy_number` NOT NULL, valid `policy_csl`, `premium > 0`, valid date
- **sales**: `sales_id` NOT NULL, valid `sold_on` date, `selling_price > 0`
- **cars**: `car_id` NOT NULL, `km_driven >= 0`, valid fuel type, non-empty model

## Gen AI Integration

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Gold Tables  │────▶│  Prompt      │────▶│  LLM (Llama  │
│  (structured) │     │  Construction │     │  3.1 70B)    │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │
                                                  ▼
                                          ┌──────────────┐
                                          │  Response     │
                                          │  Parsing +    │
                                          │  Delta Write  │
                                          └──────────────┘

RAG Flow (UC3):
dim_policy → Text Docs → Chunks → Embeddings → FAISS Index
                                                    │
User Question → Embed → Search Index → Top-K Chunks │
                                                    ▼
                                        Context + Question → LLM → Answer
```

## Entity Relationships (Star Schema)

```
                    ┌─────────────┐
                    │ dim_customer │
                    │─────────────│
                    │ customer_sk  │
                    │ customer_id  │
                    │ name, region │
                    │ state, city  │
                    └──────┬──────┘
                           │
┌───────────┐    ┌─────────┴─────────┐    ┌────────────┐
│ dim_date  │────│   fact_claims      │────│ dim_policy │
│───────────│    │───────────────────│    │────────────│
│ date_sk   │    │ claim_sk          │    │ policy_sk  │
│ date, yr  │    │ customer_sk (FK)  │    │ policy_num │
│ month, qtr│    │ policy_sk (FK)    │    │ csl, deduct│
└───────────┘    │ car_sk (FK)       │    │ premium    │
                 │ date_sk (FK)      │    └────────────┘
                 │ amount, severity  │
                 │ processing_days   │    ┌────────────┐
                 │ is_rejected       │────│  dim_car   │
                 └───────────────────┘    │────────────│
                                          │ car_sk     │
┌───────────┐    ┌───────────────────┐    │ model, fuel│
│ dim_date  │────│   fact_sales       │────│ mileage    │
│           │    │───────────────────│    └────────────┘
└───────────┘    │ sale_sk           │
                 │ car_sk (FK)       │
                 │ date_sk (FK)      │
                 │ region            │
                 │ selling_price     │
                 │ days_to_sell      │
                 └───────────────────┘
```
