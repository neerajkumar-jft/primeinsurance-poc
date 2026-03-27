# PrimeInsurance Data Intelligence Platform

End-to-end data platform on Databricks for a multi-region auto insurance company. Built as a POC to solve three business failures: inflated customer counts under regulatory scrutiny, an 18-day claims processing backlog, and revenue leakage from unsold inventory.

Team: Neeraj Kumar (lead), Abhinav Sarkar, AK Singh, Paras Dhyani

## Architecture

```
Source Files (14 files, 6 regions, CSV + JSON)
    |
    v
Unity Catalog Volumes (primeins.bronze.raw_data)
    |
    v
[BRONZE] DLT + Auto Loader -> 5 streaming tables (raw, as-is)
    |
    v
[SILVER] DLT Expectations -> 5 clean tables + 5 quarantine + dq_issues
    |
    v
[GOLD] Star schema (4 dims + 2 facts) + 4 materialized views
    |
    v
[GEN AI] 4 use cases on databricks-gpt-oss-20b
    |
    v
[SERVING] SQL Warehouse + Genie Space
```

Catalog: `primeins`
Schemas: `primeins.bronze`, `primeins.silver`, `primeins.gold`

## Repository structure

```
primeinsurance-poc/
  architecture.md                  # full platform design document
  README.md
  data/
    autoinsurancedata/             # 14 source files across 6 regional folders
  notebooks/
    setup/
      00_create_catalog_and_upload.py   # catalog, schemas, volume, permissions
    bronze/
      01_bronze_ingestion_dlt.py        # DLT pipeline, 5 streaming tables
    silver/
      02_silver_dlt_pipeline.py         # harmonization, quality rules, quarantine
    gold/
      03_gold_dlt_pipeline.py           # star schema, fact tables, materialized views
      05_executive_dashboard.py         # dashboard queries for all business metrics
    genai/
      04_uc1_dq_explanations.py         # DQ log -> plain English for compliance
      04_uc2_anomaly_engine.py          # fraud scoring + investigation briefs
      04_uc3_policy_rag.py              # RAG policy assistant (FAISS + embeddings)
      04_uc4_executive_insights.py      # KPI aggregation -> executive summaries
  docs/
    architecture-diagram.drawio         # main architecture diagram
    architecture-complete.drawio        # 4-page: data flow, components, DQ, Gen AI
    submissions/
      bronze_data_quality_findings.md   # Bronze table profiling results
      bronze_data_quality_findings.csv  # same, CSV format
      silver_quality_rules.csv          # 21 quality rules with justifications
      silver_quality_rules_genie_note.md
      dimensional_model_design.md       # star schema design document
      star_schema_diagram.drawio        # visual star schema diagram
      gold_issues_and_resolutions.md    # issues encountered during Gold build
```

## Pipeline

Three DLT pipelines run in sequence:

| Pipeline | Tables created | Notes |
|----------|---------------|-------|
| `primeins_bronze_pipeline` | customers, claims, policy, sales, cars | Auto Loader, incremental, schema evolution |
| `primeins_silver_pipeline` | 5 clean + 5 quarantine + dq_issues | DLT Expectations, harmonization, quarantine |
| `primeins_gold_pipeline` | 4 dims + 2 facts + 4 materialized views | Star schema, auto-refreshing aggregations |

Gen AI notebooks run as standalone jobs after the pipeline completes.

## Gold layer tables

| Table | Type | Purpose |
|-------|------|---------|
| dim_customer | Dimension | Deduplicated customers (1,604 from 3,605 raw) |
| dim_policy | Dimension | Policy coverage details, links customers to claims |
| dim_car | Dimension | Vehicle reference, bridges insurance and sales |
| dim_region | Dimension | 5-row lookup (East, West, Central, South, North) |
| fact_claims | Fact | 1 row per claim, amounts, rejection status, severity |
| fact_sales | Fact | 1 row per listing, days_listed, is_sold flag |
| mv_rejection_rate_by_policy | Materialized View | Rejection rate by coverage tier |
| mv_claims_by_severity | Materialized View | Claim stats by severity level |
| mv_unsold_inventory | Materialized View | Unsold cars by model and region |
| mv_claims_by_region | Materialized View | Claim volume and rejection by region |
| dq_explanation_report | AI Output | Plain English DQ explanations (UC1) |
| claim_anomaly_explanations | AI Output | Fraud investigation briefs (UC2) |
| rag_query_history | AI Output | Policy Q&A with citations (UC3) |
| ai_business_insights | AI Output | Executive summaries by domain (UC4) |

## Gen AI use cases

All four use databricks-gpt-oss-20b via OpenAI-compatible API. Shared infrastructure: extract_text() parser, Pydantic validation, PII guardrails, MLflow tracing, tenacity retry.

| UC | Name | Input | Output | What it does |
|----|------|-------|--------|-------------|
| 1 | DQ Explainer | silver.dq_issues | gold.dq_explanation_report | Translates 7 technical DQ issues to plain English |
| 2 | Claims Anomaly | silver.claims | gold.claim_anomaly_explanations | 5 fraud rules score 1,000 claims, LLM writes briefs for 128 flagged |
| 3 | RAG Policy Assistant | gold.dim_policy | gold.rag_query_history | FAISS + sentence-transformers, answers with cited policy numbers |
| 4 | Executive Insights | All Gold tables | gold.ai_business_insights | Aggregated KPIs to executive summaries for 3 domains |

## Data quality

| Layer | Approach | On failure |
|-------|----------|-----------|
| Bronze | No validation, preserve raw, mergeSchema for evolution | Log and continue |
| Silver | DLT Expectations (expect_or_drop, expect_or_fail, expect) | Route to quarantine table + log to dq_issues |
| Gold | Referential integrity between facts and dimensions | Block promotion |

## Known limitations

- Claim date fields (incident_date, claim_logged_on, claim_processed_on) are corrupted at source. Only time portions survived. Processing time in days cannot be calculated from this data. Synthetic processing days are generated in UC4 only.
- UC3 FAISS index rebuilds from scratch every run and lives in memory only. Not production-ready at scale.
