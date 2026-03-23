# PrimeInsurance Data Platform POC

## Overview

Proof of Concept for unifying data from 6 acquired regional auto insurance systems onto the Databricks Intelligence Data Platform. Solves three critical business failures:

1. **Regulatory Compliance** - Unified, auditable customer registry (90-day deadline)
2. **Claims Backlog** - Single view of customer/policy/claims history (18 days → 7 days target)
3. **Revenue Leakage** - Cross-regional inventory visibility and aging alerts

## Architecture

**Medallion Architecture** on Databricks with Unity Catalog governance:

```
Raw Files (Volumes) → Bronze (DLT) → Silver (DLT) → Gold (DLT) → SQL Warehouse → GenAI
```

- **Bronze:** Raw ingestion with source tracking
- **Silver:** Schema harmonization, quality enforcement, deduplication, quarantine
- **Gold:** Star schema dimensional model with business views
- **GenAI:** Natural language querying, quality explanations, business insights

## Data Sources

| Dataset | Files | Format | Records |
|---------|-------|--------|---------|
| Customers | 7 | CSV | ~3,610 |
| Claims | 2 | JSON | 1,000 |
| Policy | 1 | CSV | 1,000 |
| Sales | 3 | CSV | ~4,982 |
| Cars | 1 | CSV | 2,500 |

## Project Structure

```
├── data/                    # Raw data files from regional systems
├── docs/                    # Architecture and design documents
├── notebooks/
│   ├── 00_setup/            # Catalog, schema, volume creation
│   ├── 01_bronze/           # Raw ingestion DLT pipelines
│   ├── 02_silver/           # Quality & harmonization DLT pipelines
│   ├── 03_gold/             # Dimensional model DLT pipelines
│   └── 04_genai/            # AI-powered insights and querying
```

## Setup

1. Create Databricks workspace
2. Run `notebooks/00_setup/01_create_catalog_schemas.sql`
3. Run `notebooks/00_setup/02_create_volumes.sql`
4. Upload data files from `data/` to corresponding volumes
5. Configure and run DLT pipelines (bronze → silver → gold)

## Team

| Member | Responsibility |
|--------|----------------|
| Neeraj Kumar | Infrastructure setup, Bronze layer |
| AK Singh | Silver layer, Gold layer |
| Paras Dhyani | Silver layer, Gold layer |
| Abhinav Sarkar | GenAI implementation |
