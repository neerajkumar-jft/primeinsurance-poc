# PrimeInsurance Demo Talking Points

## Business Problems Being Solved
1. **Regulatory Pressure**: Duplicate customers across 6 regions (~12% inflation), no auditable count, 90-day compliance deadline
2. **Claims Backlog**: 18-day avg processing time (industry benchmark: 7 days), no unified view across regions
3. **Revenue Leakage**: 69-78% unsold car inventory invisible across regions

---

## Architecture Decision: Why Scripts?

### Why `setup.sh` (Infrastructure as Code)
- Manually clicking through Unity Catalog UI to create catalog → schemas → volume is not repeatable
- New team member would have no idea what was created or why
- No audit trail — did someone already create this? With what settings?
- **Idempotent** — run it 10 times, same result. Safe for CI/CD pipelines
- Same reason teams use Terraform for cloud infrastructure — you don't click in AWS console, you write code
- One-liner: *"We treat infrastructure the same way we treat code — version controlled, automated, and repeatable"*

### Why `upload_data.sh` (Data Ingestion Script)
- PrimeInsurance has data from 6 regional offices — lives on local systems, not in Databricks
- Manual drag-and-drop doesn't scale when new regional files arrive monthly
- Script checks what's already uploaded (idempotent) — only uploads new files, no duplicates
- Simulates what would be a scheduled/triggered job in production
- This is the **Extract** part of ELT — pulling from source systems into the lakehouse

### Why `databricks.yml` (Databricks Asset Bundles)
- Same philosophy as setup.sh — infrastructure as code, but for Databricks resources
- One file defines all pipelines and jobs — version controlled in git
- `databricks bundle deploy` creates everything in the workspace from scratch
- `databricks bundle run full_pipeline` triggers the entire Bronze → Silver → Gold chain
- No clicking in the UI to create pipelines — fully automated, fully reproducible

---

## The 4-Command Demo Flow
```bash
git clone <repo>
cd primeinsurance-poc
databricks auth login          # authenticate once
./setup.sh                     # creates catalog, schemas, volume
./upload_data.sh               # uploads all 14 source files
databricks bundle deploy       # creates all 3 pipelines + orchestration job
databricks bundle run full_pipeline  # runs Bronze → Silver → Gold end to end
```
**Story**: "From a fresh clone to a fully running data platform in 4 commands."

---

## Medallion Architecture Layers

### Bronze Layer
- **What**: Raw ingestion of 14 source files (CSVs + JSONs) from 6 regional offices
- **How**: Delta Live Tables + Auto Loader (`cloudFiles`) — incremental, only processes new files
- **Why DLT**: Declarative, handles schema evolution, built-in lineage, restartable
- **Why Auto Loader**: Checkpoint-based — first run processes all files, second run processes zero (no duplicates)
- **Tables**: bronze_customers, bronze_claims, bronze_policy, bronze_sales, bronze_cars
- **Key fix**: `_metadata.file_path` instead of `input_file_name()` — Unity Catalog requirement

### Silver Layer
- **What**: Cleaned, standardized, quality-enforced data
- **How**: DLT pipeline reading from Bronze using `spark.readStream.table()` (cross-pipeline streaming read)
- **Key transforms**:
  - 7 customer files with 3 different ID column names → unified `customer_id`
  - Region abbreviations (W, E, S, C) → full names (West, East, South, Central)
  - Education typo 'terto' → 'tertiary'
  - Excel serial date fragments → proper dates
  - String "NULL"/"?" → actual nulls
- **DLT Expectations**: quality rules that drop bad records or flag them
- **Quarantine tables**: failed records preserved for compliance review (not deleted)
- **Tables**: silver_customers, silver_claims, silver_policy, silver_sales, silver_cars + 2 quarantine tables

### Why `spark.readStream.table()` in Silver (not `spark.read.table()`):
- `spark.read` = batch — reprocesses entire Bronze table every run
- `spark.readStream` = incremental — only picks up new Bronze records since last run
- DLT pipelines expect streaming DataFrames — `read` forces full refresh every time
- `dlt.read()` only works within the same pipeline — cross-pipeline reads need `spark.readStream.table()`

### Gold Layer
- **What**: Star schema — business-ready dimensional model
- **Dimensions**: dim_customer (deduplicated), dim_policy, dim_car, dim_region, dim_date
- **Facts**: fact_claims, fact_sales
- **Materialized Views** (pre-computed, auto-refreshed):
  - `mv_customer_count_by_region` → answers regulatory problem: true unique customer count
  - `mv_claims_performance` → answers claims problem: avg processing days + rejection rate by region
  - `mv_unsold_inventory` → answers revenue problem: cars sitting unsold by model/region
- dim_customer deduplication: scores each record by completeness, keeps most complete per customer_id

---

## Databricks Concepts Used

### Delta Live Tables (DLT)
- Declarative pipeline framework — describe what you want, Databricks handles how
- `@dlt.table` decorator turns a function into a managed table
- `@dlt.expect_or_drop` — quality rule that drops failing records
- `@dlt.expect` — quality rule that flags but keeps failing records
- Handles dependencies automatically — builds tables in right order
- Two modes: Triggered (runs once) vs Continuous (always on)

### Auto Loader (`cloudFiles`)
- `spark.readStream.format("cloudFiles")` — incremental file ingestion
- Maintains checkpoint — knows exactly which files were already processed
- `pathGlobFilter` — watch a directory but only process specific filename patterns
- `_metadata.file_path` — Unity Catalog equivalent of `input_file_name()`

### Unity Catalog
- Three-level namespace: `catalog.schema.table`
- Volumes: `dbfs:/Volumes/` — managed storage for files (replaces DBFS)
- Lineage: automatic end-to-end lineage from source files → bronze → silver → gold
- Access control at catalog/schema/table level

### Databricks Asset Bundles (DABs)
- `databricks.yml` defines all resources (pipelines, jobs)
- `${resources.pipelines.bronze_pipeline.id}` — reference between resources
- `${workspace.current_user.home}` — dynamic user home, works for any user on fresh clone
- `databricks bundle deploy` — creates/updates all resources
- `databricks bundle run <job>` — triggers execution

### Job Orchestration
- Job contains 3 tasks: bronze → silver → gold with `depends_on`
- `pipeline_task` — runs a DLT pipeline as a job task
- DAG visible in Jobs & Workflows UI — shows execution flow

---

## Key Engineering Decisions

| Decision | Alternative | Why We Chose This |
|---|---|---|
| DLT for all 3 layers | Plain notebooks | Declarative, lineage, quality built-in |
| Auto Loader (streaming) | `spark.read` batch | Incremental — no reprocessing, no duplicates |
| Separate Bronze/Silver/Gold pipelines | One big pipeline | Separation of concerns, can re-run independently |
| `spark.readStream.table()` cross-pipeline | `dlt.read()` | `dlt.read()` only works within same pipeline |
| DABs (`databricks.yml`) | Manual UI creation | Reproducible, version controlled, CI/CD ready |
| Idempotent scripts | One-time setup notebooks | Safe to re-run, production-grade pattern |
| Quarantine tables | Drop bad records | Compliance — preserve evidence of data issues |

---

## Scoring Coverage
| Phase | Weight | Status |
|---|---|--------|
| Architecture | 10% | Done |
| Bronze | 10% | Done |
| Silver | 20% | Done |
| Gold | 15% | Done |
| Gen AI | 30% | Pending |
| Submission | 15% | Pending |
