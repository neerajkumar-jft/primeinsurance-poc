# PrimeInsurance Data Platform - Architecture Design

## 1. Overview

PrimeInsurance operates across 5 US regions with data fragmented across 6 acquired regional systems. This architecture unifies all data onto the Databricks Intelligence Data Platform using a medallion architecture (Bronze/Silver/Gold) governed by Unity Catalog.

### Current State Failures This Architecture Solves

| Current Failure | Root Cause | Architecture Solution |
|----------------|------------|----------------------|
| No shared pipeline | Disconnected scripts, notebooks, SQL jobs | Delta Live Tables (DLT) — single declarative pipeline framework |
| No quality checks | Each team defines rules inconsistently | DLT Expectations — quality rules declared in pipeline SQL |
| No dependency management | Downstream tables silently serve stale data | DLT dependency graph — automatic orchestration, failure propagation |
| No lineage | Cannot trace a number back to source file | Unity Catalog lineage — automatic column-level tracking |
| Full reprocessing every run | No incremental processing | DLT streaming tables with Auto Loader — process only new files |

---

## 2. Data Layers

### 2.1 Bronze Layer (Raw Ingestion)

**Purpose:** Land raw data exactly as received from regional systems. No transformation, no filtering.

| Property | Value |
|----------|-------|
| Schema | `prime_insurance_jellsinki_poc.bronze` |
| Input | Raw files from Volumes (CSV, JSON) |
| Output | Delta tables with original columns + metadata |
| Processing | Auto Loader (streaming) / COPY INTO (batch) |
| Quality | Schema enforcement only — no business rules |

**What enters:** Raw CSV/JSON files from 6 regional systems, exactly as exported.

**What leaves:** Delta tables with all original columns preserved, plus:
- `_source_file` — full path to the originating file
- `_ingestion_timestamp` — when the record was loaded
- `_source_region` — which regional system it came from

**Tables:**
| Table | Source | Format | Records |
|-------|--------|--------|---------|
| `raw_customers_1` through `raw_customers_7` | 7 CSV files | CSV | ~3,610 |
| `raw_claims_1`, `raw_claims_2` | 2 JSON files | JSON | 1,000 |
| `raw_policy` | 1 CSV file | CSV | 1,000 |
| `raw_sales_1`, `raw_sales_2`, `raw_sales_3` | 3 CSV files | CSV | ~4,982 |
| `raw_cars` | 1 CSV file | CSV | 2,500 |

### 2.2 Silver Layer (Data Quality & Harmonization)

**Purpose:** Standardize schemas, enforce quality, deduplicate, quarantine bad records.

| Property | Value |
|----------|-------|
| Schema | `prime_insurance_jellsinki_poc.silver` |
| Input | Bronze Delta tables |
| Output | Cleaned, harmonized Delta tables + quarantine tables |
| Processing | DLT SQL with Expectations |
| Quality | Business rules enforced — invalid records quarantined |

**What enters:** Raw records with inconsistent schemas, duplicates, corrupted dates, missing fields.

**What leaves:**
- **Clean tables** — standardized column names, expanded abbreviations, parsed dates, unified types
- **Quarantine tables** — failed records preserved with failure reason for compliance investigation

**Transformations:**
1. **Schema harmonization** — unify 3 different customer ID column names, 4 region column variants, etc.
2. **Value standardization** — expand abbreviated regions (W→West), fix typos (terto→tertiary), normalize dates to ISO 8601
3. **Null handling** — replace `?` with NULL, fill missing columns with NULL
4. **Deduplication** — resolve cross-regional customer duplicates (customers_1 and customers_7 share IDs)
5. **Quality enforcement** — DLT expectations validate every record; failures routed to quarantine

**Tables:**
| Table | Description |
|-------|-------------|
| `customers_harmonized` | All 7 sources unified to single schema |
| `customers_unified` | Deduplicated customer registry (regulatory requirement) |
| `claims_cleaned` | Standardized dates, nulls, types |
| `policy_cleaned` | Standardized policy records |
| `sales_cleaned` | Normalized dates, unsold inventory flagged |
| `cars_cleaned` | Standardized vehicle catalog |
| `quarantine_customers` | Customer records that failed validation |
| `quarantine_claims` | Claims records that failed validation |
| `quarantine_sales` | Sales records that failed validation |

### 2.3 Gold Layer (Dimensional Model)

**Purpose:** Business-ready star schema optimized for analytics and AI consumption.

| Property | Value |
|----------|-------|
| Schema | `prime_insurance_jellsinki_poc.gold` |
| Input | Silver Delta tables |
| Output | Dimension tables, fact tables, business views |
| Processing | DLT SQL joins + aggregations |
| Quality | Referential integrity checks |

**What enters:** Clean, deduplicated records from Silver.

**What leaves:** Star schema with dimensions and facts, plus pre-built business views.

---

## 3. Pipeline Design

### 3.1 Why Delta Live Tables (DLT)

DLT solves every failure in the current state:

| Current Failure | How DLT Fixes It |
|----------------|-------------------|
| Disconnected scripts | Single declarative pipeline — all transformations in one place |
| No dependency management | DLT builds a DAG automatically — if a source fails, downstream tables are not updated |
| No quality checks | Expectations are declared inline — quality is part of the pipeline, not an afterthought |
| Full reprocessing | Streaming tables + Auto Loader process only new/changed files |
| No lineage | DLT + Unity Catalog provide automatic column-level lineage |

### 3.2 Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DLT Pipeline                                     │
│                                                                          │
│  ┌─────────┐     ┌──────────┐     ┌──────────┐                         │
│  │ Volumes  │────▶│  Bronze  │────▶│  Silver  │────▶┌──────────┐       │
│  │ (files)  │     │  (raw)   │     │ (clean)  │     │   Gold   │       │
│  └─────────┘     └──────────┘     └────┬─────┘     │  (model) │       │
│                                        │            └──────────┘       │
│                                        ▼                                │
│                                  ┌────────────┐                         │
│                                  │ Quarantine  │                         │
│                                  │  (failed)   │                         │
│                                  └────────────┘                         │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                              ┌──────────────────┐
                              │   SQL Warehouse   │
                              │  (Serving Layer)  │
                              └────────┬─────────┘
                                       │
                          ┌────────────┼────────────┐
                          ▼            ▼            ▼
                    ┌──────────┐ ┌──────────┐ ┌──────────┐
                    │ Dashboards│ │ GenAI    │ │ Notebooks│
                    │ (BI)     │ │ (AI/SQL) │ │ (Adhoc)  │
                    └──────────┘ └──────────┘ └──────────┘
```

### 3.3 Pipeline Execution Model

- **Mode:** Triggered (scheduled) — not continuous for POC
- **Incremental:** Auto Loader tracks processed files; only new files trigger processing
- **Idempotent:** Re-running the pipeline produces the same result
- **Failure handling:** DLT stops downstream tables if upstream fails — no stale data served

### 3.4 DLT Notebook Organization

```
notebooks/
├── 00_setup/
│   ├── 01_create_catalog_schemas.sql    -- Catalog, schemas, volumes
│   ├── 02_create_volumes.sql            -- Single raw_data volume for all source files
│   └── 03_upload_data_to_volumes.py    -- Copies 14 files preserving folder structure
├── 01_bronze/
│   ├── bronze_customers.sql             -- Raw ingestion of 7 customer CSV files
│   ├── bronze_claims.sql                -- Raw ingestion of 2 claims JSON files
│   ├── bronze_policy.sql                -- Raw ingestion of policy CSV
│   ├── bronze_sales.sql                 -- Raw ingestion of 3 sales CSV files
│   └── bronze_cars.sql                  -- Raw ingestion of cars CSV
├── 02_silver/
│   ├── silver_customers.sql             -- Customer harmonization + dedup
│   ├── silver_claims.sql                -- Claims cleaning
│   ├── silver_policy.sql                -- Policy cleaning
│   ├── silver_sales.sql                 -- Sales cleaning + unsold flagging
│   └── silver_cars.sql                  -- Cars cleaning
├── 03_gold/
│   ├── gold_customers.sql               -- dim_customers
│   ├── gold_cars.sql                    -- dim_cars
│   ├── gold_policies.sql               -- dim_policies
│   ├── gold_claims.sql                  -- fact_claims
│   ├── gold_sales.sql                   -- fact_sales
│   └── gold_views.sql                   -- Business views
└── 04_genai/
    ├── ai_data_quality_monitor.py       -- AI quality explanations
    ├── ai_query_interface.py            -- Natural language to SQL
    └── ai_business_insights.py          -- Automated insight generation
```

---

## 4. Data Quality Strategy

### 4.1 Quality Checks by Layer

| Layer | Check Type | Mechanism | On Failure |
|-------|-----------|-----------|------------|
| Bronze | Schema enforcement | Auto Loader schema hints | Rescue column (`_rescued_data`) |
| Bronze | File tracking | `_source_file` metadata | N/A — always captured |
| Silver | NOT NULL on key fields | DLT `EXPECT ... ON VIOLATION DROP ROW` | Row quarantined |
| Silver | Valid value ranges | DLT `EXPECT ... ON VIOLATION DROP ROW` | Row quarantined |
| Silver | Referential integrity (soft) | DLT `EXPECT` (warn only) | Warning logged, row passes |
| Silver | Deduplication | Window functions (ROW_NUMBER) | Duplicate removed, latest kept |
| Gold | Referential integrity (hard) | DLT `EXPECT ... ON VIOLATION DROP ROW` | Orphan dropped |
| Gold | Aggregation validation | DLT `EXPECT` (warn only) | Warning logged |

### 4.2 DLT Expectation Behavior Matrix

```sql
-- WARN: Record passes through, violation logged in event log
CONSTRAINT warn_low_balance EXPECT (balance >= 0)

-- DROP: Record removed from target, sent to quarantine, pipeline continues
CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW

-- FAIL: Pipeline stops entirely — for critical structural issues
CONSTRAINT valid_schema EXPECT (_rescued_data IS NULL) ON VIOLATION FAIL UPDATE
```

### 4.3 Quarantine Pattern

Every entity has a corresponding quarantine table in Silver:

```sql
-- Clean records: pass expectations
CREATE OR REFRESH STREAMING TABLE silver.customers_harmonized (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_region EXPECT (region IN ('East','West','Central','South','North')) ON VIOLATION DROP ROW
) AS SELECT ...

-- Quarantine: captures what failed and why
CREATE OR REFRESH STREAMING TABLE silver.quarantine_customers
AS SELECT *, 'failed_validation' AS quarantine_reason, current_timestamp() AS quarantined_at
   FROM STREAM(bronze.raw_customers_all)
   WHERE customer_id IS NULL OR region NOT IN ('East','West','Central','South','North')
```

The compliance team queries quarantine tables directly. Every quarantined record has:
- Original data (unchanged)
- Quarantine reason
- Timestamp
- Source file traceability (via `_source_file` from Bronze)

### 4.4 Quality Metrics

DLT automatically captures expectation metrics in the event log:
- Records passing/failing each rule
- Percentage compliance per expectation
- Trends over time

These are surfaced via the GenAI layer as plain-English quality reports.

---

## 5. Analytical Model (Gold Layer)

### 5.1 Star Schema Design

```
                    ┌──────────────┐
                    │  dim_date    │
                    │──────────────│
                    │ date_key (PK)│
                    │ full_date    │
                    │ year         │
                    │ quarter      │
                    │ month        │
                    │ day_of_week  │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│dim_customers │    │ fact_claims  │    │ dim_policies  │
│──────────────│    │──────────────│    │──────────────│
│customer_key  │◀───│customer_key  │───▶│policy_key    │
│customer_id   │    │policy_key    │    │policy_number │
│region        │    │car_key       │    │policy_state  │
│state         │    │incident_date │    │policy_csl    │
│city          │    │claim_amount  │    │deductible    │
│job           │    │injury_amount │    │annual_premium│
│marital_status│    │property_amt  │    │umbrella_limit│
│education     │    │vehicle_amt   │    │customer_key  │
│default_flag  │    │severity      │    │car_key       │
│balance       │    │is_rejected   │    └──────────────┘
│hh_insurance  │    │days_to_log   │
│car_loan      │    │days_to_process│   ┌──────────────┐
│is_duplicate  │    └──────────────┘    │  dim_cars     │
│source_count  │                        │──────────────│
└──────────────┘    ┌──────────────┐    │car_key       │
                    │ fact_sales   │───▶│car_id        │
                    │──────────────│    │name          │
                    │sales_id      │    │fuel          │
                    │car_key       │    │transmission  │
                    │region        │    │mileage       │
                    │selling_price │    │engine        │
                    │ad_placed_date│    │max_power     │
                    │sold_date     │    │seats         │
                    │days_to_sell  │    │model         │
                    │is_sold       │    └──────────────┘
                    │is_aging      │
                    │aging_bucket  │
                    └──────────────┘
```

### 5.2 Key Measures & Calculations

| Measure | Table | Calculation | Business Problem |
|---------|-------|-------------|-----------------|
| `days_to_sell` | fact_sales | `DATEDIFF(sold_date, ad_placed_date)` | Revenue leakage — aging detection |
| `is_aging` | fact_sales | `sold_date IS NULL AND DATEDIFF(current_date, ad_placed_date) > 90` | Unsold inventory alert |
| `aging_bucket` | fact_sales | 0-30, 31-60, 61-90, 90+ days | Inventory aging distribution |
| `days_to_log` | fact_claims | `DATEDIFF(claim_logged_on, incident_date)` | Claims backlog measurement |
| `days_to_process` | fact_claims | `DATEDIFF(claim_processed_on, claim_logged_on)` | Processing time vs 7-day benchmark |
| `source_count` | dim_customers | Count of regional systems a customer appears in | Duplicate detection for audit |
| `is_duplicate` | dim_customers | `source_count > 1` | Regulatory compliance — inflated count |

### 5.3 Business Views

| View | Purpose | Key Columns |
|------|---------|-------------|
| `vw_customer_360` | Single customer view across all policies, claims, sales | customer_id, policies[], claims[], total_premium |
| `vw_aging_inventory` | Cars unsold > 90 days with cross-region availability | car_id, region, days_listed, sold_in_other_region |
| `vw_claims_processing` | Claim times vs benchmark with adjuster workload | claim_id, days_to_process, benchmark_delta |
| `vw_revenue_leakage` | Revenue lost from aged/discounted inventory | region, aging_bucket, potential_revenue_loss |
| `vw_regulatory_customer_count` | Auditable unique customer count with dedup proof | unique_count, duplicate_count, inflation_pct |

---

## 6. Serving Layer

### 6.1 Why a Separate SQL Warehouse

The pipeline (DLT) and the serving layer (SQL Warehouse) **must be separate** because:

| Concern | DLT Pipeline | SQL Warehouse |
|---------|-------------|---------------|
| Purpose | Transform and load data | Serve queries to users |
| Compute | Runs on pipeline clusters (ephemeral) | Runs on warehouse endpoints (persistent) |
| Scaling | Scales for data volume | Scales for query concurrency |
| Cost | Pay per pipeline run | Pay per query / per hour |
| Access | Data engineers only | Business users, dashboards, AI |

If analytics queries ran on pipeline compute, a heavy dashboard refresh could block data processing. If pipelines ran on warehouse compute, costs would be unpredictable and resource contention would affect query latency.

### 6.2 SQL Warehouse Configuration

- **Type:** Serverless SQL Warehouse (auto-scaling, zero idle cost)
- **Access:** Gold schema tables and views only
- **Users:** Business analysts, compliance officers, executives, GenAI layer
- **Interfaces:**
  - Databricks SQL Editor — ad-hoc queries
  - Dashboards — scheduled visual reports
  - AI/BI — natural language queries
  - External BI tools (Tableau, Power BI) via JDBC/ODBC

### 6.3 Access Control

```
Unity Catalog Permissions:
├── Data Engineers  → ALL on bronze, silver, gold
├── Analysts        → SELECT on gold only
├── Compliance      → SELECT on gold + silver.quarantine_*
└── GenAI Service   → SELECT on gold + silver (read-only)
```

---

## 7. GenAI Integration

### 7.1 Where AI Plugs In

```
Gold Layer (star schema)
    │
    ├──▶ AI Data Quality Monitor
    │    Input:  DLT event log + quarantine tables
    │    Output: Plain-English quality reports
    │    Solves: "Why were 200 records rejected yesterday?"
    │
    ├──▶ AI Query Interface (Text-to-SQL)
    │    Input:  Natural language question
    │    Output: SQL query + results + explanation
    │    Solves: "How many unique customers do we have?" (no SQL needed)
    │
    └──▶ AI Business Insights
         Input:  Gold tables + views
         Output: Proactive alerts + trend analysis
         Solves: "34 cars aging >90 days in South — same models sold in West in 2 weeks"
```

### 7.2 GenAI Use Cases

| Use Case | Business Problem | Data Consumed | Data Produced |
|----------|-----------------|---------------|---------------|
| **Data Quality Explanations** | Compliance officer needs to understand why records were quarantined | DLT event log, quarantine tables, expectation metrics | Natural language report: "47 customer records from East region were quarantined because CustomerID was null. This is 2.3% of the batch." |
| **Natural Language Querying** | Executives want answers without writing SQL or waiting for engineering | Gold schema tables + views | SQL query + formatted results + plain-English answer |
| **Aging Inventory Alerts** | Sales managers can't see cross-regional inventory status | fact_sales, dim_cars, vw_aging_inventory | "Region South has 34 cars unsold for 90+ days. Model X sold out in West in 14 days. Recommend redistribution." |
| **Claims Backlog Analysis** | Management needs to understand processing bottlenecks | fact_claims, dim_policies, vw_claims_processing | "Average processing time is 18 days. Top bottleneck: claims requiring cross-system policy lookup. Unified view would cut 7 days." |
| **Regulatory Audit Support** | Auditors need defensible customer counts | dim_customers, vw_regulatory_customer_count | "Total unique customers: 8,847. Duplicates resolved: 1,062 (12%). Each dedup decision traceable to source records." |

### 7.3 Implementation Approach

- **Foundation Model:** Databricks Foundation Model APIs (DBRX / Llama) or external (GPT-4)
- **Vector Store:** Databricks Vector Search for schema/documentation embedding
- **Orchestration:** Python notebooks calling Foundation Model APIs with gold-layer context
- **Guardrails:** AI responses always include the SQL query used — users can verify
- **AI/BI:** Databricks AI/BI enables natural language querying directly in the SQL interface — requires descriptive table/column names and Unity Catalog comments on all Gold objects

### 7.4 AI output tables

AI-generated outputs are stored in Gold so they're queryable through the same SQL Warehouse:

| Table | Content | Consumer |
|-------|---------|----------|
| `gold.ai_quality_reports` | Plain-English data quality summaries from DLT event log + quarantine analysis | Compliance team dashboards |
| `gold.ai_claims_alerts` | Anomalous claims flagged with investigation briefs | Operations team, claims adjusters |

Natural language query results from AI/BI are not persisted — they're returned directly in the interface where the user asked the question.

---

## 8. Unity Catalog Governance

### 8.1 Catalog Structure

```
prime_insurance_jellsinki_poc (catalog)
│
├── bronze (schema)
│   ├── raw_data (volume)    -- All 14 source files, mirrors data/ folder structure
│   │   ├── Insurance 1/     -- customers_1.csv, Sales_2.csv
│   │   ├── Insurance 2/     -- customers_2.csv, sales_1.csv
│   │   ├── Insurance 3/     -- customers_3.csv, sales_4.csv
│   │   ├── Insurance 4/     -- customers_4.csv, cars.csv
│   │   ├── Insurance 5/     -- customers_5.csv, policy.csv
│   │   ├── Insurance 6/     -- customers_6.csv, claims_1.json
│   │   ├── customers_7.csv  -- Root-level file
│   │   └── claims_2.json    -- Root-level file
│   └── raw_* tables         -- 14 DLT streaming tables
│
├── silver (schema)
│   ├── *_cleaned tables     -- 6 harmonized entity tables
│   ├── customers_unified    -- Deduplicated customer registry
│   └── quarantine_* tables  -- 3 quarantine tables
│
└── gold (schema)
    ├── dim_* tables         -- 4 dimension tables
    ├── fact_* tables        -- 2 fact tables
    └── vw_* views           -- 5 business views
```

### 8.2 Lineage

Unity Catalog provides **automatic lineage** from source file to gold view:

```
Volume file (customers_3.csv)
  → bronze.raw_customers_3 (_source_file tracked)
    → silver.customers_harmonized (schema unified)
      → silver.customers_unified (deduplicated)
        → gold.dim_customers (surrogate keys added)
          → gold.vw_regulatory_customer_count (aggregated)
```

Every number in a compliance report traces back to the exact source file and record.

---

## 9. Summary: Architecture Maps to Business Failures

| Business Failure | Architecture Component | How It Solves |
|-----------------|----------------------|---------------|
| **Regulatory: inflated customer count** | Silver dedup → Gold dim_customers → vw_regulatory_customer_count | Unified, auditable count with lineage from source to report |
| **Claims: 18-day processing time** | Silver unified records → Gold fact_claims + customer 360 view | Single view of customer, policy, claims history — no manual cross-referencing |
| **Revenue: aging inventory unnoticed** | Silver sales_cleaned → Gold fact_sales + vw_aging_inventory + AI alerts | Cross-regional visibility, automatic aging detection, redistribution recommendations |
| **No quality enforcement** | DLT Expectations at Silver layer with quarantine | Rules declared in pipeline, failures captured and explained, pipeline continues |
| **No lineage** | Unity Catalog + `_source_file` metadata | Every record traceable from source file to gold view |
| **Full reprocessing** | DLT streaming tables + Auto Loader | Incremental — only new files processed |
