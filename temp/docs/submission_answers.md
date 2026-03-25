# Submission Answers - Ready to Copy-Paste

## M1: Architecture

### Q: Pipeline processing with bad records?
Our pipeline uses per-entity quality rules in the Silver layer. Each record is evaluated against rules (null checks, format validation, range checks). Records failing any rule are routed to dedicated quarantine tables (`silver.quarantine_customers`, `silver.quarantine_claims`, etc.) with a `_quarantine_reason` column listing exactly which rules failed and a `_quarantined_at` timestamp. Clean records proceed to Silver. The pipeline never stops for bad data, but nothing is silently dropped. The compliance team can query quarantine tables directly.

### Q: Unity Catalog for file governance?
We use Unity Catalog Volumes (`databricks-hackathon-insurance.bronze.workshop_data`) to store raw source files. This gives us governed storage where team members can discover files, verify what exists, and trace lineage from source file through Bronze to Gold. Unity Catalog enforces access control — only authorized users can read or write to each schema. The setup notebook auto-validates file presence and uploads missing files from our GitHub repo.

### Q: BI dashboard auto-refresh?
Gold layer tables are Delta tables served through Databricks SQL Warehouse. Pre-computed aggregations (`claims_sla_monitor`, `inventory_aging_alerts`, `regulatory_readiness`) update when the pipeline reruns. Dashboards connected to the SQL Warehouse automatically reflect the latest data. Genie Space queries Gold tables directly, so business users always see current numbers.

### Q: Dimensional model for cross-entity queries?
We designed a Kimball star schema with conformed dimensions (`dim_customer`, `dim_policy`, `dim_car`, `dim_date`) and two fact tables (`fact_claims`, `fact_sales`). The key insight: customers aren't directly linked to claims — the join path is claims → policy (via `policy_id`) → customer (via `customer_id`). Similarly, cars connect both worlds: policy → car (insured asset) and sales → car (inventory). A query like "rejection rate by policy type by region" joins `fact_claims` to `dim_policy` on `policy_sk` and groups by `policy_csl` and `incident_state`.

### Q: Gen AI layer placement?
The AI layer sits on top of Gold and Silver, never Bronze. Gold provides curated, business-ready data with consistent definitions. Each use case reads structured data, constructs prompts with business context, sends to the foundation model (Llama 3.1 70B) via OpenAI SDK, and writes AI-generated outputs back to Gold tables (`dq_explanation_report`, `claim_anomaly_explanations`, `rag_query_history`, `ai_business_insights`). This keeps AI outputs governed and queryable alongside regular business data.

---

## M2: Bronze

### Q: Schema evolution (new Birth Date column)?
The pipeline will NOT fail. We use `unionByName(allowMissingColumns=True)` when merging files, and `overwriteSchema=true` when writing to Delta. The new `Birth Date` column gets added to the table schema automatically. All existing records from other regions get NULL for this column. The column name with space is preserved as-is in Bronze — we handle renaming in Silver. No pipeline failure, no data loss.

### Q: How do the 7 customer files differ?
We found significant schema differences across the 7 files:
- **ID column**: `CustomerID` (files 1,4,5,6,7), `Customer_ID` (file 2), `cust_id` (file 3)
- **Region**: `Region` (most files), `Reg` (file 1)
- **City**: `City` (most files), `City_in_state` (file 2)
- **Education**: `Education` (most), `Edu` (file 2), missing entirely (file 4)
- **Marital**: `Marital` (most), `Marital_status` (files 1, 6), missing (file 2)
- **Job**: Present in most, missing from file 1
- **HHInsurance**: Missing from file 2

This is exactly the kind of mess that caused the ~12% inflated customer count — same person registered with different IDs and slightly different column structures across regions.

---

## M3: Silver

### Q: Inconsistencies found per entity?
| Entity | Issues Found | Region/File |
|--------|-------------|-------------|
| customers | 7 different column schemas (ID, region, city, education, marital all named differently), cross-region duplicates | All 7 files |
| claims | Date fields are Excel serial fragments ("27:00.0"), literal "NULL" strings, all values stored as strings | Both JSON files |
| policy | Date format needs parsing, typo in column name (`policy_deductable`) | Insurance 5 |
| sales | Datetime format `dd-MM-yyyy HH:mm` (with time), null `sold_on` for unsold cars, inconsistent file naming | Insurance 1,2,3 |
| cars | Numeric values mixed with units ("82 bhp", "190Nm@ 2000rpm"), need extraction | Insurance 4 |

### Q: Quality rules per table?
| Table | Rule | Trigger |
|-------|------|---------|
| customers | customer_id_null | customer_id IS NULL or empty (after unifying CustomerID/Customer_ID/cust_id) |
| customers | region_invalid | region NOT IN (East, West, North, South, Central) after standardization |
| customers | state_null | state IS NULL or empty |
| customers | cross_region_duplicates | Same person matched across regions via hash key |
| claims | claim_id_null | claimid IS NULL or empty |
| claims | policy_id_null | policyid IS NULL or empty |
| claims | incident_date_invalid | incident_date IS NULL (unparseable serial date) |
| claims | negative_amounts | injury < 0 OR property < 0 OR vehicle < 0 |
| policy | policy_number_not_null | policy_number IS NULL |
| policy | premium_positive | policy_annual_premium <= 0 |
| policy | customer_id_not_null | customer_id IS NULL |
| sales | sales_id_not_null | sales_id IS NULL |
| sales | price_positive | original_selling_price <= 0 |
| sales | days_on_lot_negative | calculated days_on_lot < 0 |
| cars | car_id_not_null | car_id IS NULL |
| cars | km_not_negative | km_driven < 0 |
| cars | fuel_invalid | fuel NOT IN (Diesel, Petrol, Cng, Lpg, Electric) |

### Q: Where do failed records go?
Failed records go to dedicated quarantine tables per entity (`quarantine_customers`, `quarantine_claims`, `quarantine_policy`, `quarantine_sales`, `quarantine_cars`). Each record includes the original data, a `_quarantine_reason` column listing which specific rules failed (comma-separated), and a `_quarantined_at` timestamp. Nothing disappears silently. The compliance team can query `SELECT * FROM silver.quarantine_claims WHERE _quarantine_reason LIKE '%claim_id_null%'` to see exactly what was rejected.

### Q: SQL Warehouse usage?
We used serverless compute for pipeline execution (notebook tasks in Databricks Workflows). For the serving layer, SQL Warehouse handles dashboard queries and Genie Space, providing low-latency, high-concurrency access to Gold tables. The separation means 20+ concurrent dashboard users don't impact pipeline performance.

---

## M4: Gold - Dimensional Model

### Q: Query walkthrough for "Which policy type had the highest claim rejection rate last quarter, broken down by region?"
**Tables touched**: `fact_claims`, `dim_policy`, `dim_date`

**Joins**:
- `fact_claims.policy_sk = dim_policy.policy_sk` (get policy_csl)
- `fact_claims.incident_date_sk = dim_date.date_sk` (filter by quarter)

```sql
SELECT dp.policy_csl, fc.incident_state as region,
       COUNT(*) as total_claims,
       SUM(CASE WHEN fc.claim_rejected = 'Y' THEN 1 ELSE 0 END) as rejected,
       ROUND(SUM(CASE WHEN fc.claim_rejected = 'Y' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as rejection_rate
FROM `databricks-hackathon-insurance`.gold.fact_claims fc
JOIN `databricks-hackathon-insurance`.gold.dim_policy dp ON fc.policy_sk = dp.policy_sk
JOIN `databricks-hackathon-insurance`.gold.dim_date dd ON fc.incident_date_sk = dd.date_sk
WHERE dd.quarter = 4 AND dd.year = 2024
GROUP BY dp.policy_csl, fc.incident_state
ORDER BY rejection_rate DESC
```

### Q: What business question can't your model answer?
Our model cannot answer questions about individual customer communication history, adjuster assignment, or claim workflow events (e.g., "who approved this claim?") since those aren't in the source files. To complete the picture, we'd add `dim_adjuster` and `fact_customer_interaction` tables.

### Q: Business queries for pre-computation?
1. **Claims SLA by region/severity/policy type** (`claims_sla_monitor`) — addresses claims backlog, queried daily by ops
2. **Unique customer count by region with dedup proof** (`regulatory_customer_registry`) — addresses regulatory problem, needed quarterly
3. **Aging inventory with cross-regional redistribution signals** (`inventory_aging_alerts`) — addresses revenue leakage, queried weekly
4. **Regulatory readiness score** (`regulatory_readiness`) — single KPI for leadership, queried on-demand

Each is pre-computed because they join multiple large tables and are queried repeatedly by different users. Recomputing on every query would be wasteful.

---

## M5: Gen AI

### Q: Prompt design strategy (UC1)?
We iterated through 3 versions:
- **V1**: Simple "explain this issue" → returned generic paragraphs, no structure, no business context
- **V2**: Added JSON output format + PrimeInsurance context → structured but inconsistent formatting, sometimes returned markdown instead of JSON
- **V3 (final)**: Added compliance officer persona, few-shot example showing exact expected format, strict JSON field definitions (explanation, business_impact, recommended_action, priority, estimated_effort)

Key learnings: the model needs a persona to set tone, a concrete example to set format, and explicit field names to produce parseable output. We added retry logic — if JSON parsing fails, we re-prompt with stricter instructions and retry up to 3 times.

### Q: Anomaly detection approach (UC2)?
5 weighted rules justified by data distributions:
1. **High claim amount (z-score > 2.0)** — Weight: 30% — Flags top ~2.3% by amount. Justified by computing mean and standard deviation of `total_claim_amount`.
2. **Severity-amount mismatch** — Weight: 25% — Minor/Trivial severity but amount above 75th percentile. Cross-tabulation of severity vs amount reveals clear outliers.
3. **Repeat claimant (3+ claims per policy)** — Weight: 20% — 95th percentile is ~2 claims per policy. 3+ is statistically unusual.
4. **Missing police report on major incidents** — Weight: 15% — Business rule: Major Damage and Total Loss should always have police reports.
5. **Suspiciously quick processing (<1 day)** — Weight: 10% — Known fraud pattern, lower weight because some expedited claims are legitimate.

Threshold: score >= 50 = HIGH priority. This flags a realistic number of claims for a fraud investigation team to review.

### Q: RAG retrieval flow (UC3)?
1. Each `dim_policy` row is converted to a natural language document (~100 tokens per policy)
2. Documents are encoded using `all-MiniLM-L6-v2` sentence transformer (384-dim embeddings). Falls back to TF-IDF if sentence-transformers isn't available.
3. Embeddings stored in FAISS index for fast cosine similarity search
4. User question → embed → search FAISS index → retrieve top-5 most relevant policies
5. Retrieved policy texts sent as context to Llama 3.1 70B with instructions to answer ONLY from provided data
6. Answer + source policy numbers + confidence score saved to `rag_query_history`

**Why convert structured data to text?** `dim_policy` is a table, not a document. LLMs work on natural language. Converting to text enables semantic retrieval — "which policies have umbrella coverage" matches documents mentioning "umbrella limit" even without exact keyword overlap.

**Chunk strategy**: 1 policy = 1 chunk, since each policy record is self-contained (~100 tokens). For actual PDF documents, we'd use 500-token chunks with 50-token overlap.

**"I don't know" detection**: If max similarity score < 0.05, the system returns "I don't have enough information" instead of hallucinating. Honest AI > confident-but-wrong AI.

### Q: All 4 UCs use same model but different prompts. Why?
Each use case has a different objective requiring different prompt structure:
- **UC1 (Explain)**: Compliance persona, structured JSON with impact/fix/priority fields
- **UC2 (Investigate)**: Fraud analyst persona, risk-focused with specific data points cited
- **UC3 (Answer)**: Grounded retrieval — must answer only from provided context, cite sources
- **UC4 (Summarize)**: Executive narrative with alerts, trends, and actionable recommendations

The model is the same, but the prompt guides it toward the correct output format, reasoning depth, and tone for each use case.

### Q: Which UC would need the most redesign for production?
**UC3 (RAG)** because:
- Needs scalable vector database (Databricks Vector Search) instead of in-memory FAISS
- Embedding index needs automated updates when policies change
- Latency optimization for real-time adjuster queries (<2 second response time)
- Access control — adjusters should only see policies in their region
- Multi-turn conversation support for follow-up questions
- Evaluation framework to measure retrieval quality and answer accuracy over time

### Q: What happens if the LLM returns a malformed response?
Our code includes retry logic with 3 attempts. On JSON parse failure, we strip markdown code blocks and retry parsing. If still invalid, we re-prompt with "Return ONLY valid JSON, no markdown." After all retries fail, we write a structured error response to the output table (pipeline doesn't crash) so failed records are visible for manual review.

---

## M6: Final Submission

### Q: What surprised you?
- Customer schema differences were far more extensive than expected — 7 completely different column naming patterns across files, not just ID format differences
- Claims date fields contained Excel serial number fragments ("27:00.0") instead of actual dates — required creative parsing with `try_to_date`
- Customer deduplication revealed approximately [X]% overlap across regions (fill in actual number)
- Prompt engineering required 3 iterations — V1 was completely unusable, V3 with persona + few-shot examples produced compliance-ready structured output

### Q: What would you do differently?
- Use Structured Streaming for real-time claims processing instead of batch
- Train an ML fraud detection model on historical confirmed-fraud labels instead of rule-based scoring
- Use Databricks Vector Search (managed service) instead of in-memory FAISS for production RAG
- Add automated alerting (Slack/email) when high-priority anomalies or data quality regressions are detected
- Implement Delta Live Tables for the full pipeline for better monitoring and automatic dependency management
- Build a customer self-service claims status portal powered by the Gold layer
