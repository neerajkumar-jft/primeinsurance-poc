#!/usr/bin/env python3
"""Generate consolidated hackathon submission Q&A PDF."""

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.lib import colors
import os

OUTPUT_PATH = os.path.expanduser("~/Workspace/hackathon/docs/hackathon_submission_answers.pdf")

doc = SimpleDocTemplate(
    OUTPUT_PATH,
    pagesize=A4,
    leftMargin=20*mm,
    rightMargin=20*mm,
    topMargin=20*mm,
    bottomMargin=20*mm,
)

styles = getSampleStyleSheet()

# Custom styles
styles.add(ParagraphStyle(
    name='ModuleHeader',
    parent=styles['Heading1'],
    fontSize=16,
    textColor=HexColor('#1a5276'),
    spaceAfter=6,
    spaceBefore=12,
    fontName='Helvetica-Bold',
))

styles.add(ParagraphStyle(
    name='QuestionNum',
    parent=styles['Heading2'],
    fontSize=12,
    textColor=HexColor('#2c3e50'),
    spaceAfter=4,
    spaceBefore=10,
    fontName='Helvetica-Bold',
))

styles.add(ParagraphStyle(
    name='QuestionText',
    parent=styles['Normal'],
    fontSize=10,
    textColor=HexColor('#34495e'),
    spaceAfter=6,
    spaceBefore=2,
    fontName='Helvetica-Oblique',
    leading=14,
    leftIndent=10,
    rightIndent=10,
    backColor=HexColor('#f0f4f8'),
))

styles.add(ParagraphStyle(
    name='AnswerBody',
    parent=styles['Normal'],
    fontSize=10,
    textColor=HexColor('#2c3e50'),
    spaceAfter=4,
    spaceBefore=4,
    fontName='Helvetica',
    leading=14,
    alignment=TA_JUSTIFY,
))

styles.add(ParagraphStyle(
    name='AnswerBullet',
    parent=styles['Normal'],
    fontSize=10,
    textColor=HexColor('#2c3e50'),
    spaceAfter=2,
    spaceBefore=1,
    fontName='Helvetica',
    leading=13,
    leftIndent=20,
    bulletIndent=10,
))

styles.add(ParagraphStyle(
    name='CodeBlock',
    parent=styles['Normal'],
    fontSize=8,
    textColor=HexColor('#2c3e50'),
    spaceAfter=4,
    spaceBefore=4,
    fontName='Courier',
    leading=11,
    leftIndent=15,
    rightIndent=10,
    backColor=HexColor('#f8f9fa'),
))

styles.add(ParagraphStyle(
    name='CopyMarker',
    parent=styles['Normal'],
    fontSize=8,
    textColor=HexColor('#7f8c8d'),
    spaceAfter=2,
    spaceBefore=6,
    fontName='Helvetica-Bold',
))

styles.add(ParagraphStyle(
    name='TitleStyle',
    parent=styles['Title'],
    fontSize=20,
    textColor=HexColor('#1a5276'),
    spaceAfter=4,
    fontName='Helvetica-Bold',
    alignment=TA_CENTER,
))

styles.add(ParagraphStyle(
    name='SubTitle',
    parent=styles['Normal'],
    fontSize=11,
    textColor=HexColor('#5d6d7e'),
    spaceAfter=20,
    fontName='Helvetica',
    alignment=TA_CENTER,
))

story = []

# Title page
story.append(Spacer(1, 40*mm))
story.append(Paragraph("PrimeInsurance POC", styles['TitleStyle']))
story.append(Paragraph("Hackathon Submission - Questions &amp; Answers", styles['SubTitle']))
story.append(Spacer(1, 10*mm))
story.append(Paragraph("Team: Neeraj Kumar, Aman Kumar Singh, Paras Bansal, Abhinav", styles['SubTitle']))
story.append(Paragraph("Catalog: prime-ins-jellsinki-poc | Model: databricks-gpt-oss-20b", styles['SubTitle']))
story.append(PageBreak())


def hr():
    return HRFlowable(width="100%", thickness=0.5, color=HexColor('#bdc3c7'), spaceBefore=6, spaceAfter=6)

def module(title):
    story.append(Paragraph(title, styles['ModuleHeader']))
    story.append(hr())

def question(num, text):
    story.append(Paragraph(f"Question {num}", styles['QuestionNum']))
    story.append(Paragraph(text, styles['QuestionText']))
    story.append(Paragraph("Answer - copy from here", styles['CopyMarker']))

def answer(text):
    story.append(Paragraph(text, styles['AnswerBody']))

def bullet(text):
    story.append(Paragraph(f"\u2022 {text}", styles['AnswerBullet']))

def code(text):
    story.append(Paragraph(text.replace('\n', '<br/>').replace(' ', '&nbsp;'), styles['CodeBlock']))

def end_answer():
    story.append(Paragraph("Answer - copy till here", styles['CopyMarker']))
    story.append(Spacer(1, 4*mm))


# ========================================================
# MODULE 1: ARCHITECTURE (Q4-Q8 from screenshots 12-16)
# ========================================================
module("Module 1: Architecture &amp; Design")

# Q4 (12.png)
question("4", "PrimeInsurance's pipeline processes files from 4 regional systems daily. During one run, 200 customer records fail validation - some have a missing customer_id, others have an unrecognized Reg value. The pipeline must continue processing without stopping, clean records must reach Silver, and failed records must be preserved for the compliance team. Which Databricks component enables this, and how do you ensure failed records are captured and tagged?")

answer("We handle this in the Silver layer through per-entity quality rules and quarantine tables. No DLT Expectations needed for this - we built it directly in PySpark because we wanted full control over the quarantine routing logic.")

answer("Each Silver notebook (02_silver_customers.py, 02_silver_claims.py, etc.) defines quality rules as PySpark conditions. For customers, there are three rules: customer_id_null (after coalescing CustomerID/Customer_ID/cust_id), region_invalid (value not in East/West/North/South/Central after standardization), and state_null. Every record gets flagged per rule as a boolean column (_fail_customer_id_null, etc.), then an overall _quality_passed column is computed as the negation of any failure.")

answer("Records that fail get written to silver.quarantine_customers with two extra columns: _quarantine_reason (comma-separated list of which rules triggered, e.g. 'region_invalid, state_null') and _quarantined_at (timestamp). Records that pass flow into silver.customers. The pipeline never stops for bad data, and nothing disappears silently.")

answer("The compliance team can query quarantine tables directly:")
code("SELECT * FROM silver.quarantine_customers\nWHERE _quarantine_reason LIKE '%customer_id_null%'")

answer("This pattern repeats for every entity - claims, policy, sales, cars - each with its own quarantine table and entity-specific rules.")

end_answer()

# Q5 (13.png)
question("5", "PrimeInsurance has 14 raw files from 4 regional systems, in CSV and JSON formats, stored across regional sub-folders. These files need to be stored in Databricks in a way that is: Cataloged and discoverable, Idempotent, Traceable. How will you structure your data to store these files alongside your Delta tables, under one governance layer? How does it fit into your overall architecture?")

answer("We use Unity Catalog Volumes for raw file storage. The setup notebook (00_setup.py) creates a Volume at prime-ins-jellsinki-poc.bronze.source_files, then copies all 14 source files from the cloned Git repo into it, preserving the regional folder structure (Insurance 1/, Insurance 2/, etc.).")

answer("This puts raw files inside the same governance boundary as our Delta tables. Team members can browse Volumes in the Catalog Explorer, see what files exist, and trace lineage from source file through to Gold. Access control is inherited from Unity Catalog - only authorized users can read or write.")

answer("For idempotency, the Bronze ingestion notebook checks whether all five Bronze tables already have data before re-ingesting. If they do, it exits early. Each ingested record gets two metadata columns: _ingested_at (timestamp) and _source_file (original file path), so any Bronze row can be traced back to the exact CSV or JSON it came from.")

answer("The fit in our architecture: Volume holds raw files (landing zone) -> Bronze Delta tables (exact copy with lineage metadata) -> Silver (cleaned, validated) -> Gold (star schema). Unity Catalog governs all layers.")

end_answer()

# Q6 (14.png)
question("6", "Every morning, PrimeInsurance's operations team opens the claims dashboard. It must show the current claim rejection rate by region, average claim processing time (gap between Claim_Logged_on and Claim_Processed_on), and refresh when the pipeline runs. The metrics must refresh automatically when new pipeline data arrives, no engineer should need to press anything. Which components in your architecture work together to meet both requirements?")

answer("Three components work together: Gold pre-computed tables, Databricks SQL Warehouse, and the pipeline workflow.")

answer("The Gold layer has pre-aggregated tables that compute these metrics at write time. claims_sla_monitor calculates rejection rate, average processing days, and SLA breach percentage per region. regulatory_readiness rolls everything into a single 0-100 score. These tables get overwritten on every pipeline run using CREATE OR REPLACE TABLE, so the data is always current.")

answer("SQL Warehouse serves these tables to dashboards with low latency. Our Lakeview dashboard (created by 06_create_dashboard.py via the REST API) has 6 pages covering all three business problems - claims SLA, customer registry, inventory aging, anomaly detection, regulatory readiness, and AI insights. The dashboard queries point at Gold tables.")

answer("The Databricks Workflow job ('Prime Insurance - POC') runs the full notebook chain: setup -> bronze -> silver -> gold -> AI use cases -> dashboard refresh. When the pipeline runs, Gold tables update, and any dashboard connected to SQL Warehouse automatically reflects the new data on next query. No manual refresh needed.")

end_answer()

# Q7 (15.png)
question("7", "PrimeInsurance's operations and leadership teams need to answer questions like: Which policy type (by policy_csl) has the highest claim rejection rate? What is the average claim processing time by incident severity? Which car models are listed for sale but have no sold_on date - and for how long? Each cuts across customers, claims, policies, sales, and cars simultaneously. How will you structure your data to make all of these questions answerable from a single, consistent query? How do they connect?")

answer("We built a star schema in the Gold layer with four dimension tables and two fact tables, all under prime-ins-jellsinki-poc.gold.")

answer("Dimensions: dim_customer (one row per deduplicated customer - region, job, marital status, education), dim_policy (one row per policy - carries policy_csl, deductible, premium, and foreign keys to both customer and car), dim_car (one row per vehicle - make, model, year, type), dim_date (generated calendar dimension with date surrogate keys in yyyyMMdd format).")

answer("Facts: fact_claims (grain: one claim, surrogate keys to customer/policy/car/three date dims, measures: total_claim_amount, processing_days, sla_breach, claim_rejected, injury/property/vehicle breakdowns) and fact_sales (grain: one car listing, links to dim_car and date dims for ad_placed_on and sold_on, measures: selling_price, days_on_lot, is_sold).")

answer("How each question gets answered:")
bullet("Rejection rate by policy_csl: join fact_claims to dim_policy on policy_sk, GROUP BY policy_csl, average claim_rejected. One query, no subselects.")
bullet("Avg processing time by severity: fact_claims carries incident_severity as a degenerate dimension and processing_days as a measure. Straight GROUP BY, no joins needed.")
bullet("Unsold cars and how long: fact_sales WHERE sold_date_sk IS NULL, join to dim_car for make/model, compute datediff(current_date, ad_placed_on) for days on lot.")

answer("The connection point is dim_policy - it sits between customers and cars, so fact_claims reaches any dimension through a single hop. fact_sales links to the same dim_car, so car-level analysis across both facts works through their shared car dimension. We kept degenerate dimensions (severity, state, collision type) on the fact table to avoid unnecessary joins for common filters.")

end_answer()

# Q8 (16.png)
question("8", "Which Gold-layer table(s) does each AI use case consume, and why does the intelligence layer sit above Gold rather than Silver or Bronze? What Databricks features enable these outputs to live in your architecture, and how are they made available to business users?")

answer("Each use case reads from specific tables and writes AI output back to Gold:")
bullet("UC1 (DQ Explanations): reads silver.dq_issues -> writes gold.dq_explanation_report")
bullet("UC2 (Anomaly Detection): reads silver.claims -> writes gold.claim_anomaly_explanations")
bullet("UC3 (Policy RAG): reads gold.dim_policy -> writes gold.rag_query_history")
bullet("UC4 (Executive Insights): reads all Gold fact/dim tables -> writes gold.ai_business_insights")

answer("UC1 and UC2 actually sit between Silver and Gold - they consume Silver data and produce Gold outputs. UC3 and UC4 consume Gold data. The intelligence layer sits above the cleaned/modeled data because LLMs need consistent, validated inputs. Sending raw Bronze data (with 7 different column names for customer_id, Excel serial dates, literal 'NULL' strings) to an LLM would produce garbage. Gold gives us standardized definitions, resolved entities, and computed measures that the LLM can reason about.")

answer("Databricks features that make this work: Foundation Model serving endpoints (databricks-gpt-oss-20b accessible via OpenAI SDK), Unity Catalog (AI outputs stored as governed Delta tables alongside business data, same access controls), MLflow (experiment tracking for prompt versions, token usage, success rates), and SQL Warehouse (business users query AI outputs with standard SQL - 'SELECT * FROM gold.claim_anomaly_explanations WHERE priority = HIGH'). Genie Space can also be pointed at these tables for natural language queries.")

end_answer()

story.append(PageBreak())

# ========================================================
# MODULE 2: BRONZE (Q5-Q7 from screenshots 21-24)
# ========================================================
module("Module 2: Bronze Layer")

# Q5 (21.png - building bronze layer goals/outcomes already covered by upload)

# Q6 (22.png)
question("6", "Imagine the latest customer file from Insurance 3 arrives with a new column, Birth Date. This column was never present in any previous customer file. Your Bronze pipeline has already run successfully multiple times before this file arrived. The column name contains a space. Will the pipeline succeed or fail when this file is loaded? What specifically causes that outcome? How do you handle it, keeping in mind that the Bronze layer must preserve data exactly as received?")

answer("The pipeline will succeed without any changes. Two things make this work.")

answer("First, we use unionByName(allowMissingColumns=True) when merging all 7 customer CSV files. This means when Insurance 3's file now has a 'Birth Date' column that other files lack, Spark adds the column to the unified DataFrame and fills it with NULL for rows from other files. No schema conflict, no error.")

answer("Second, we write to Delta with overwriteSchema=true. So the Bronze customers table schema evolves automatically - the new 'Birth Date' column gets added to the table on the next write. All existing records from previous files get NULL in that column.")

answer("The space in the column name is preserved as-is in Bronze. We do not rename or clean anything at this layer. Bronze's job is to be a faithful copy of what arrived. Column name standardization happens in Silver, where we'd add 'Birth Date' to our coalesce mapping if it needs to be unified with other columns.")

answer("If we ever needed to query this column in Delta, backtick quoting handles the space: SELECT `Birth Date` FROM bronze.customers.")

end_answer()

# Q7 (23.png)
question("7", "PrimeInsurance's 7 customer files do not share a consistent schema. customers_3.csv uses Reg and Marital_Status, while others use Region and Marital. What did you observe in the schema, specifically for the customers? What did you discover when you loaded the file and ran your quality rules? Did any file cause an unexpected result in the Bronze table, and how did you handle it?")

answer("We found schema chaos across all 7 files. Here is what we documented:")
bullet("ID column: CustomerID (files 1,4,5,6,7), Customer_ID (file 2), cust_id (file 3)")
bullet("Region: Region (most files), Reg (files 1,3)")
bullet("City: City (most files), City_in_state (file 2)")
bullet("Education: Education (most), Edu (file 2), missing entirely (file 4)")
bullet("Marital: Marital (most), Marital_status (files 1,6), missing (file 2)")
bullet("Job: present in most, missing from file 1")
bullet("HHInsurance: missing from file 2")

answer("In Bronze, unionByName(allowMissingColumns=True) handles this by creating a superset schema with all column variants present. So the Bronze customers table has columns for CustomerID, Customer_ID, cust_id, Region, Reg, City, City_in_state, etc. Most rows have NULLs in the variant columns that don't apply to their source file.")

answer("When we ran quality rules in Silver, we used coalesce() to unify variants: customer_id = COALESCE(CustomerID, Customer_ID, cust_id). Same pattern for region, city, education, marital. After unification, the region_invalid rule caught records from file 1 where Reg contained abbreviations like 'E' or 'W' instead of full names. Our region standardization map handles these: 'e' -> 'East', 'w' -> 'West', etc.")

answer("The unexpected result: after deduplication using MD5 hash of (state|city|job|marital|balance), we found roughly 20% of customer records were duplicates across regions - the same person registered with different IDs in different regional systems. This confirmed the inflated customer count problem PrimeInsurance was facing.")

end_answer()

story.append(PageBreak())

# ========================================================
# MODULE 3: SILVER (Q4-Q5 from screenshots 29-30)
# ========================================================
module("Module 3: Silver Layer")

# Q4 (29.png)
question("4", "During last night's pipeline run, 83 customer records and 47 claims records failed your quality rules. The next morning, the compliance team needs to review every rejected record; they need to know what was rejected, why it was rejected, and they cannot wait for an engineer to run a query for them. Where did those failed records go? How did you make sure they did not disappear silently? Walk us through your approach and show the relevant code for any one table.")

answer("Failed records go to dedicated quarantine tables per entity: silver.quarantine_customers, silver.quarantine_claims, silver.quarantine_policy, etc. Each quarantined record keeps all original columns plus two additions: _quarantine_reason (comma-separated list of which rules failed) and _quarantined_at (timestamp of when it was flagged).")

answer("Nothing disappears silently because the code explicitly splits the DataFrame into two paths: records where _quality_passed = true go to the clean Silver table, records where _quality_passed = false go to the quarantine table. Both writes happen in the same notebook run.")

answer("The compliance team queries quarantine tables directly in SQL Warehouse without engineering help:")
code("SELECT * FROM silver.quarantine_customers\nWHERE _quarantine_reason LIKE '%region_invalid%'\nORDER BY _quarantined_at DESC")

answer("Here is the approach for customers (02_silver_customers.py):")
code("# define rules as PySpark conditions\nquality_rules = [\n    ('customer_id_null', col('customer_id').isNull()),\n    ('region_invalid', ~col('region').isin('East','West','North','South','Central')),\n    ('state_null', col('state').isNull()),\n]\n\n# flag each rule\nfor rule_name, condition in quality_rules:\n    df = df.withColumn(f'_fail_{rule_name}', when(condition, True).otherwise(False))\n\n# overall pass/fail\ndf = df.withColumn('_quality_passed', ~(col('_fail_customer_id_null') | ...))\n\n# quarantine bad records\nquarantine_df = df.filter('_quality_passed = false')\n    .withColumn('_quarantine_reason', concat_ws(', ', ...))\n    .withColumn('_quarantined_at', current_timestamp())\nquarantine_df.write.saveAsTable('silver.quarantine_customers')\n\n# clean records proceed\ndf.filter('_quality_passed = true').write.saveAsTable('silver.customers')")

answer("Additionally, per-entity DQ issue summaries (rule_name, records_affected, affected_ratio, severity, suggested_fix) are written to silver.dq_issues_{entity} tables, then merged into a unified silver.dq_issues view. This feeds UC1 (AI-generated explanations) so the compliance team gets both raw rejected records and plain-English explanations of what went wrong.")

end_answer()

# Q5 (30.png)
question("5", "Did you use a SQL Warehouse at any point while building the Silver layer? If yes - where, and why did you choose it over running the same query in a notebook?")

answer("We used serverless compute (notebook tasks in Databricks Workflows) for all Silver layer pipeline execution. The transformation logic - schema unification, coalesce mappings, quality rules, quarantine routing, deduplication - runs as PySpark in notebooks because it needs programmatic control: loops over quality rules, dynamic column creation, conditional logic for different file formats.")

answer("SQL Warehouse enters the picture on the serving side. Once Silver and Gold tables are written, SQL Warehouse handles dashboard queries and Genie Space, providing low-latency, concurrent access. The separation is intentional: pipeline compute (notebooks on serverless) is optimized for throughput, while SQL Warehouse is optimized for many concurrent short queries from business users.")

answer("We did use SQL Warehouse during development for ad-hoc exploration - checking quarantine table contents, verifying DQ issue counts, spot-checking deduplication results. But the pipeline itself runs entirely on notebook compute.")

end_answer()

story.append(PageBreak())

# ========================================================
# MODULE 4: GOLD (Q2, Q4, Q7 from screenshots 33, 38, 15 already covered)
# ========================================================
module("Module 4: Gold Layer - Dimensional Model")

# Q2 (33.png)
question("2", 'The compliance team needs a single query to answer: "Which policy type had the highest claim rejection rate last quarter, broken down by region?" Walk through exactly which tables in your model this query would touch. What joins would connect them, and what key does each join use? Is there any business question from PrimeInsurance\'s three failures that your current model cannot answer? If yes, what is missing?')

answer("Tables touched: fact_claims, dim_policy, dim_date.")

answer("Joins: fact_claims.policy_sk = dim_policy.policy_sk (to get policy_csl), fact_claims.incident_date_sk = dim_date.date_sk (to filter by quarter).")

code("SELECT dp.policy_csl, fc.incident_state as region,\n       COUNT(*) as total_claims,\n       SUM(CASE WHEN fc.claim_rejected = 'Y' THEN 1 ELSE 0 END) as rejected,\n       ROUND(rejected * 100.0 / total_claims, 2) as rejection_rate\nFROM gold.fact_claims fc\nJOIN gold.dim_policy dp ON fc.policy_sk = dp.policy_sk\nJOIN gold.dim_date dd ON fc.incident_date_sk = dd.date_sk\nWHERE dd.quarter = 4 AND dd.year = 2024\nGROUP BY dp.policy_csl, fc.incident_state\nORDER BY rejection_rate DESC")

answer("The model covers all three business failures (claims backlog, regulatory compliance, revenue leakage). What it cannot answer: questions about individual claim workflow events - who approved a claim, when did the adjuster first contact the claimant, what communications happened. That would require dim_adjuster and fact_customer_interaction tables, which are not in our source data. We also lack policy renewal history, so questions like 'what is the retention rate by policy type' are not answerable.")

end_answer()

# Q4 (38.png)
question("4", "Based on PrimeInsurance's three business failures, what are the queries the business team would run most frequently on top of your fact tables? List the queries, be specific about what each one is measuring and which business failure it addresses. For each query, what makes it a good candidate to be pre-computed rather than run fresh every time?")

answer("We pre-computed four Gold aggregation tables, each tied to a specific business failure:")

answer("1. claims_sla_monitor (Claims Backlog): Computes avg processing days, rejection rate, and SLA breach percentage per region. The ops team checks this every morning. Pre-computed because it scans all of fact_claims and groups across multiple dimensions - running this fresh on every dashboard load would be wasteful with 20+ concurrent users.")

answer("2. regulatory_customer_registry (Regulatory Compliance): Shows deduplicated customer count by region with COLLECT_SET of original_customer_ids and source files for audit trail. Needed for quarterly regulatory reporting. Pre-computed because the deduplication logic (MD5 hashing across 5 fields) is expensive, and the result rarely changes between pipeline runs.")

answer("3. inventory_aging_alerts (Revenue Leakage): Flags unsold cars sitting more than 60 days on lot and LEFT JOINs to find regions where the same model sells in under 30 days on average, suggesting redistribution. Pre-computed because it joins fact_sales to dim_car and does cross-regional comparison - a multi-pass operation that should run once at pipeline time, not on every query.")

answer("4. regulatory_readiness (All Three): Single 0-100 score combining customer registry completeness (40%), data quality score (30%), and claims processing efficiency (30%). Leadership checks this on-demand. Pre-computed because it aggregates metrics from three different Gold tables into one KPI.")

end_answer()

story.append(PageBreak())

# ========================================================
# MODULE 5: GEN AI (Q2 for UC1, Q2 for UC2, Q4 for UC3, Q2 final)
# ========================================================
module("Module 5: Gen AI Intelligence Layer")

# UC1 Q2 (45.png)
question("UC1-Q2", "The DQ issues table contains technical fields like rule_name, affected_ratio, and suggested_fix. Your prompt must translate these into a narrative a compliance officer can act on. Describe your prompt design strategy: What fields from dq_issues did you include in the prompt, and why? How did you structure the prompt to ensure the LLM returns a formatted explanation (not a generic paragraph)? What happened when you ran it the first time? What did you need to change?")

answer("We include all fields from dq_issues in the prompt: entity, rule_name, records_affected, total_records, affected_ratio, severity, and suggested_fix. Each field matters - rule_name tells the model what broke, affected_ratio gives scale, severity sets urgency, suggested_fix provides the pipeline's own recommendation for the model to elaborate on.")

answer("Prompt structure that worked (V3, final): We give the model a persona - 'senior data quality analyst at PrimeInsurance with 15 years of P&C insurance experience.' This sets the right tone for compliance audiences. The system prompt includes severity definitions (CRITICAL/HIGH/MEDIUM/LOW with business meaning), then demands exactly 5 JSON keys: what_was_found, why_it_matters, what_caused_it, what_was_done, how_to_prevent. Each key must be 2-3 sentences. We also add explicit constraints: no N/A values, no invented data, reference actual numbers from the issue.")

answer("What happened first time (V1): returned generic paragraphs with no structure, no JSON, no business context. V2 added JSON format requirement but the model inconsistently wrapped output in markdown fences or added extra keys. V3 added the persona, severity definitions, and strict constraints. Pydantic validation (DQExplanation model with min_length=20 per field) catches any remaining format issues - if a field comes back as 'N/A' or empty, the validator replaces it with '[Not provided by model - requires manual review]' instead of crashing.")

answer("Additional handling: databricks-gpt-oss-20b returns responses as a list of dicts (not a plain string), so we have an extract_text() helper that pulls the text block before JSON parsing. Tenacity retry with 5 attempts and random exponential backoff (2-60s) handles transient failures. PII redaction (regex for emails, phones, SSNs) runs on all LLM outputs before writing to Gold.")

end_answer()

# UC2 Q2 (48.png)
question("UC2-Q2", "Describe your anomaly detection approach: What 5 rules did you implement, and what weight did you assign each? Justify the weighting - why does one rule carry more signal than another? At what anomaly score threshold did you decide a claim is worth flagging for investigation? How did you arrive at that number? How many claims in your dataset were flagged? Is that number realistic for a fraud investigation team to handle?")

answer("Five weighted rules producing a 0-100 anomaly score:")

answer("Rule 1: High claim amount outlier (25 pts) - Uses MAD-scaled z-score (Median Absolute Deviation * 1.4826) instead of standard deviation. MAD is robust against the skewed distribution of claim amounts. Triggers when z-score > 2.0. Weight justified: inflated amounts are the most direct indicator of fraud per NICB (National Insurance Crime Bureau) Tier-1 indicators.")

answer("Rule 2: Severity-amount mismatch (25 pts) - IQR analysis per severity band. Computes Q1, Q3, and upper fence (Q3 + 1.5*IQR) for each severity level. A $50K claim on 'Minor Damage' triggers this rule. Same weight as Rule 1 because structural inconsistency between severity and amount is equally strong signal.")

answer("Rule 3: No police report + high amount (20 pts) - Flags claims above median amount with no police report. Legitimate high-value accidents almost always have police documentation. Weight is slightly lower because missing reports can have innocent explanations (minor fender benders, remote areas).")

answer("Rule 4: No witnesses + bodily injuries (15 pts) - Injury claims with zero witnesses are hard to verify independently. Common in staged accidents. Lower weight because zero witnesses can occur legitimately in single-car incidents.")

answer("Rule 5: Single vehicle total loss (15 pts) - Single Vehicle Collision + Total Loss severity. Known as 'owner give-ups' - intentionally destroying a car to collect insurance. Lower weight because it is circumstantial without other indicators.")

answer("Thresholds: HIGH >= 60 (auto-refer to Special Investigations Unit), MEDIUM >= 35 (enhanced adjuster review). We arrived at 60 by reasoning that a claim needs to trigger at least the two heaviest rules (25+25=50) plus one more to warrant SIU referral. The 35 threshold catches claims triggering any two rules.")

answer("Only HIGH and MEDIUM claims get sent to the LLM for investigation brief generation. The LLM writes structured briefs (what_is_suspicious, risk_factors, recommended_action) but does not make the detection decision - it is an explainer, not a classifier. Flagged count depends on the dataset distribution, but typically 10-15% of claims land in MEDIUM+ territory, which is realistic for a fraud team doing enhanced review (they triage, not investigate every one).")

end_answer()

# UC3 Q4 (53.png)
question("UC3-Q4", "Explain the end-to-end retrieval flow in your RAG system: How did you decide on your chunk size and overlap? What happens if chunks are too large or too small? dim_policy is a structured table, not a document. What did you convert it to before chunking, and why does a structured table need this conversion at all for RAG to work? When you tested a question like 'which policies have umbrella coverage?', how many chunks were retrieved, and how did the system decide which ones were relevant?")

answer("End-to-end flow: gold.dim_policy rows -> natural language documents -> sentence-transformer embeddings -> FAISS index -> user question embedded -> top-K retrieval -> LLM generates answer with cited policy numbers -> output saved to gold.rag_query_history.")

answer("Chunk strategy: 1 policy = 1 chunk, no splitting, no overlap. Each policy record converts to roughly 80-120 words of natural language - well within the 256-token input limit of all-MiniLM-L6-v2. We chose this because each policy is a self-contained unit. Splitting a policy across chunks would break context (the deductible in one chunk, the CSL in another). For actual PDF documents, we would use 500-token chunks with 50-token overlap, but that is not needed here.")

answer("Why convert structured data to text? Embedding models understand natural language, not column-value pairs. 'deductible: 500' produces a poor embedding. 'The deductible is $500' produces a good one. Our conversion function (policy_to_document) writes rich English: decodes CSL ('100/300' becomes '$100K per person and $300K per accident'), classifies premium tiers (Basic/Standard/Premium), and mentions umbrella coverage explicitly when present. This means 'which policies have umbrella coverage' matches documents containing 'includes umbrella coverage with a limit of $2,000,000' through semantic similarity, not keyword overlap.")

answer("Retrieval: we embed the user question with the same all-MiniLM-L6-v2 model (384-dimensional vectors), search the FAISS IndexFlatIP index (exact inner product on L2-normalized vectors = cosine similarity), and retrieve TOP_K=5 most similar policy documents. For 'which policies have umbrella coverage?', the system retrieves the 5 policies whose documents have the highest semantic similarity to the question - policies that mention umbrella limits rank highest because that phrase is semantically close to the query.")

answer("Confidence scoring is a composite: 50% top similarity score + 20% score gap between 1st and 2nd result + 30% average of top-3 scores. HIGH confidence (>= 0.65) means the top result is clearly relevant and well-separated from noise.")

end_answer()

# Final Gen AI Q2 (58.png)
question("Final-Q2", "Looking across all the Gen AI use cases you built, answer the following: (a) All four use cases use the same databricks-gpt-oss-20b model. But the prompt design is different every time - what changed and why? (b) extract_text() appears in the same piece of code across all use cases. Why is it there at all? What would happen in each use case if you removed it? (c) If you removed the last CREATE OR REPLACE TABLE without pausing, what would happen? (d) If PrimeInsurance's production model went down for three days, which use case would require the most redesign - and why?")

answer("(a) Same model, different prompts because each task has a different audience and output shape. UC1 uses a senior DQ analyst persona producing 5-field diagnostic reports for the compliance team. UC2 uses a senior fraud investigator persona producing 3-field investigation briefs for SIU. UC3 uses a policy intelligence assistant that must answer ONLY from provided context and cite policy numbers - it is grounded retrieval, not generation. UC4 uses a senior data analyst producing executive narrative summaries with headline, key findings, alerts, and recommendations. The persona, output schema, and constraints change because the downstream consumer is different each time.")

answer("(b) extract_text() exists because databricks-gpt-oss-20b returns response.choices[0].message.content as a list of dicts ([{'type': 'text', 'text': '...'}]) rather than a plain string. Without extract_text(), every use case would crash with 'list object has no attribute strip' or 'Expecting value: line 1 column 1' when trying to JSON-parse the response. It is a model-specific adapter. If we switched to a model that returns plain strings, we could remove it.")

answer("(c) The CREATE OR REPLACE TABLE at the end of each UC writes AI-generated results to Gold. Without it, the LLM would run, generate outputs, consume tokens and compute, and then the results would exist only in the notebook's local DataFrame - lost when the session ends. The dashboard and Genie Space queries would return stale data from the previous run. For UC4 specifically, the executive summaries would never reach the ai_business_insights table, so the 'AI Insights' dashboard page would show yesterday's analysis.")

answer("(d) UC3 (RAG) would require the most redesign. UC1, UC2, and UC4 are batch jobs - they run once per pipeline execution and write results to tables. If the model is down for three days, you just rerun them when it comes back. UC3 is different because it serves real-time adjuster queries. Adjusters looking up policy details during claims processing cannot wait three days. You would need a fallback: either a cached response layer, a secondary model endpoint, or a degraded mode that returns raw policy data without LLM synthesis. The other UCs just queue up and reprocess.")

end_answer()

story.append(PageBreak())

# ========================================================
# MODULE 6: FINAL SUBMISSION
# ========================================================
module("Module 6: Final Submission")

question("1", "Upload your complete Gen AI track submission (covered by file uploads - notebooks, screenshots, team summary).")
answer("This question requires file uploads: all implemented notebooks (UC1-UC4), Unity Catalog Gold schema screenshot showing dq_explanation_report, claim_anomaly_explanations, rag_query_history, ai_business_insights, and 1-page team summary PDF.")
end_answer()

question("2", "Share your GitHub repository link and upload a screenshot of your repository showing the folder structure and all files committed.")
answer("Repository: https://github.com/neerajkumar-jft/primeinsurance-poc")
answer("Structure: notebooks/ (00_config through 06_create_dashboard), docs/ (submission_answers, architecture), deploy/ (job_config.json), data/ (regional source files), README.md with full setup and dashboard guide.")
end_answer()


# Build PDF
doc.build(story)
print(f"PDF generated: {OUTPUT_PATH}")
