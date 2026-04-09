"""
Builds and creates the PrimeInsurance Genie Space (prod, 5-table version).

Extends genie_space_config.py to cover all three stated business failures:
  1. Regulatory customer count   -> dim_customer (deduplicated)
  2. 18-day claims backlog       -> fact_claims + dim_policy
  3. Unsold inventory leakage    -> fact_sales + dim_car

5 tables chosen deliberately:
  - primeins.gold.dim_customer
  - primeins.gold.dim_policy
  - primeins.gold.fact_claims
  - primeins.gold.fact_sales
  - primeins.gold.dim_car

Excluded (with reasons):
  - dim_region: 5-row reference, adds noise, semantics already in
    dim_customer.region / fact_sales.region
  - mv_* materialized views: duplicate aggregate questions that Genie can
    compute from the base facts; including them confuses table selection
  - AI output tables (claim_anomaly_explanations, dq_explanation_report,
    ai_business_insights, rag_query_history*): these are derived insight
    artifacts, not canonical data; exposing them to Genie would produce
    meta-answers about model output instead of business answers
  - Silver quality tables (dq_issues, quarantine_*): compliance domain,
    belongs in a separate Regulator Genie space (see Phase 2 roadmap)

Usage:
    python genie_space_config_prod.py                        # dry-run
    python genie_space_config_prod.py --create               # create on prod
    python genie_space_config_prod.py --create --profile professional
"""

import argparse
import json
import subprocess
import uuid


def u() -> str:
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Text instructions — single consolidated block.
# Everything that cannot be taught via example SQL lives here.
# ---------------------------------------------------------------------------
TEXT_INSTRUCTIONS = [
    """# PrimeInsurance Genie Space — Rules & Domain Context

## Business context
PrimeInsurance is a multi-region auto insurance company built from six
acquisitions. This space answers questions about three core business concerns:

  1. **Customer registry** — how many true (deduplicated) customers we have
  2. **Claims performance** — rejection rates, severity patterns, backlog
  3. **Inventory health** — aging used-car listings and regional demand gaps

If a question doesn't map to one of these, say so politely and offer the
nearest supported topic.

## 1. CRITICAL JOIN RULE
`fact_claims.policy_number` is STRING but `dim_policy.policy_number` is INT.
When joining these two tables you MUST cast one side. Always use:

    fact_claims.policy_number = CAST(dim_policy.policy_number AS STRING)

Never join them without the cast.

## 2. Customer join path
`dim_customer` has no direct link to `fact_claims`. To connect claims to
customers, always go through `dim_policy`:

    fact_claims
      JOIN dim_policy ON fact_claims.policy_number = CAST(dim_policy.policy_number AS STRING)
      JOIN dim_customer ON dim_policy.customer_id = dim_customer.customer_id

## 3. Corrupted claim date fields
The columns `incident_date`, `claim_logged_on`, and `claim_processed_on` in
`fact_claims` are CORRUPTED AT SOURCE — only time portions survived, not full
dates. Do NOT use them for date, tenure, SLA, or processing-time analysis.
If a user asks "how long do claims take to process", respond:
"Claim processing dates are corrupted at the source — only time portions
survived. The platform cannot compute real processing duration from fact_claims.
UC4 generates synthetic processing days at the consumption layer only, which
should never be used for compliance reporting."

## 4. Inventory aging — the sales_head question
`fact_sales` is the live-listings table for used cars. Key columns:
  - `ad_placed_on` (timestamp the listing went live)
  - `sold_on` (timestamp it sold; NULL = still unsold)
  - `days_listed` (integer days since ad_placed_on)
  - `is_sold` (boolean)
  - `region` (East / West / Central / South / North)

"Unsold" means `sold_on IS NULL`. "Aging" buckets on `days_listed`:
  - Fresh:    < 30 days
  - Watch:    30–60 days
  - Stale:    60–90 days
  - Critical: > 90 days

When a user asks about "aging inventory" or "stuck cars", always filter
`sold_on IS NULL` and sort by `days_listed DESC`.

## 5. Customer deduplication story
`dim_customer` contains 1,604 canonical customers that were deduplicated from
3,605 raw rows across 7 source files. Always use `dim_customer` for customer
counts, never anything else. The dedup methodology is in architecture.md.
The 12% inflation story is the regulatory headline.

## 6. Null-as-string handling in dim_customer
The columns `region`, `job`, `marital`, and `education` may contain the string
'None' or 'NA' instead of SQL NULL. When filtering, exclude them with
`WHERE column NOT IN ('None', 'NA')` rather than `IS NOT NULL`.

## 7. Premium tier definitions
When users ask about policy tiers, use these thresholds on `policy_annual_premium`:

  - Basic:    < $1,000
  - Standard: $1,000 – $1,499
  - Premium:  >= $1,500

These are the authoritative tier definitions for this dataset.

## 8. Severity values
`fact_claims.incident_severity` has exactly four values:
`Minor Damage`, `Major Damage`, `Total Loss`, `Trivial Damage`. Do not invent
other severity labels.

## 9. Formatting rules
- Currency: `$` prefix + thousands separators (e.g., $12,652.40)
- Percentages: rounded to 2 decimal places
- States: 2-letter abbreviation as stored. Only these states appear in the
  dataset: OH, IN, IL, SC, NY, WV, NC, VA, PA.
- Regions: East, West, Central, South, North (no abbreviations)

## 10. Clarification rule
If a user asks about revenue, subscriptions, or terms that are not in the
dataset, ask: "This dataset covers auto insurance policies, customers, claims,
and used-car sales listings. Did you mean annual premium from policies, total
claim payouts, or used-car selling prices?"
"""
]


# ---------------------------------------------------------------------------
# Example SQL — highest-leverage signal. Each teaches one reusable pattern.
# ---------------------------------------------------------------------------
EXAMPLE_SQLS = [
    # ── Claims questions ──
    {
        "question": "What is the claim rejection rate by incident severity?",
        "sql": """SELECT
  incident_severity,
  COUNT(*) AS total_claims,
  SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) AS rejected_claims,
  ROUND(100.0 * SUM(CASE WHEN is_rejected THEN 1 ELSE 0 END) / COUNT(*), 2) AS rejection_rate_pct
FROM primeins.gold.fact_claims
GROUP BY incident_severity
ORDER BY rejection_rate_pct DESC""",
    },
    {
        "question": "Which states have the highest total claim amounts?",
        "sql": """SELECT
  incident_state,
  COUNT(*) AS num_claims,
  ROUND(SUM(total_claim_amount), 2) AS total_claim_amount,
  ROUND(AVG(total_claim_amount), 2) AS avg_claim_amount
FROM primeins.gold.fact_claims
GROUP BY incident_state
ORDER BY total_claim_amount DESC""",
    },
    {
        "question": "Which region has the highest claim volume?",
        "sql": """SELECT
  cust.region,
  COUNT(c.claim_id) AS claim_count,
  ROUND(SUM(c.total_claim_amount), 2) AS total_amount
FROM primeins.gold.fact_claims c
JOIN primeins.gold.dim_policy p
  ON c.policy_number = CAST(p.policy_number AS STRING)
JOIN primeins.gold.dim_customer cust
  ON p.customer_id = cust.customer_id
WHERE cust.region NOT IN ('None', 'NA')
GROUP BY cust.region
ORDER BY claim_count DESC""",
    },
    {
        "question": "Show me claims over $10,000",
        "sql": """SELECT
  claim_id,
  incident_type,
  incident_severity,
  incident_state,
  ROUND(total_claim_amount, 2) AS claim_amount,
  is_rejected
FROM primeins.gold.fact_claims
WHERE total_claim_amount > 10000
ORDER BY total_claim_amount DESC""",
    },
    # ── Policy questions ──
    {
        "question": "How many policies are in each premium tier?",
        "sql": """SELECT
  CASE
    WHEN policy_annual_premium < 1000 THEN 'Basic'
    WHEN policy_annual_premium < 1500 THEN 'Standard'
    ELSE 'Premium'
  END AS tier,
  COUNT(*) AS num_policies,
  ROUND(AVG(policy_annual_premium), 2) AS avg_premium
FROM primeins.gold.dim_policy
GROUP BY 1
ORDER BY
  CASE
    WHEN CASE
      WHEN policy_annual_premium < 1000 THEN 'Basic'
      WHEN policy_annual_premium < 1500 THEN 'Standard'
      ELSE 'Premium'
    END = 'Basic' THEN 1
    WHEN CASE
      WHEN policy_annual_premium < 1000 THEN 'Basic'
      WHEN policy_annual_premium < 1500 THEN 'Standard'
      ELSE 'Premium'
    END = 'Standard' THEN 2
    ELSE 3
  END""",
    },
    {
        "question": "Which policies include umbrella coverage?",
        "sql": """SELECT
  policy_number,
  policy_state,
  policy_csl,
  policy_deductible,
  ROUND(policy_annual_premium, 2) AS policy_annual_premium,
  umbrella_limit
FROM primeins.gold.dim_policy
WHERE umbrella_limit > 0
ORDER BY umbrella_limit DESC""",
    },
    # ── Customer questions ──
    {
        "question": "How many customers do we have (deduplicated)?",
        "sql": """SELECT COUNT(DISTINCT customer_id) AS total_customers
FROM primeins.gold.dim_customer""",
    },
    {
        "question": "Break down customers by region and show their average policy premium",
        "sql": """SELECT
  cust.region,
  COUNT(DISTINCT cust.customer_id) AS num_customers,
  COUNT(p.policy_number) AS num_policies,
  ROUND(AVG(p.policy_annual_premium), 2) AS avg_premium
FROM primeins.gold.dim_customer cust
LEFT JOIN primeins.gold.dim_policy p
  ON cust.customer_id = p.customer_id
WHERE cust.region NOT IN ('None', 'NA')
GROUP BY cust.region
ORDER BY num_customers DESC""",
    },
    {
        "question": "Show me customers with more than 5 claims",
        "sql": """SELECT
  cust.customer_id,
  cust.region,
  cust.job,
  COUNT(c.claim_id) AS claim_count,
  ROUND(SUM(c.total_claim_amount), 2) AS total_claimed
FROM primeins.gold.dim_customer cust
JOIN primeins.gold.dim_policy p
  ON cust.customer_id = p.customer_id
JOIN primeins.gold.fact_claims c
  ON c.policy_number = CAST(p.policy_number AS STRING)
GROUP BY cust.customer_id, cust.region, cust.job
HAVING COUNT(c.claim_id) > 5
ORDER BY claim_count DESC""",
    },
    # ── Inventory questions (fact_sales + dim_car) ──
    {
        "question": "Which cars are aging in inventory?",
        "sql": """SELECT
  car.model,
  s.region,
  COUNT(*) AS unsold_count,
  ROUND(AVG(s.days_listed), 0) AS avg_days_listed,
  MAX(s.days_listed) AS max_days_listed
FROM primeins.gold.fact_sales s
JOIN primeins.gold.dim_car car ON s.car_id = car.car_id
WHERE s.sold_on IS NULL AND s.days_listed > 60
GROUP BY car.model, s.region
ORDER BY max_days_listed DESC""",
    },
    {
        "question": "Which region has the worst aging inventory problem?",
        "sql": """SELECT
  region,
  COUNT(*) AS unsold_listings,
  ROUND(AVG(days_listed), 0) AS avg_days_listed,
  SUM(CASE WHEN days_listed > 90 THEN 1 ELSE 0 END) AS critical_count
FROM primeins.gold.fact_sales
WHERE sold_on IS NULL
GROUP BY region
ORDER BY critical_count DESC""",
    },
    {
        "question": "Which car models sell fastest?",
        "sql": """SELECT
  car.model,
  COUNT(*) AS sales_count,
  ROUND(AVG(s.days_listed), 1) AS avg_days_to_sell,
  ROUND(AVG(s.original_selling_price), 2) AS avg_price
FROM primeins.gold.fact_sales s
JOIN primeins.gold.dim_car car ON s.car_id = car.car_id
WHERE s.sold_on IS NOT NULL
GROUP BY car.model
HAVING COUNT(*) >= 5
ORDER BY avg_days_to_sell ASC""",
    },
]


# ---------------------------------------------------------------------------
# Sample questions — clickable prompts in the Genie UI
# ---------------------------------------------------------------------------
SAMPLE_QUESTIONS = [
    "Which region has the highest claim volume?",
    "What is the claim rejection rate by incident severity?",
    "How many customers do we have (deduplicated)?",
    "Show me customers with more than 5 claims",
    "Which cars are aging in inventory?",
    "Which region has the worst aging inventory problem?",
    "How many policies are in each premium tier?",
    "Show me claims over $10,000",
]


# ---------------------------------------------------------------------------
# Table descriptions (supplement existing COMMENT ON COLUMN metadata)
# ---------------------------------------------------------------------------
TABLES = [
    {
        "identifier": "primeins.gold.dim_customer",
        "description": [
            "Canonical customer dimension. 1,604 deduplicated customers harmonized "
            "from 7 regional source files (reduced from 3,605 raw rows — the 12% "
            "inflation story). Demographics (job, marital, education, region/state/city) "
            "and financial flags (balance, hh_insurance, car_loan, default_flag). "
            "Join path to claims: dim_customer -> dim_policy -> fact_claims. "
            "Some text columns contain 'None'/'NA' strings; filter with NOT IN."
        ],
    },
    {
        "identifier": "primeins.gold.dim_policy",
        "description": [
            "Policy dimension. 999 active auto insurance policies. Contains coverage "
            "details (CSL, deductible, umbrella_limit), annual premium, state, "
            "bind date, and foreign keys to dim_car and dim_customer. "
            "CRITICAL: join to fact_claims via CAST(policy_number AS STRING) "
            "because the source tables disagree on type."
        ],
    },
    {
        "identifier": "primeins.gold.fact_claims",
        "description": [
            "Claims fact table. 1,000 rows, one per claim. Includes amounts "
            "(injury, property, vehicle, total), incident type/severity/location, "
            "rejection status. WARNING: date fields (incident_date, "
            "claim_logged_on, claim_processed_on) are CORRUPTED AT SOURCE and "
            "must not be used for any date analysis. Severity is one of "
            "'Minor Damage', 'Major Damage', 'Total Loss', 'Trivial Damage'."
        ],
    },
    {
        "identifier": "primeins.gold.fact_sales",
        "description": [
            "Used-car inventory fact table. 1,849 listings. One row per ad. "
            "Key columns: car_id (FK to dim_car), region, ad_placed_on, "
            "sold_on (NULL = unsold), original_selling_price, days_listed, "
            "is_sold. The sales_head's primary table — use for aging inventory, "
            "regional demand gaps, and selling velocity questions. "
            "Filter sold_on IS NULL for unsold listings."
        ],
    },
    {
        "identifier": "primeins.gold.dim_car",
        "description": [
            "Car reference dimension. 2,500 rows with one entry per unique vehicle. "
            "Key columns: car_id, name, model, fuel, transmission, km_driven, "
            "mileage (numeric, kmpl), engine (cc), max_power (bhp), seats. "
            "Used to group fact_sales by model, enabling 'which models sell "
            "fastest' and 'which models are aging' questions."
        ],
    },
]


def build_serialized_space() -> dict:
    text_instructions = sorted(
        [{"id": u(), "content": [t]} for t in TEXT_INSTRUCTIONS],
        key=lambda x: x["id"],
    )
    example_sqls = sorted(
        [
            {"id": u(), "question": [ex["question"]], "sql": [ex["sql"]]}
            for ex in EXAMPLE_SQLS
        ],
        key=lambda x: x["id"],
    )
    sample_questions = sorted(
        [{"id": u(), "question": [q]} for q in SAMPLE_QUESTIONS],
        key=lambda x: x["id"],
    )
    return {
        "version": 2,
        "data_sources": {
            "tables": sorted(TABLES, key=lambda t: t["identifier"]),
        },
        "instructions": {
            "text_instructions": text_instructions,
            "example_question_sqls": example_sqls,
        },
        "config": {
            "sample_questions": sample_questions,
        },
    }


def build_payload() -> dict:
    return {
        "title": "PrimeInsurance Data Intelligence",
        "description": (
            "Natural-language interface over the PrimeInsurance Gold layer. "
            "Ask questions about customers, policies, claims, and used-car inventory "
            "in plain English. Covers all three business concerns: regulatory customer "
            "count, claims performance (rejection, severity, volume), and inventory "
            "aging (unsold listings, regional demand). 5 curated tables with explicit "
            "join rules and corrupted-field safeguards baked into the instructions."
        ),
        "warehouse_id": "8d2c586aebb179ed",
        "serialized_space": json.dumps(build_serialized_space()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--create", action="store_true", help="Actually create the space")
    ap.add_argument("--profile", default="professional", help="Databricks CLI profile")
    args = ap.parse_args()

    payload = build_payload()

    if not args.create:
        ss = json.loads(payload["serialized_space"])
        preview = {**payload, "serialized_space": ss}
        print("=== DRY RUN — payload preview ===")
        print(json.dumps(preview, indent=2)[:3000])
        print("...")
        print(f"\nTables:           {len(TABLES)}")
        print(f"Text instructions: {len(TEXT_INSTRUCTIONS)}")
        print(f"Example SQLs:     {len(EXAMPLE_SQLS)}")
        print(f"Sample questions: {len(SAMPLE_QUESTIONS)}")
        print("\nRe-run with --create to actually create the space.")
        return

    result = subprocess.run(
        [
            "databricks", "api", "post", "/api/2.0/genie/spaces",
            "--profile", args.profile,
            "--json", json.dumps(payload),
        ],
        capture_output=True, text=True,
    )
    out = result.stdout
    if "-------------------------" in out:
        out = out.split("-------------------------\n", 1)[1]
    print(out)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
