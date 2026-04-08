"""
Builds and creates the PrimeInsurance Genie Space via the Databricks REST API.

Usage:
    python genie_space_config.py                # dry-run: prints payload
    python genie_space_config.py --create       # actually create the space
    python genie_space_config.py --create --profile pro

Tables included:
    primeins.gold.dim_policy
    primeins.gold.dim_customer
    primeins.gold.fact_claims

Design principles (from Databricks best-practice docs):
    1. SQL examples are the highest-leverage signal — they teach reusable patterns.
    2. Column metadata (already added via COMMENT ON COLUMN) is next.
    3. Text instructions are a LAST resort — used only for rules that SQL examples
       cannot express (join casting, corrupted fields, tier definitions, formatting).
"""

import argparse
import json
import subprocess
import uuid


def u() -> str:
    """Generate a 32-char lowercase hex UUID (no dashes) as required by the API."""
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Text instructions — the API accepts only ONE text_instructions entry, so
# all rules are consolidated into a single document. Kept minimal per the
# best-practice docs: SQL examples and column comments handle most guidance;
# text instructions only cover things SQL examples cannot express.
# ---------------------------------------------------------------------------
TEXT_INSTRUCTIONS = [
    """# PrimeInsurance Genie Space — Rules

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

## 3. Corrupted date fields
The columns `incident_date`, `claim_logged_on`, and `claim_processed_on` in
`fact_claims` are CORRUPTED AT SOURCE — only time portions survived, not full
dates. Do NOT use them for date, tenure, or SLA analysis. If a user asks about
claim processing time, tell them those fields are unusable and offer
`policy_bind_date` from `dim_policy` as the only reliable date column.

## 4. Null-as-string handling in dim_customer
The columns `region`, `job`, `marital`, and `education` may contain the string
'None' or 'NA' instead of SQL NULL. When filtering, exclude them with
`WHERE column NOT IN ('None', 'NA')` rather than `IS NOT NULL`.

## 5. Premium tier definitions
When users ask about policy tiers, use these thresholds on `policy_annual_premium`:

- **Basic**: < $1,000
- **Standard**: $1,000 – $1,499
- **Premium**: >= $1,500

These are the authoritative tier definitions for this dataset.

## 6. Clarification rule
When a user asks about "sales", "revenue", or "subscriptions" (which do not
exist in this dataset), ask: "This dataset covers auto insurance policies,
customers, and claims — it does not have sales/revenue data. Did you mean
annual premium revenue from policies, or total claim payouts?"

## Instructions you must follow when providing summaries
- Format currency with a `$` prefix and thousands separators (e.g., $12,652.40).
- Round percentages to 2 decimal places.
- When showing US states, use the 2-letter abbreviation as stored. The dataset
  only contains: OH, IN, IL, SC, NY, WV, NC, VA, PA.
- When a user asks about "claims processing time" or "SLA", respond that those
  date fields are corrupted and cannot be analyzed.
"""
]


# ---------------------------------------------------------------------------
# Example SQL queries — the highest-leverage signal for Genie
# Each entry teaches Genie one reusable pattern. Titles are phrased the way
# a user would actually ask the question, because Genie matches prompts to
# example titles via embedding similarity.
# ---------------------------------------------------------------------------
EXAMPLE_SQLS = [
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
        "question": "What is the average claim amount by incident type?",
        "sql": """SELECT
  incident_type,
  COUNT(*) AS num_claims,
  ROUND(AVG(total_claim_amount), 2) AS avg_claim_amount,
  ROUND(MIN(total_claim_amount), 2) AS min_claim_amount,
  ROUND(MAX(total_claim_amount), 2) AS max_claim_amount
FROM primeins.gold.fact_claims
GROUP BY incident_type
ORDER BY avg_claim_amount DESC""",
    },
    {
        "question": "Show me claims joined to policies and customers with the cast",
        "sql": """-- Canonical 3-way join pattern. Use this as the template whenever a
-- question needs columns from more than one of fact_claims, dim_policy, dim_customer.
SELECT
  c.claim_id,
  c.total_claim_amount,
  c.incident_severity,
  p.policy_state,
  p.policy_annual_premium,
  cust.region AS customer_region,
  cust.job AS customer_job
FROM primeins.gold.fact_claims c
JOIN primeins.gold.dim_policy p
  ON c.policy_number = CAST(p.policy_number AS STRING)
JOIN primeins.gold.dim_customer cust
  ON p.customer_id = cust.customer_id
LIMIT 100""",
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
]


# ---------------------------------------------------------------------------
# Sample questions — shown in the Genie UI as clickable prompts
# Should mirror the example SQL questions so users can click-to-run.
# ---------------------------------------------------------------------------
SAMPLE_QUESTIONS = [
    "What is the claim rejection rate by incident severity?",
    "Which states have the highest total claim amounts?",
    "How many policies are in each premium tier?",
    "What is the average claim amount by incident type?",
    "Which policies include umbrella coverage and what are their limits?",
    "Break down customers by region and show their average policy premium",
    "How many claims were rejected versus approved, and what was the average amount for each?",
]


# ---------------------------------------------------------------------------
# Table descriptions — supplement the existing table/column comments
# ---------------------------------------------------------------------------
TABLES = [
    {
        "identifier": "primeins.gold.dim_customer",
        "description": [
            "Customer dimension. ~1,604 deduplicated customers harmonized from 7 "
            "regional files. Demographics (job, marital, education, region/state/city) "
            "and financial flags (balance, hh_insurance, car_loan, default_flag). "
            "Join path to claims: dim_customer -> dim_policy -> fact_claims."
        ],
    },
    {
        "identifier": "primeins.gold.dim_policy",
        "description": [
            "Policy dimension. One row per auto insurance policy (~999 rows). "
            "Contains coverage details (CSL, deductible, umbrella), annual premium, "
            "state, bind date, and foreign keys to dim_car and dim_customer. "
            "Join to fact_claims via CAST(policy_number AS STRING)."
        ],
    },
    {
        "identifier": "primeins.gold.fact_claims",
        "description": [
            "Claims fact table. 1,000 rows, one per claim. Includes claim amounts "
            "(injury, property, vehicle, total), incident type/severity/location, "
            "rejection status, and authorities contacted. WARNING: date fields "
            "(incident_date, claim_logged_on, claim_processed_on) are corrupted "
            "and should NOT be used for date analysis. Join to dim_policy using "
            "fact_claims.policy_number = CAST(dim_policy.policy_number AS STRING)."
        ],
    },
]


def build_serialized_space() -> dict:
    """Build the serialized_space dict that will be JSON-encoded and sent.

    NOTE: The API requires all ID-bearing lists (tables, text_instructions,
    example_question_sqls, sample_questions) to be sorted by their key.
    Tables sort by identifier; the others sort by generated id.
    """
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
        "title": "PrimeInsurance Policy & Claims Intelligence",
        "description": (
            "Natural-language interface over the Gold layer of the PrimeInsurance "
            "POC. Ask questions about policies, customers, and claims in plain English. "
            "Covers coverage tiers, rejection analysis, regional breakdowns, and "
            "umbrella coverage. Based on dim_policy, dim_customer, and fact_claims."
        ),
        "warehouse_id": "8d2c586aebb179ed",
        "serialized_space": json.dumps(build_serialized_space()),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--create", action="store_true", help="Actually create the space")
    ap.add_argument("--profile", default="pro", help="Databricks CLI profile")
    args = ap.parse_args()

    payload = build_payload()

    if not args.create:
        print("=== DRY RUN — payload preview ===")
        # Print a prettified version of the serialized_space for inspection
        ss = json.loads(payload["serialized_space"])
        preview = {**payload, "serialized_space": ss}
        print(json.dumps(preview, indent=2))
        print()
        print(f"Tables: {len(TABLES)}")
        print(f"Text instructions: {len(TEXT_INSTRUCTIONS)}")
        print(f"Example SQLs: {len(EXAMPLE_SQLS)}")
        print(f"Sample questions: {len(SAMPLE_QUESTIONS)}")
        print()
        print("Re-run with --create to actually create the space.")
        return

    result = subprocess.run(
        [
            "databricks", "api", "post", "/api/2.0/genie/spaces",
            "--profile", args.profile,
            "--json", json.dumps(payload),
        ],
        capture_output=True, text=True,
    )
    # Strip the CLI version banner if present
    out = result.stdout
    if "-------------------------" in out:
        out = out.split("-------------------------\n", 1)[1]
    print(out)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
