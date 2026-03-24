# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Claims Anomaly Detection — UC2
# MAGIC Statistical fraud-scoring system. PySpark for rule-based anomaly detection, LLM for narrative investigation briefs.

# COMMAND ----------

%pip install openai mlflow tenacity pydantic --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# auto-detect catalog
CATALOG = "prime-ins-jellsinki-poc"
spark.sql(f"USE CATALOG `{CATALOG}`")
print(f"using catalog: {CATALOG}")

# COMMAND ----------

# ============================================================
# CONFIGURATION & SETUP
# ============================================================

import mlflow
import json
import re
from datetime import datetime
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_random_exponential
from pydantic import BaseModel, Field, field_validator
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── MLflow Tracing ──
mlflow.openai.autolog()

# ── LLM Connection ──
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

# ── Constants ──
MODEL_NAME = "databricks-gpt-oss-20b"
MAX_TOKENS = 1500
SOURCE_TABLE = f"`{CATALOG}`.silver.claims"
OUTPUT_TABLE = f"`{CATALOG}`.gold.claim_anomaly_explanations"
PROMPT_VERSION = "v1"

# ── Scoring Thresholds ──
HIGH_THRESHOLD = 60    # Auto-refer to SIU
MEDIUM_THRESHOLD = 35  # Enhanced adjuster review

# ── Response parser ──
def extract_text(raw_response):
    """Extracts text block from databricks-gpt-oss-20b structured response."""
    if isinstance(raw_response, list):
        parsed = raw_response
    elif isinstance(raw_response, str):
        try:
            parsed = json.loads(raw_response)
        except json.JSONDecodeError:
            return raw_response
    else:
        return str(raw_response)
    for block in parsed:
        if block.get("type") == "text":
            return block["text"]
    return str(raw_response)

# ── LLM call with retry ──
@retry(
    wait=wait_random_exponential(min=2, max=60),
    stop=stop_after_attempt(5),
)
def call_llm(messages, max_tokens=MAX_TOKENS):
    """Calls LLM and returns parsed dict."""
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=messages,
        max_tokens=max_tokens,
    )
    raw = response.choices[0].message.content
    text = extract_text(raw)
    first_brace = text.find('{')
    last_brace = text.rfind('}')
    if first_brace != -1 and last_brace > first_brace:
        return json.loads(text[first_brace:last_brace + 1])
    return json.loads(text)

# ── Guardrails ──
def apply_guardrails(text):
    text = re.sub(r'\b[\w.-]+@[\w.-]+\.\w+\b', '[REDACTED_EMAIL]', text)
    text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[REDACTED_PHONE]', text)
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[REDACTED_SSN]', text)
    return text

print(f"✅ Connected to {WORKSPACE_URL}")
print(f"✅ Model: {MODEL_NAME}")
print(f"✅ Thresholds: HIGH >= {HIGH_THRESHOLD}, MEDIUM >= {MEDIUM_THRESHOLD}")

# COMMAND ----------

# ============================================================
# PYDANTIC OUTPUT MODEL — Investigation Brief
# ============================================================

class InvestigationBrief(BaseModel):
    """AI-generated investigation brief for a flagged claim."""

    what_is_suspicious: str = Field(min_length=20, description="What makes this claim suspicious, referencing specific data points")
    risk_factors: str = Field(min_length=20, description="Which fraud risk factors are present and why they matter")
    recommended_action: str = Field(min_length=20, description="Specific, actionable next steps for the investigator")

    @field_validator('*', mode='before')
    @classmethod
    def clean_input(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('n/a', 'none', 'null', '', '-'):
                return "[Not provided by model — requires manual review]"
        return v

    def to_formatted_text(self) -> str:
        return (
            f"WHAT IS SUSPICIOUS:\n{self.what_is_suspicious}\n\n"
            f"RISK FACTORS:\n{self.risk_factors}\n\n"
            f"RECOMMENDED ACTION:\n{self.recommended_action}"
        )

print("✅ Pydantic model ready")

# COMMAND ----------

# ============================================================
# LOAD CLAIMS DATA & COMPUTE TOTAL AMOUNT
# ============================================================

claims_df = spark.sql(f"SELECT * FROM {SOURCE_TABLE}")

total_claims = claims_df.count()
print(f"✅ Loaded {total_claims} claims from {SOURCE_TABLE}")
claims_df.select("claim_id", "total_claim_amount", "incident_severity", "incident_type",
                "bodily_injuries", "witnesses", "police_report_available").show(5, truncate=False)

# COMMAND ----------

# ============================================================
# ANOMALY DETECTION — 5 WEIGHTED FRAUD-INDICATOR RULES
# ============================================================
#
# APPROACH:
#   - Detection is 100% statistical (PySpark). No LLM involved.
#   - The LLM is only used AFTER detection, to write investigation briefs.
#   - "The LLM is not a classifier; it is an explainer."
#
# RULES & JUSTIFICATION:
#
# ┌────┬──────────────────────────────────┬────────┬─────────────────────────────────────────────┐
# │ #  │ Rule                             │ Weight │ Why (Industry Basis)                        │
# ├────┼──────────────────────────────────┼────────┼─────────────────────────────────────────────┤
# │ 1  │ High claim amount (Z-score)      │ 25     │ Fraudulent claims are systematically        │
# │    │                                  │        │ inflated. Robust Z-score (MAD-based)        │
# │    │                                  │        │ catches top outliers. NICB Tier-1 indicator. │
# ├────┼──────────────────────────────────┼────────┼─────────────────────────────────────────────┤
# │ 2  │ Severity-amount mismatch         │ 25     │ A $50K claim on "Minor Damage" is           │
# │    │                                  │        │ structurally suspicious. IQR method per      │
# │    │                                  │        │ severity band detects mismatches.            │
# ├────┼──────────────────────────────────┼────────┼─────────────────────────────────────────────┤
# │ 3  │ No police report + high amount   │ 20     │ Legitimate high-value accidents almost       │
# │    │                                  │        │ always have police reports. No report =      │
# │    │                                  │        │ no independent verification. NICB top-10.    │
# ├────┼──────────────────────────────────┼────────┼─────────────────────────────────────────────┤
# │ 4  │ No witnesses + bodily injuries   │ 15     │ Injury claims with zero witnesses are hard   │
# │    │                                  │        │ to verify. Common in staged accidents.       │
# ├────┼──────────────────────────────────┼────────┼─────────────────────────────────────────────┤
# │ 5  │ Single vehicle total loss        │ 15     │ "Owner give-ups" — intentionally destroying  │
# │    │                                  │        │ a car to collect insurance. One of the most  │
# │    │                                  │        │ common auto fraud schemes per NICB.          │
# └────┴──────────────────────────────────┴────────┴─────────────────────────────────────────────┘
#
# SCORING: Max possible = 100. Threshold: HIGH >= 60, MEDIUM >= 35
# ============================================================

# ── Rule 1: High Claim Amount Outlier (Robust Z-Score using MAD) ──
# MAD (Median Absolute Deviation) is more robust than stddev for skewed data
median_amount = claims_df.approxQuantile("total_claim_amount", [0.5], 0.01)[0]
claims_with_dev = claims_df.withColumn("abs_deviation", F.abs(F.col("total_claim_amount") - F.lit(median_amount)))
mad = claims_with_dev.approxQuantile("abs_deviation", [0.5], 0.01)[0]
# Scale MAD to be consistent with stddev (1.4826 for normal distributions)
mad_scaled = mad * 1.4826

scored_df = claims_df.withColumn(
    "rule1_zscore",
    F.round((F.col("total_claim_amount") - F.lit(median_amount)) / F.lit(mad_scaled), 2)
).withColumn(
    "rule1_amount_outlier",
    F.when(F.col("rule1_zscore") > 2.0, 25).otherwise(0)
)

print(f"📊 Rule 1 — Amount Outlier (Robust Z-Score)")
print(f"   Median amount: ${median_amount:,.0f}")
print(f"   MAD (scaled): ${mad_scaled:,.0f}")
print(f"   Threshold: Z > 2.0 → 25 points")
print(f"   Flagged: {scored_df.filter(F.col('rule1_amount_outlier') > 0).count()} claims")


# ── Rule 2: Severity-Amount Mismatch (IQR per severity band) ──
severity_stats = claims_df.groupBy("incident_severity").agg(
    F.expr("percentile_approx(total_claim_amount, 0.25)").alias("Q1"),
    F.expr("percentile_approx(total_claim_amount, 0.75)").alias("Q3"),
)
severity_stats = severity_stats.withColumn("IQR", F.col("Q3") - F.col("Q1"))
severity_stats = severity_stats.withColumn("upper_fence", F.col("Q3") + 1.5 * F.col("IQR"))

scored_df = scored_df.join(
    severity_stats.select("incident_severity", "upper_fence"),
    "incident_severity",
    "left"
).withColumn(
    "rule2_severity_mismatch",
    F.when(F.col("total_claim_amount") > F.col("upper_fence"), 25).otherwise(0)
).drop("upper_fence")

print(f"\n📊 Rule 2 — Severity-Amount Mismatch (IQR)")
severity_stats.show(truncate=False)
print(f"   Flagged: {scored_df.filter(F.col('rule2_severity_mismatch') > 0).count()} claims")


# ── Rule 3: No Police Report + High Amount ──
scored_df = scored_df.withColumn(
    "rule3_no_police_high_amount",
    F.when(
        (F.col("police_report_available") == "NO") &
        (F.col("total_claim_amount") > F.lit(median_amount)),
        20
    ).otherwise(0)
)

print(f"\n📊 Rule 3 — No Police Report + High Amount")
print(f"   Threshold: police_report = NO AND amount > ${median_amount:,.0f}")
print(f"   Flagged: {scored_df.filter(F.col('rule3_no_police_high_amount') > 0).count()} claims")


# ── Rule 4: No Witnesses + Bodily Injuries ──
scored_df = scored_df.withColumn(
    "rule4_no_witness_injuries",
    F.when(
        (F.col("witnesses") == 0) &
        (F.col("bodily_injuries") > 0),
        15
    ).otherwise(0)
)

print(f"\n📊 Rule 4 — No Witnesses + Bodily Injuries")
print(f"   Threshold: witnesses = 0 AND bodily_injuries > 0")
print(f"   Flagged: {scored_df.filter(F.col('rule4_no_witness_injuries') > 0).count()} claims")


# ── Rule 5: Single Vehicle Total Loss ──
scored_df = scored_df.withColumn(
    "rule5_single_vehicle_total_loss",
    F.when(
        (F.col("incident_type") == "Single Vehicle Collision") &
        (F.col("incident_severity") == "Total Loss"),
        15
    ).otherwise(0)
)

print(f"\n📊 Rule 5 — Single Vehicle Total Loss")
print(f"   Threshold: Single Vehicle Collision + Total Loss")
print(f"   Flagged: {scored_df.filter(F.col('rule5_single_vehicle_total_loss') > 0).count()} claims")


# ── Calculate Total Anomaly Score ──
scored_df = scored_df.withColumn(
    "anomaly_score",
    F.col("rule1_amount_outlier") +
    F.col("rule2_severity_mismatch") +
    F.col("rule3_no_police_high_amount") +
    F.col("rule4_no_witness_injuries") +
    F.col("rule5_single_vehicle_total_loss")
)

# ── Assign Priority Tier ──
scored_df = scored_df.withColumn(
    "priority",
    F.when(F.col("anomaly_score") >= HIGH_THRESHOLD, "HIGH")
    .when(F.col("anomaly_score") >= MEDIUM_THRESHOLD, "MEDIUM")
    .otherwise("LOW")
)

# ── Collect triggered rules as a string ──
scored_df = scored_df.withColumn(
    "triggered_rules",
    F.concat_ws(", ",
        F.when(F.col("rule1_amount_outlier") > 0, F.lit("Amount Outlier (Z-Score)")),
        F.when(F.col("rule2_severity_mismatch") > 0, F.lit("Severity-Amount Mismatch (IQR)")),
        F.when(F.col("rule3_no_police_high_amount") > 0, F.lit("No Police Report + High Amount")),
        F.when(F.col("rule4_no_witness_injuries") > 0, F.lit("No Witnesses + Bodily Injuries")),
        F.when(F.col("rule5_single_vehicle_total_loss") > 0, F.lit("Single Vehicle Total Loss")),
    )
)

# ── Summary ──
print(f"\n{'='*60}")
print(f"  ANOMALY SCORING SUMMARY")
print(f"{'='*60}")
print(f"  Total claims scored: {total_claims}")
print(f"  HIGH priority (>= {HIGH_THRESHOLD}): {scored_df.filter(F.col('priority') == 'HIGH').count()}")
print(f"  MEDIUM priority (>= {MEDIUM_THRESHOLD}): {scored_df.filter(F.col('priority') == 'MEDIUM').count()}")
print(f"  LOW priority (< {MEDIUM_THRESHOLD}): {scored_df.filter(F.col('priority') == 'LOW').count()}")
flagged_count = scored_df.filter(F.col("priority").isin("HIGH", "MEDIUM")).count()
print(f"  Total flagged for review: {flagged_count} ({flagged_count/total_claims*100:.1f}%)")
print(f"{'='*60}")

# COMMAND ----------

# ============================================================
# PREVIEW — All claims sorted by anomaly score
# ============================================================

display(scored_df.select(
    "claim_id", "total_claim_amount", "incident_severity", "incident_type",
    "anomaly_score", "priority", "triggered_rules",
    "rule1_zscore", "bodily_injuries", "witnesses", "police_report_available"
).orderBy(F.col("anomaly_score").desc()))

# COMMAND ----------

# ============================================================
# GENERATE AI INVESTIGATION BRIEFS — Only for HIGH/MEDIUM claims
# ============================================================
# The LLM does NOT score claims. It writes investigation briefs
# for claims that were ALREADY flagged by statistical rules.
# ============================================================

flagged_df = scored_df.filter(F.col("priority").isin("HIGH", "MEDIUM"))
flagged_claims = [row.asDict() for row in flagged_df.collect()]

print(f"📊 {len(flagged_claims)} claims to generate briefs for\n")

if len(flagged_claims) == 0:
    print("No claims flagged — nothing to send to LLM.")
    results = []
else:
    SYSTEM_PROMPT = """You are a senior insurance fraud investigator at PrimeInsurance with 20 years of experience in auto claims fraud detection. You write investigation briefs for the
Special Investigations Unit (SIU).

You are given a claim that has been statistically flagged as anomalous by the automated scoring system. Your job is to write a concise investigation brief explaining WHY this claim is
suspicious and WHAT the investigator should do next.

You must always respond in json format with exactly these 3 keys:
{
    "what_is_suspicious": "2-3 sentences. Reference specific data points — dollar amounts, severity, number of vehicles, witnesses. Explain what pattern the data shows.",
    "risk_factors": "2-3 sentences. Which specific fraud indicators are present. Reference industry patterns like staged accidents, owner give-ups, inflated claims.",
    "recommended_action": "2-3 sentences. Specific next steps — not generic. Reference what to verify, who to interview, what records to pull."
}

<constraints>
- Reference the ACTUAL numbers from the claim data
- Do NOT invent facts not present in the claim
- Do NOT be vague — an investigator must be able to act on your brief
- Focus on the triggered rules and explain why they matter together
</constraints>"""

    USER_PROMPT_TEMPLATE = """Claim flagged for investigation:

- Claim ID: {claim_id}
- Anomaly Score: {anomaly_score}/100 (Priority: {priority})
- Triggered Rules: {triggered_rules}

Claim Details:
- Total Amount: ${total_claim_amount:,.0f} (Injury: ${injury:,.0f}, Property: ${property:,.0f}, Vehicle: ${vehicle:,.0f})
- Incident Type: {incident_type}
- Collision Type: {collision_type}
- Severity: {incident_severity}
- Vehicles Involved: {vehicles_involved}
- Bodily Injuries: {bodily_injuries}
- Witnesses: {witnesses}
- Police Report: {police_report_available}
- Authorities Contacted: {authorities_contacted}
- Claim Rejected: {claim_rejected}
- Location: {incident_city}, {incident_state}
- Incident Date: {incident_date}
- Claim Logged: {claim_logged_on}
- Claim Processed: {claim_processed_on}"""

    current_user = spark.sql("SELECT current_user()").collect()[0][0]
    mlflow.set_experiment(f"/Users/{current_user}/prime_ins_poc_claims_anomaly")

    results = []

    with mlflow.start_run(run_name=f"claims_anomaly_{datetime.now().strftime('%Y%m%d_%H%M')}") as run:

        mlflow.set_tags({
            "model": MODEL_NAME,
            "prompt_version": PROMPT_VERSION,
            "pipeline": "claims_anomaly",
            "source_table": SOURCE_TABLE,
            "output_table": OUTPUT_TABLE,
        })
        mlflow.log_params({
            "total_claims": total_claims,
            "flagged_claims": len(flagged_claims),
            "high_threshold": HIGH_THRESHOLD,
            "medium_threshold": MEDIUM_THRESHOLD,
            "num_rules": 5,
            "model": MODEL_NAME,
        })

        success_count = 0
        fail_count = 0

        for idx, claim in enumerate(flagged_claims):
            issue_start = datetime.now()

            try:
                messages = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": USER_PROMPT_TEMPLATE.format(**claim)},
                ]

                data = call_llm(messages)
                brief = InvestigationBrief(**data)
                formatted = apply_guardrails(brief.to_formatted_text())

                duration_ms = int((datetime.now() - issue_start).total_seconds() * 1000)

                results.append({
                    "claim_id": claim["claim_id"],
                    "total_claim_amount": float(claim["total_claim_amount"]),
                    "incident_severity": claim["incident_severity"],
                    "incident_type": claim["incident_type"],
                    "anomaly_score": int(claim["anomaly_score"]),
                    "priority": claim["priority"],
                    "triggered_rules": claim["triggered_rules"],
                    "what_is_suspicious": brief.what_is_suspicious,
                    "risk_factors": brief.risk_factors,
                    "recommended_action": brief.recommended_action,
                    "investigation_brief": formatted,
                    "model_name": MODEL_NAME,
                    "prompt_version": PROMPT_VERSION,
                    "generated_at": datetime.now().isoformat(),
                    "generation_status": "SUCCESS",
                    "duration_ms": duration_ms,
                })

                success_count += 1
                print(f"✅ [{idx+1}/{len(flagged_claims)}] {claim['claim_id']} | Score: {claim['anomaly_score']} | {claim['priority']} — {duration_ms}ms")

            except Exception as e:
                fail_count += 1
                duration_ms = int((datetime.now() - issue_start).total_seconds() * 1000)

                results.append({
                    "claim_id": claim["claim_id"],
                    "total_claim_amount": float(claim["total_claim_amount"]),
                    "incident_severity": claim["incident_severity"],
                    "incident_type": claim["incident_type"],
                    "anomaly_score": int(claim["anomaly_score"]),
                    "priority": claim["priority"],
                    "triggered_rules": claim["triggered_rules"],
                    "what_is_suspicious": f"Generation failed: {str(e)}",
                    "risk_factors": "",
                    "recommended_action": "",
                    "investigation_brief": f"Generation failed: {str(e)}",
                    "model_name": MODEL_NAME,
                    "prompt_version": PROMPT_VERSION,
                    "generated_at": datetime.now().isoformat(),
                    "generation_status": "FAILED",
                    "duration_ms": duration_ms,
                })
                print(f"[{idx+1}/{len(flagged_claims)}] {claim['claim_id']} | Error: {e}")

        total_duration = sum(r["duration_ms"] for r in results)
        mlflow.log_metrics({
            "success_count": success_count,
            "fail_count": fail_count,
            "success_rate": success_count / len(flagged_claims) if flagged_claims else 0,
            "total_duration_ms": total_duration,
            "avg_duration_ms": total_duration / len(results) if results else 0,
            "high_priority_count": sum(1 for r in results if r["priority"] == "HIGH"),
            "medium_priority_count": sum(1 for r in results if r["priority"] == "MEDIUM"),
        })

        print(f"\n{'='*60}")
        print(f"  RESULTS: {success_count} succeeded, {fail_count} failed")
        print(f"  Total time: {total_duration/1000:.1f}s")
        print(f"  MLflow Run: {run.info.run_id}")
        print(f"{'='*60}")

# COMMAND ----------

# ============================================================
# PREVIEW INVESTIGATION BRIEFS
# ============================================================

if results:
    for r in results:
        priority_icon = {"HIGH": "🔴", "MEDIUM": "🟠"}.get(r["priority"], "⚪")
        print("=" * 70)
        print(f"  {priority_icon} {r['claim_id']} | Score: {r['anomaly_score']}/100 | {r['priority']}")
        print(f"  Amount: ${r['total_claim_amount']:,.0f} | {r['incident_severity']} | {r['incident_type']}")
        print(f"  Rules: {r['triggered_rules']}")
        print("=" * 70)
        print(r["investigation_brief"])
        print()
else:
    print("No briefs to preview.")

# COMMAND ----------

# ============================================================
# SAVE TO GOLD TABLE
# ============================================================

if results:
    results_df = spark.createDataFrame(results)

    table_exists = spark.catalog.tableExists(OUTPUT_TABLE)

    if not table_exists:
        results_df.write.mode("overwrite").saveAsTable(OUTPUT_TABLE)
        print(f"✅ Created {OUTPUT_TABLE} with {len(results)} rows")
    else:
        results_df.createOrReplaceTempView("new_anomalies")
        spark.sql(f"""
            MERGE INTO {OUTPUT_TABLE} AS target
            USING new_anomalies AS source
            ON target.claim_id = source.claim_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"✅ Merged {len(results)} rows into {OUTPUT_TABLE}")

    total = spark.sql(f"SELECT COUNT(*) as cnt FROM {OUTPUT_TABLE}").collect()[0]["cnt"]
    print(f"📊 Total rows in {OUTPUT_TABLE}: {total}")
else:
    print("No results to save.")

# COMMAND ----------

# ============================================================
# VERIFY FINAL OUTPUT
# ============================================================

display(spark.sql(f"""
    SELECT
        claim_id,
        anomaly_score,
        priority,
        total_claim_amount,
        incident_severity,
        triggered_rules,
        generation_status,
        duration_ms,
        LEFT(what_is_suspicious, 120) as suspicious_preview,
        LEFT(recommended_action, 120) as action_preview
    FROM {OUTPUT_TABLE}
    ORDER BY anomaly_score DESC
"""))
