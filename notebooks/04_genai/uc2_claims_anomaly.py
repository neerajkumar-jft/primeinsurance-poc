# Databricks notebook source
# MAGIC %md
# MAGIC # UC2: Claims anomaly detection
# MAGIC Statistical scoring first, LLM explanation second.
# MAGIC
# MAGIC We score every claim against 5 weighted rules using MAD-based z-scores
# MAGIC and IQR thresholds. Claims above the scoring threshold get an AI-generated
# MAGIC investigation brief. The LLM is not a classifier, it's an explainer.
# MAGIC
# MAGIC Output: prime_insurance_jellsinki_poc.gold.claim_anomaly_explanations

# COMMAND ----------

# MAGIC %pip install openai
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: LLM client setup

# COMMAND ----------

from openai import OpenAI
import json

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)
MODEL = "databricks-gpt-oss-20b"
print(f"Model: {MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load claims and compute robust statistics

# COMMAND ----------

from pyspark.sql import functions as F

# Read silver claims joined with policy for premium context
claims = spark.sql("""
    SELECT c.*, p.policy_annual_premium, p.policy_deductible, p.umbrella_limit
    FROM prime_insurance_jellsinki_poc.silver.claims_cleaned c
    LEFT JOIN prime_insurance_jellsinki_poc.silver.policy_cleaned p
      ON c.policy_id = p.policy_number
""")

# Add total_amount column
claims = claims.withColumn(
    "total_amount",
    F.coalesce(F.col("injury"), F.lit(0)) +
    F.coalesce(F.col("property_amount"), F.lit(0)) +
    F.coalesce(F.col("vehicle"), F.lit(0))
)

claims.createOrReplaceTempView("claims_temp")
claims = spark.table("claims_temp")
total_claims = claims.count()
print(f"Total claims: {total_claims}")

# Rule 1 stats: MAD-based z-score for total claim amount
# MAD is more robust than stddev for skewed claim distributions
median_amount = float(claims.approxQuantile("total_amount", [0.5], 0.01)[0])
mad_amount = float(claims.withColumn("abs_dev", F.abs(F.col("total_amount") - median_amount))
    .approxQuantile("abs_dev", [0.5], 0.01)[0])
# Scale MAD to be comparable to stddev for normal distributions
mad_scaled = mad_amount * 1.4826 if mad_amount > 0 else 1.0

print(f"Total amount: median={median_amount:.0f}, MAD={mad_amount:.0f}, MAD_scaled={mad_scaled:.0f}")

# Rule 2 stats: IQR per severity band for severity-amount mismatch
severity_stats = {}
for sev_row in claims.groupBy("incident_severity").agg(
    F.expr("percentile_approx(total_amount, 0.25)").alias("q1"),
    F.expr("percentile_approx(total_amount, 0.75)").alias("q3"),
).collect():
    sev = sev_row["incident_severity"]
    q1 = float(sev_row["q1"])
    q3 = float(sev_row["q3"])
    iqr = q3 - q1
    severity_stats[sev] = {"q1": q1, "q3": q3, "iqr": iqr, "upper": q3 + 1.5 * iqr}
    print(f"  {sev}: Q1={q1:.0f}, Q3={q3:.0f}, IQR={iqr:.0f}, upper_fence={q3 + 1.5 * iqr:.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Define 5 anomaly scoring rules
# MAGIC
# MAGIC | # | Rule | Weight | Method | Fraud indicator |
# MAGIC |---|------|--------|--------|-----------------|
# MAGIC | 1 | High claim amount outlier | 25 | MAD z-score > 2.0 | Inflated claims, top ~5% contain higher fraud proportion |
# MAGIC | 2 | Severity-amount mismatch | 25 | IQR per severity band | $50K claim on Minor Damage is structurally suspicious |
# MAGIC | 3 | No police report, high amount | 20 | Binary: no report AND amount > median | No independent verification of the incident |
# MAGIC | 4 | No witnesses with bodily injuries | 15 | Binary: witnesses=0 AND bodily_injuries>0 | Staged accident pattern, hard to verify injuries |
# MAGIC | 5 | Single-vehicle total loss | 15 | Binary: Single Vehicle AND Total Loss | "Owner give-up" fraud scheme |

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Broadcast severity stats for IQR lookup
sev_upper = severity_stats

# Score each claim
scored = claims.withColumn(
    # Rule 1: High claim amount - MAD z-score > 2.0 -> 25 points
    # Using MAD because claim amounts are heavily right-skewed.
    # Standard deviation would be distorted by a few very large claims.
    "rule1_high_amount",
    F.when(
        F.abs(F.col("total_amount") - median_amount) / mad_scaled > 2.0,
        F.lit(25)
    ).otherwise(F.lit(0))
).withColumn(
    # Rule 2: Severity-amount mismatch - IQR per severity band -> 25 points
    # A claim is flagged if amount > Q3 + 1.5*IQR for its severity category.
    # e.g. a $50K claim categorized as Minor Damage is structurally suspicious.
    "rule2_severity_mismatch",
    F.when(
        (F.col("incident_severity") == "Minor Damage") &
        (F.col("total_amount") > sev_upper.get("Minor Damage", {}).get("upper", 999999)),
        F.lit(25)
    ).when(
        (F.col("incident_severity") == "Major Damage") &
        (F.col("total_amount") > sev_upper.get("Major Damage", {}).get("upper", 999999)),
        F.lit(25)
    ).when(
        (F.col("incident_severity") == "Total Loss") &
        (F.col("total_amount") > sev_upper.get("Total Loss", {}).get("upper", 999999)),
        F.lit(25)
    ).otherwise(F.lit(0))
).withColumn(
    # Rule 3: No police report on above-median claim -> 20 points
    # Legitimate high-value accidents almost always produce police documentation.
    # Without a report there is no independent verification of the incident.
    "rule3_no_police_report",
    F.when(
        (F.col("police_report_available").isNull() | (F.col("police_report_available") == "NO")) &
        (F.col("total_amount") > median_amount),
        F.lit(20)
    ).otherwise(F.lit(0))
).withColumn(
    # Rule 4: No witnesses with bodily injuries -> 15 points
    # Injury claims without witnesses are difficult to independently verify.
    # This pattern often appears in staged accident schemes.
    "rule4_no_witnesses_injury",
    F.when(
        (F.col("witnesses") == 0) &
        (F.col("bodily_injuries") > 0),
        F.lit(15)
    ).otherwise(F.lit(0))
).withColumn(
    # Rule 5: Single-vehicle total loss -> 15 points
    # Aligns with the "owner give-up" scheme where a vehicle is intentionally
    # destroyed to collect insurance. Legitimate events (deer strikes, black ice)
    # can also produce this, so the weight is moderate.
    "rule5_single_vehicle_total_loss",
    F.when(
        (F.col("incident_type") == "Single Vehicle Collision") &
        (F.col("incident_severity") == "Total Loss"),
        F.lit(15)
    ).otherwise(F.lit(0))
).withColumn(
    "anomaly_score",
    F.col("rule1_high_amount") + F.col("rule2_severity_mismatch") +
    F.col("rule3_no_police_report") + F.col("rule4_no_witnesses_injury") +
    F.col("rule5_single_vehicle_total_loss")
).withColumn(
    # HIGH >= 60: multiple indicators firing together (auto SIU referral)
    # MEDIUM >= 35: one major + one minor indicator (enhanced adjuster review)
    "priority_tier",
    F.when(F.col("anomaly_score") >= 60, F.lit("HIGH"))
     .when(F.col("anomaly_score") >= 35, F.lit("MEDIUM"))
     .otherwise(F.lit("NONE"))
).withColumn(
    "triggered_rules",
    F.concat_ws(", ",
        F.when(F.col("rule1_high_amount") > 0, F.lit("high_amount_outlier")),
        F.when(F.col("rule2_severity_mismatch") > 0, F.lit("severity_amount_mismatch")),
        F.when(F.col("rule3_no_police_report") > 0, F.lit("no_police_report_high_value")),
        F.when(F.col("rule4_no_witnesses_injury") > 0, F.lit("no_witnesses_bodily_injury")),
        F.when(F.col("rule5_single_vehicle_total_loss") > 0, F.lit("single_vehicle_total_loss")),
    )
)

# Filter to HIGH and MEDIUM only (score >= 35)
flagged = scored.filter(F.col("anomaly_score") >= 35)

print(f"Flagged claims: {flagged.count()} out of {total_claims}")
print(f"\nBy tier:")
flagged.groupBy("priority_tier").count().orderBy("priority_tier").show()

# Also show total scored distribution
print("Score distribution (all claims):")
scored.groupBy("priority_tier").count().orderBy("priority_tier").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate AI investigation briefs for HIGH and MEDIUM claims

# COMMAND ----------

def parse_llm_response(response):
    content = response.choices[0].message.content
    try:
        parsed = json.loads(content)
        if isinstance(parsed, list):
            for block in parsed:
                if block.get("type") == "text":
                    return block.get("text", content)
        return content
    except (json.JSONDecodeError, TypeError):
        return content


def generate_brief(claim):
    prompt = f"""You are a claims fraud investigator at PrimeInsurance.
Write a brief investigation summary for this flagged claim. Be specific,
reference the actual numbers. No generic statements.

Claim #{claim.get('claim_id')}:
- Total amount: ${claim.get('total_amount', 0):,.0f} (injury: ${claim.get('injury') or 0:,.0f}, property: ${claim.get('property_amount') or 0:,.0f}, vehicle: ${claim.get('vehicle') or 0:,.0f})
- Incident type: {claim.get('incident_type')}, Severity: {claim.get('incident_severity')}
- Collision type: {claim.get('collision_type')}
- Vehicles involved: {claim.get('number_of_vehicles_involved')}, Witnesses: {claim.get('witnesses')}
- Bodily injuries: {claim.get('bodily_injuries')}
- Police report: {claim.get('police_report_available')}, Property damage: {claim.get('property_damage')}
- Processing time: {claim.get('days_to_process')} days
- Claim rejected: {claim.get('claim_rejected')}
- Anomaly score: {claim.get('anomaly_score')}/100 ({claim.get('priority_tier')})
- Rules triggered: {claim.get('triggered_rules')}
- Policy premium: ${claim.get('policy_annual_premium') or 0:,.0f}

Answer three questions:
1. What makes this claim suspicious? (cite the specific numbers)
2. Which risk factors are present?
3. What should the investigator do next? (specific, actionable steps)

Under 200 words."""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=400,
        temperature=0.3
    )
    return parse_llm_response(response)

# COMMAND ----------

# Only generate briefs for HIGH and MEDIUM, limit to 50 for cost
flagged_rows = flagged.orderBy(F.col("anomaly_score").desc()).limit(50).collect()

results = []
for i, row in enumerate(flagged_rows):
    claim = row.asDict()
    print(f"[{i+1}/{len(flagged_rows)}] Claim #{claim['claim_id']} (score: {claim['anomaly_score']}, {claim['priority_tier']})...")

    try:
        brief = generate_brief(claim)
    except Exception as e:
        brief = f"Brief generation failed: {str(e)}"

    results.append({
        "claim_id": claim.get("claim_id"),
        "policy_id": claim.get("policy_id"),
        "incident_type": claim.get("incident_type"),
        "incident_severity": claim.get("incident_severity"),
        "total_amount": float(claim.get("total_amount") or 0),
        "injury": float(claim.get("injury") or 0),
        "property_amount": float(claim.get("property_amount") or 0),
        "vehicle": float(claim.get("vehicle") or 0),
        "days_to_process": claim.get("days_to_process"),
        "claim_rejected": claim.get("claim_rejected"),
        "anomaly_score": int(claim.get("anomaly_score") or 0),
        "priority_tier": claim.get("priority_tier"),
        "triggered_rules": claim.get("triggered_rules"),
        "investigation_brief": brief,
    })

print(f"\nGenerated {len(results)} investigation briefs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to gold.claim_anomaly_explanations

# COMMAND ----------

schema = StructType([
    StructField("claim_id", IntegerType(), False),
    StructField("policy_id", IntegerType(), True),
    StructField("incident_type", StringType(), True),
    StructField("incident_severity", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("injury", DoubleType(), True),
    StructField("property_amount", DoubleType(), True),
    StructField("vehicle", DoubleType(), True),
    StructField("days_to_process", IntegerType(), True),
    StructField("claim_rejected", StringType(), True),
    StructField("anomaly_score", IntegerType(), False),
    StructField("priority_tier", StringType(), False),
    StructField("triggered_rules", StringType(), True),
    StructField("investigation_brief", StringType(), True),
])

df = spark.createDataFrame(results, schema)
df.write.mode("overwrite").saveAsTable(
    "prime_insurance_jellsinki_poc.gold.claim_anomaly_explanations"
)
print("Written to prime_insurance_jellsinki_poc.gold.claim_anomaly_explanations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify output

# COMMAND ----------

# MAGIC %sql
SELECT claim_id, anomaly_score, priority_tier, triggered_rules,
       total_amount, incident_severity, investigation_brief
FROM prime_insurance_jellsinki_poc.gold.claim_anomaly_explanations
ORDER BY anomaly_score DESC
LIMIT 10;

# COMMAND ----------

# MAGIC %sql
-- Summary by priority tier
SELECT priority_tier, COUNT(*) AS claims, ROUND(AVG(anomaly_score), 1) AS avg_score,
       ROUND(AVG(total_amount), 0) AS avg_amount
FROM prime_insurance_jellsinki_poc.gold.claim_anomaly_explanations
GROUP BY priority_tier
ORDER BY CASE WHEN priority_tier = 'HIGH' THEN 1 WHEN priority_tier = 'MEDIUM' THEN 2 ELSE 3 END;
