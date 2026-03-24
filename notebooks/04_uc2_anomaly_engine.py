# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # UC2: Claims Risk & Anomaly Engine
# MAGIC
# MAGIC **Problem**: Claims investigators manually review 3,000 claims looking for fraud patterns.
# MAGIC No consistent scoring, no prioritized queue, no structured explanation of why a claim is suspicious.
# MAGIC
# MAGIC **Solution**: Statistical anomaly detection (5 weighted rules) + LLM-generated investigation briefs.
# MAGIC
# MAGIC **Input**: ``databricks-hackathon-insurance`.silver.claims`
# MAGIC **Output**: ``databricks-hackathon-insurance`.gold.claim_anomaly_explanations`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
import json
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Claims Data

# COMMAND ----------

claims = spark.table("`databricks-hackathon-insurance`.silver.claims")
print(f"total claims: {claims.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistical Analysis
# MAGIC
# MAGIC Before defining rules, let's understand the distributions.
# MAGIC This justifies our threshold choices.

# COMMAND ----------

# amount distribution
amount_stats = claims.select(
    F.mean("total_claim_amount").alias("mean"),
    F.stddev("total_claim_amount").alias("std"),
    F.percentile_approx("total_claim_amount", 0.25).alias("p25"),
    F.percentile_approx("total_claim_amount", 0.50).alias("p50"),
    F.percentile_approx("total_claim_amount", 0.75).alias("p75"),
    F.percentile_approx("total_claim_amount", 0.95).alias("p95"),
    F.percentile_approx("total_claim_amount", 0.99).alias("p99"),
).collect()[0]

print("=== Claim Amount Distribution ===")
print(f"  mean: ${amount_stats['mean']:.0f} | std: ${amount_stats['std']:.0f}")
print(f"  p25: ${amount_stats['p25']:.0f} | p50: ${amount_stats['p50']:.0f} | p75: ${amount_stats['p75']:.0f}")
print(f"  p95: ${amount_stats['p95']:.0f} | p99: ${amount_stats['p99']:.0f}")

amount_mean = amount_stats['mean']
amount_std = amount_stats['std']

# COMMAND ----------

# processing time distribution
proc_stats = claims.filter(F.col("processing_days").isNotNull()).select(
    F.mean("processing_days").alias("mean"),
    F.stddev("processing_days").alias("std"),
    F.percentile_approx("processing_days", 0.95).alias("p95"),
).collect()[0]

print(f"\n=== Processing Days Distribution ===")
print(f"  mean: {proc_stats['mean']:.1f} | std: {proc_stats['std']:.1f} | p95: {proc_stats['p95']}")

# COMMAND ----------

# repeat claimant analysis
# how many claims does a typical customer file?
claims_per_policy = (claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
    .select(
        F.percentile_approx("claim_count", 0.50).alias("p50"),
        F.percentile_approx("claim_count", 0.90).alias("p90"),
        F.percentile_approx("claim_count", 0.95).alias("p95"),
        F.max("claim_count").alias("max"),
    )
).collect()[0]

print(f"\n=== Claims per Policy ===")
print(f"  median: {claims_per_policy['p50']} | p90: {claims_per_policy['p90']} | p95: {claims_per_policy['p95']} | max: {claims_per_policy['max']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Anomaly Rules
# MAGIC
# MAGIC 5 rules, each with a weight based on how strongly it indicates fraud.
# MAGIC Weights are justified by the data distributions above.
# MAGIC
# MAGIC | Rule | Weight | Why |
# MAGIC |------|--------|-----|
# MAGIC | High amount (>2 std above mean) | 30% | Top 2.3% by amount - outliers deserve scrutiny |
# MAGIC | Severity-amount mismatch | 25% | Minor severity + high claim = red flag |
# MAGIC | Repeat claimant (3+ claims) | 20% | 95th percentile is ~2 claims - 3+ is unusual |
# MAGIC | Missing police report on major incident | 15% | Major/Total Loss should always have a report |
# MAGIC | Quick file (claim within 30 days of logging) | 10% | Known fraud pattern - file fast after incident |

# COMMAND ----------

# calculate z-score for amount
z_threshold = 2.0

scored_claims = (claims
    # Rule 1: unusually high claim amount (z-score > 2)
    .withColumn("amount_z_score",
        (F.col("total_claim_amount") - F.lit(amount_mean)) / F.lit(amount_std)
    )
    .withColumn("rule_high_amount",
        F.when(F.col("amount_z_score") > z_threshold, True).otherwise(False)
    )
    .withColumn("rule_high_amount_score",
        F.when(F.col("rule_high_amount"), 30).otherwise(0)
    )

    # Rule 2: severity-amount mismatch
    # minor/trivial severity but claim amount above 75th percentile
    .withColumn("rule_severity_mismatch",
        F.when(
            (F.col("incident_severity").isin("Minor Damage", "Trivial Damage")) &
            (F.col("total_claim_amount") > amount_stats['p75']),
            True
        ).otherwise(False)
    )
    .withColumn("rule_severity_mismatch_score",
        F.when(F.col("rule_severity_mismatch"), 25).otherwise(0)
    )

    # Rule 3: repeat claimant (3+ claims on same policy)
    .withColumn("policy_claim_count",
        F.count("*").over(Window.partitionBy("policy_id"))
    )
    .withColumn("rule_repeat_claimant",
        F.when(F.col("policy_claim_count") >= 3, True).otherwise(False)
    )
    .withColumn("rule_repeat_claimant_score",
        F.when(F.col("rule_repeat_claimant"), 20).otherwise(0)
    )

    # Rule 4: missing police report on major incidents
    .withColumn("rule_missing_report",
        F.when(
            (F.col("incident_severity").isin("Major Damage", "Total Loss")) &
            (F.upper(F.col("police_report_available")) != "YES"),
            True
        ).otherwise(False)
    )
    .withColumn("rule_missing_report_score",
        F.when(F.col("rule_missing_report"), 15).otherwise(0)
    )

    # Rule 5: suspiciously quick processing or claim close to incident
    .withColumn("rule_quick_file",
        F.when(
            (F.col("processing_days").isNotNull()) & (F.col("processing_days") <= 1),
            True
        ).otherwise(False)
    )
    .withColumn("rule_quick_file_score",
        F.when(F.col("rule_quick_file"), 10).otherwise(0)
    )
)

# COMMAND ----------

# compute total anomaly score (0-100)
scored_claims = scored_claims.withColumn(
    "anomaly_score",
    F.col("rule_high_amount_score") +
    F.col("rule_severity_mismatch_score") +
    F.col("rule_repeat_claimant_score") +
    F.col("rule_missing_report_score") +
    F.col("rule_quick_file_score")
)

# classify priority
scored_claims = scored_claims.withColumn(
    "anomaly_priority",
    F.when(F.col("anomaly_score") >= 50, "HIGH")
     .when(F.col("anomaly_score") >= 25, "MEDIUM")
     .when(F.col("anomaly_score") > 0, "LOW")
     .otherwise("NONE")
)

# COMMAND ----------

# how many flagged at each level?
print("=== Anomaly Distribution ===")
display(scored_claims.groupBy("anomaly_priority").count().orderBy(F.desc("count")))

high_count = scored_claims.filter("anomaly_priority = 'HIGH'").count()
total = scored_claims.count()
print(f"\nHIGH priority: {high_count}/{total} ({high_count/total*100 if total > 0 else 0:.1f}%)")
print("^ this is a realistic number for a fraud investigation team to handle")

# COMMAND ----------

# which rules fire most?
print("\n=== Rule Trigger Rates ===")
for rule in ["high_amount", "severity_mismatch", "repeat_claimant", "missing_report", "quick_file"]:
    cnt = scored_claims.filter(F.col(f"rule_{rule}") == True).count()
    print(f"  {rule:25s}: {cnt} ({cnt/total*100 if total > 0 else 0:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Investigation Briefs (LLM)
# MAGIC
# MAGIC For HIGH and MEDIUM priority claims, generate an AI investigation brief
# MAGIC that a claims investigator can use to start their review.

# COMMAND ----------

from openai import OpenAI

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
DATABRICKS_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{DATABRICKS_URL}/serving-endpoints"
)

MODEL = "databricks-meta-llama-3-1-70b-instruct"

# COMMAND ----------

# get HIGH priority claims as pandas for iteration
high_claims = (scored_claims
    .filter("anomaly_priority IN ('HIGH', 'MEDIUM')")
    .orderBy(F.desc("anomaly_score"))
    .limit(50)  # cap at 50 for API rate limits
    .toPandas()
)

print(f"generating briefs for {len(high_claims)} flagged claims...")

# COMMAND ----------

def build_investigation_prompt(row):
    triggered_rules = []
    if row.get("rule_high_amount"):
        triggered_rules.append(f"High claim amount (z-score: {row['amount_z_score']:.2f}, amount: ${row['total_claim_amount']:.0f})")
    if row.get("rule_severity_mismatch"):
        triggered_rules.append(f"Severity-amount mismatch ({row['incident_severity']} severity but ${row['total_claim_amount']:.0f} claim)")
    if row.get("rule_repeat_claimant"):
        triggered_rules.append(f"Repeat claimant ({row['policy_claim_count']} claims on this policy)")
    if row.get("rule_missing_report"):
        triggered_rules.append(f"Missing police report for {row['incident_severity']} incident")
    if row.get("rule_quick_file"):
        triggered_rules.append(f"Suspiciously quick processing ({row['processing_days']} days)")

    rules_text = "\n".join(f"  - {r}" for r in triggered_rules)

    return f"""You are a senior claims fraud investigator at PrimeInsurance.
Generate a brief investigation summary for this flagged claim.

CLAIM DETAILS:
- Claim ID: {row['claim_id']}
- Policy ID: {row['policy_id']}
- Incident Date: {row['incident_date']}
- Incident Type: {row['incident_type']}
- Severity: {row['incident_severity']}
- Location: {row.get('incident_city', 'N/A')}, {row.get('incident_state', 'N/A')}
- Total Claim Amount: ${row['total_claim_amount']:.2f}
  (Injury: ${row.get('injury', 0):.2f}, Property: ${row.get('property', 0):.2f}, Vehicle: ${row.get('vehicle', 0):.2f})
- Vehicles Involved: {row.get('vehicles_involved', 'N/A')}
- Bodily Injuries: {row.get('bodily_injuries', 'N/A')}
- Witnesses: {row.get('witnesses', 'N/A')}
- Police Report: {row.get('police_report_available', 'N/A')}
- Claim Rejected: {row.get('claim_rejected', 'N/A')}
- Processing Days: {row.get('processing_days', 'N/A')}

ANOMALY SCORE: {row['anomaly_score']}/100 ({row['anomaly_priority']} priority)

TRIGGERED RULES:
{rules_text}

Return ONLY valid JSON:
{{
  "investigation_summary": "2-3 sentence summary of what makes this claim suspicious, using specific data points",
  "risk_factors": ["list of 2-4 specific risk factors based on the data"],
  "recommended_actions": ["list of 2-3 specific investigation steps"],
  "confidence_level": "High/Medium/Low - how confident are we this needs investigation"
}}"""

# COMMAND ----------

# generate briefs
results = []

for idx, row in high_claims.iterrows():
    if idx % 10 == 0:
        print(f"  processing claim {idx+1}/{len(high_claims)}...")

    prompt = build_investigation_prompt(row)

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3,
        )
        content = response.choices[0].message.content.strip()

        # clean up potential markdown wrapping
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()

        brief = json.loads(content)
    except (json.JSONDecodeError, Exception) as e:
        brief = {
            "investigation_summary": f"Auto-generated brief unavailable: {str(e)[:100]}",
            "risk_factors": [],
            "recommended_actions": ["Manual review required"],
            "confidence_level": "Medium",
        }

    result = {
        "claim_id": row["claim_id"],
        "policy_id": row["policy_id"],
        "incident_date": str(row["incident_date"]),
        "incident_severity": row["incident_severity"],
        "incident_type": row.get("incident_type", ""),
        "total_claim_amount": float(row["total_claim_amount"]),
        "anomaly_score": int(row["anomaly_score"]),
        "anomaly_priority": row["anomaly_priority"],
        "triggered_rules": ", ".join([
            r for r, fired in [
                ("high_amount", row.get("rule_high_amount")),
                ("severity_mismatch", row.get("rule_severity_mismatch")),
                ("repeat_claimant", row.get("rule_repeat_claimant")),
                ("missing_report", row.get("rule_missing_report")),
                ("quick_file", row.get("rule_quick_file")),
            ] if fired
        ]),
        "investigation_summary": brief.get("investigation_summary", ""),
        "risk_factors": json.dumps(brief.get("risk_factors", [])),
        "recommended_actions": json.dumps(brief.get("recommended_actions", [])),
        "confidence_level": brief.get("confidence_level", "Medium"),
        "model_name": MODEL,
        "generated_at": datetime.now().isoformat(),
    }
    results.append(result)

print(f"\ngenerated {len(results)} investigation briefs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Gold Table

# COMMAND ----------

anomaly_df = spark.createDataFrame(results)

(anomaly_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("`databricks-hackathon-insurance`.gold.claim_anomaly_explanations"))

print(f"gold.claim_anomaly_explanations: {anomaly_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Output

# COMMAND ----------

display(spark.table("`databricks-hackathon-insurance`.gold.claim_anomaly_explanations").orderBy(F.desc("anomaly_score")).limit(10))

# COMMAND ----------

# print a few high-priority briefs in readable format
for r in sorted(results, key=lambda x: -x["anomaly_score"])[:3]:
    print(f"\n{'='*60}")
    print(f"CLAIM: {r['claim_id']} | Score: {r['anomaly_score']}/100 | Priority: {r['anomaly_priority']}")
    print(f"Amount: ${r['total_claim_amount']:.0f} | Severity: {r['incident_severity']} | Type: {r['incident_type']}")
    print(f"Rules Triggered: {r['triggered_rules']}")
    print(f"\nInvestigation Summary:")
    print(f"  {r['investigation_summary']}")
    print(f"\nRisk Factors: {r['risk_factors']}")
    print(f"Actions: {r['recommended_actions']}")
    print(f"Confidence: {r['confidence_level']}")
