# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality — Source Owner Weekly Digest
# MAGIC
# MAGIC **Problem**: data engineering owns the rules, but source owners own the *root
# MAGIC cause* of bad data. Without a feedback loop, source owners never see what they
# MAGIC produced — they don't know about the swapped columns in `customers_6.csv`, the
# MAGIC blank padding rows in the sales export, or the corrupted claim dates. The
# MAGIC quality issues are caught but the people who could fix them upstream never hear
# MAGIC about it.
# MAGIC
# MAGIC **Solution**: a weekly digest, computed per source owner, that summarizes:
# MAGIC - total records quarantined from their files this week
# MAGIC - top 5 rejection reasons
# MAGIC - per-file breakdown
# MAGIC - week-over-week change
# MAGIC
# MAGIC The digest message is composed in plain markdown and stored in
# MAGIC `silver.dq_source_owner_digests`. From there it can be:
# MAGIC - **Read by source owners via Genie** ("show me my latest DQ digest")
# MAGIC - **Posted to Slack** if a webhook URL is configured
# MAGIC - **Surfaced on a dashboard tile** for the data engineering on-call
# MAGIC
# MAGIC **Why Delta-table-first delivery**: this workspace doesn't have SMTP. Even if it
# MAGIC did, an email goes to one place and gets lost. A Delta table is auditable,
# MAGIC queryable, can be Genie-fronted, and is reproducible weeks later. Slack is
# MAGIC offered as an optional secondary channel.
# MAGIC
# MAGIC **Source-to-owner mapping**: the source files don't carry owner metadata.
# MAGIC We maintain a small `silver.dq_source_owners` lookup table, seeded with the
# MAGIC known files from architecture.md. Editable by hand later.
# MAGIC
# MAGIC **Inputs**:
# MAGIC - `silver.quarantine_*` (read-only)
# MAGIC - `silver.dq_source_owners` (created and seeded by this notebook)
# MAGIC
# MAGIC **Outputs**:
# MAGIC - `silver.dq_source_owners` (created if missing, seeded once)
# MAGIC - `silver.dq_source_owner_digests` (append-only with idempotent upsert per week)
# MAGIC
# MAGIC **Idempotency**: re-running for the same week deletes existing rows for that
# MAGIC `(week_starting, owner_email)` then re-inserts. Safe to schedule.
# MAGIC
# MAGIC **Zero impact on Phase 1**: read-only on quarantine tables. Writes only to two
# MAGIC new tables in the silver schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 1 — CONFIGURATION ==========

# COMMAND ----------

dbutils.widgets.text("catalog", "primeins", "Unity Catalog")
dbutils.widgets.text("week_starting", "", "Week start date YYYY-MM-DD (blank = current week Monday)")
dbutils.widgets.text("slack_webhook", "", "Optional Slack webhook URL for secondary delivery")
dbutils.widgets.dropdown("seed_owners", "auto", ["auto", "force", "skip"],
                          "Seed dq_source_owners table? auto=only if empty")

CATALOG       = dbutils.widgets.get("catalog")
WEEK_STARTING = dbutils.widgets.get("week_starting").strip()
SLACK_WEBHOOK = dbutils.widgets.get("slack_webhook").strip() or None
SEED_OWNERS   = dbutils.widgets.get("seed_owners")

OWNERS_TABLE   = f"{CATALOG}.silver.dq_source_owners"
DIGESTS_TABLE  = f"{CATALOG}.silver.dq_source_owner_digests"

QUARANTINE_TABLES = [
    "quarantine_customers",
    "quarantine_claims",
    "quarantine_policy",
    "quarantine_sales",
    "quarantine_cars",
]

from datetime import datetime, timedelta, date

# Compute the Monday of the requested week (or current week if blank)
if WEEK_STARTING:
    week_start_date = datetime.strptime(WEEK_STARTING, "%Y-%m-%d").date()
else:
    today = date.today()
    week_start_date = today - timedelta(days=today.weekday())  # Monday
week_end_date = week_start_date + timedelta(days=7)

print(f"Catalog:           {CATALOG}")
print(f"Week starting:     {week_start_date}")
print(f"Week ending:       {week_end_date} (exclusive)")
print(f"Owners table:      {OWNERS_TABLE}")
print(f"Digests table:     {DIGESTS_TABLE}")
print(f"Slack webhook:     {'configured' if SLACK_WEBHOOK else 'not configured'}")
print(f"Seed owners:       {SEED_OWNERS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 2 — TABLE DDL ==========

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {OWNERS_TABLE} (
    source_file_pattern  STRING NOT NULL  COMMENT 'substring matched against _source_file (e.g. customers_6)',
    owner_email          STRING NOT NULL,
    owner_name           STRING,
    owner_team           STRING,
    notes                STRING
)
USING DELTA
COMMENT 'Maps source file patterns to source-system owners. Edit by hand to add new mappings.'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIGESTS_TABLE} (
    week_starting        DATE NOT NULL,
    owner_email          STRING NOT NULL,
    owner_name           STRING,
    owner_team           STRING,
    total_quarantined    BIGINT,
    files_affected       INT,
    rules_triggered      INT,
    top_rejection_reason STRING,
    wow_change           BIGINT       COMMENT 'this week minus last week (NULL if no prior data)',
    digest_markdown      STRING       COMMENT 'pre-formatted markdown body for the digest',
    composed_at          TIMESTAMP NOT NULL,
    delivered_to_slack   BOOLEAN
)
USING DELTA
COMMENT 'Per-owner weekly DQ digest. Idempotent upsert by (week_starting, owner_email).'
""")

print(f"Created/verified: {OWNERS_TABLE}")
print(f"Created/verified: {DIGESTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 3 — SEED OWNER MAPPINGS ==========
# MAGIC
# MAGIC Initial seed mapping reflects the source structure documented in architecture.md.
# MAGIC In real life this table would be edited by data engineering as the source
# MAGIC inventory grows. The pattern is matched as a substring against `_source_file`,
# MAGIC so `customers_6` matches `customers_6.csv`.

# COMMAND ----------

current_owners_count = spark.table(OWNERS_TABLE).count()
print(f"Current rows in {OWNERS_TABLE}: {current_owners_count}")

should_seed = (
    SEED_OWNERS == "force" or
    (SEED_OWNERS == "auto" and current_owners_count == 0)
)

if should_seed:
    if SEED_OWNERS == "force":
        spark.sql(f"TRUNCATE TABLE {OWNERS_TABLE}")
        print("Force seeding — table truncated.")

    seed = [
        # Customers — 7 files, 6 different owners across regional Insurance folders
        ("customers_1", "insurance1-data@example.com", "Insurance 1 Data Team", "Insurance 1",
         "Region: see Insurance 1 folder"),
        ("customers_2", "insurance2-data@example.com", "Insurance 2 Data Team", "Insurance 2",
         "Missing HHInsurance column"),
        ("customers_3", "insurance3-data@example.com", "Insurance 3 Data Team", "Insurance 3", None),
        ("customers_4", "insurance4-data@example.com", "Insurance 4 Data Team", "Insurance 4",
         "Missing Education column"),
        ("customers_5", "insurance5-data@example.com", "Insurance 5 Data Team", "Insurance 5",
         "Region abbreviations W/C/E/S; 'terto' typo"),
        ("customers_6", "insurance6-data@example.com", "Insurance 6 Data Team", "Insurance 6",
         "Marital_status and Education columns swapped at source"),
        ("customers_7", "core-data@example.com", "Core Customer Team", "HQ",
         "Root-level customers_7.csv"),

        # Claims — 2 JSON files
        ("claims_1", "claims-data@example.com", "Claims Data Team", "Claims",
         "Date fields corrupted at source — '27:00.0' format"),
        ("claims_2", "claims-data@example.com", "Claims Data Team", "Claims",
         "Same as claims_1; root-level"),

        # Policy
        ("policy",   "policy-admin@example.com", "Policy Admin Team", "Underwriting",
         "1 row with negative umbrella_limit"),

        # Sales — 3 files
        ("sales_1",  "sales-ops@example.com", "Sales Ops Team", "Sales", None),
        ("Sales_2",  "sales-ops@example.com", "Sales Ops Team", "Sales",
         "62.9% empty padding rows from export script"),
        ("sales_4",  "sales-ops@example.com", "Sales Ops Team", "Sales", None),

        # Cars
        ("cars",     "fleet-data@example.com", "Fleet Data Team", "Inventory",
         "Unit strings embedded in mileage/engine/max_power"),
    ]

    seed_df = spark.createDataFrame(
        seed,
        "source_file_pattern STRING, owner_email STRING, owner_name STRING, "
        "owner_team STRING, notes STRING"
    )
    seed_df.write.format("delta").mode("append").saveAsTable(OWNERS_TABLE)
    print(f"Seeded {len(seed)} owner mappings into {OWNERS_TABLE}")
else:
    print(f"Skipping seed (SEED_OWNERS={SEED_OWNERS}, current rows={current_owners_count})")

print("\nCurrent owner mappings:")
spark.table(OWNERS_TABLE).orderBy("source_file_pattern").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 4 — BUILD WEEKLY QUARANTINE DATAFRAME ==========
# MAGIC
# MAGIC Union the quarantine tables, narrow to (source_file, rejection_reason, load_timestamp),
# MAGIC filter to the target week. We DON'T use the primary key columns here — every
# MAGIC quarantine table has a different PK shape and we only need counts and reasons.

# COMMAND ----------

from pyspark.sql import functions as F

week_dfs = []
for q_table in QUARANTINE_TABLES:
    fqn = f"{CATALOG}.silver.{q_table}"
    try:
        df = (
            spark.table(fqn)
                 .filter(
                     (F.col("_load_timestamp") >= F.lit(week_start_date)) &
                     (F.col("_load_timestamp") <  F.lit(week_end_date))
                 )
                 .select(
                     F.lit(q_table).alias("source_table"),
                     F.col("_source_file").alias("source_file"),
                     F.col("_rejection_reason").alias("rejection_reason"),
                     F.col("_load_timestamp").alias("load_timestamp"),
                 )
        )
        week_dfs.append(df)
    except Exception as e:
        print(f"  (skipping {fqn}: {e.__class__.__name__})")

if not week_dfs:
    print("No quarantine tables readable — nothing to digest.")
    week_quarantine = None
else:
    week_quarantine = week_dfs[0]
    for d in week_dfs[1:]:
        week_quarantine = week_quarantine.unionByName(d)

    n = week_quarantine.count()
    print(f"Quarantined records in week {week_start_date}: {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 5 — JOIN TO OWNERS, GROUP, COMPOSE DIGESTS ==========
# MAGIC
# MAGIC For each owner, build a markdown digest body covering totals, top reasons, and
# MAGIC per-file breakdown. The body is stored alongside the structured fields so a
# MAGIC dashboard or Genie answer can render it directly.

# COMMAND ----------

import json

digest_records = []

if week_quarantine is not None:
    owners = spark.table(OWNERS_TABLE).collect()

    # We do per-owner aggregation in Python rather than a giant SQL pivot —
    # the data is small enough and the markdown composition is easier in Python.

    # Pre-collect: source_file, rejection_reason, count
    grouped = (
        week_quarantine
        .groupBy("source_file", "rejection_reason", "source_table")
        .agg(F.count("*").cast("long").alias("record_count"))
        .collect()
    )

    # Index by source_file for fast owner matching
    for owner_row in owners:
        owner_email   = owner_row["owner_email"]
        owner_name    = owner_row["owner_name"]
        owner_team    = owner_row["owner_team"]
        pattern       = owner_row["source_file_pattern"]

        # Match: any grouped row whose source_file contains this owner's pattern
        owner_rows = [
            r for r in grouped
            if r["source_file"] is not None and pattern in r["source_file"]
        ]
        if not owner_rows:
            continue

        total = sum(r["record_count"] for r in owner_rows)
        files = sorted({r["source_file"] for r in owner_rows})
        rules = sorted({r["rejection_reason"] for r in owner_rows if r["rejection_reason"]})
        top_reason = max(owner_rows, key=lambda r: r["record_count"])["rejection_reason"] or "(unknown)"

        # Per-file breakdown
        file_lines = []
        for f in files:
            file_total = sum(r["record_count"] for r in owner_rows if r["source_file"] == f)
            file_lines.append(f"  - `{f}` — {file_total} records")

        # Top 5 reasons
        from collections import defaultdict
        reason_totals = defaultdict(int)
        for r in owner_rows:
            reason_totals[r["rejection_reason"] or "(unknown)"] += r["record_count"]
        top5 = sorted(reason_totals.items(), key=lambda kv: kv[1], reverse=True)[:5]
        top5_lines = [f"  {i+1}. `{reason}` — {count} records" for i, (reason, count) in enumerate(top5)]

        # Compose markdown
        body = (
            f"## DQ digest for {owner_name or owner_email}\n"
            f"### Week of {week_start_date}\n\n"
            f"**Total quarantined records this week: {total}**\n\n"
            f"Source files affected ({len(files)}):\n" + "\n".join(file_lines) + "\n\n"
            f"Top {len(top5)} rejection reasons:\n" + "\n".join(top5_lines) + "\n\n"
            f"---\n"
            f"What you can do:\n"
            f"- **Source fix** (path 1): correct the data at origin so next ingestion is clean.\n"
            f"- **Rule too strict** (path 2): if these records are actually valid, ping data engineering "
            f"and we'll loosen the rule and backfill.\n"
            f"- **Manual repair** (path 3): if specific records can be salvaged with steward judgment, "
            f"flag them and a steward will work the queue.\n\n"
            f"Questions? Reply to data-engineering@example.com or ask Genie: "
            f"\"show me the DQ digest for {owner_email} this week\".\n"
        )

        digest_records.append({
            "week_starting":        week_start_date,
            "owner_email":          owner_email,
            "owner_name":           owner_name,
            "owner_team":           owner_team,
            "total_quarantined":    int(total),
            "files_affected":       int(len(files)),
            "rules_triggered":      int(len(rules)),
            "top_rejection_reason": top_reason,
            "wow_change":           None,    # filled in next cell
            "digest_markdown":      body,
        })

print(f"\nComposed {len(digest_records)} owner digests for week {week_start_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 6 — COMPUTE WEEK-OVER-WEEK CHANGE ==========
# MAGIC
# MAGIC For each owner, look up their `total_quarantined` from the previous week's digest
# MAGIC (if any) and compute the delta. Read from the existing digests table.

# COMMAND ----------

prior_week_start = week_start_date - timedelta(days=7)

prior_totals = {}
try:
    rows = (
        spark.table(DIGESTS_TABLE)
             .filter(F.col("week_starting") == F.lit(prior_week_start))
             .select("owner_email", "total_quarantined")
             .collect()
    )
    prior_totals = {r["owner_email"]: r["total_quarantined"] for r in rows}
    print(f"Prior week ({prior_week_start}) had digests for {len(prior_totals)} owners")
except Exception:
    print(f"No prior week data yet — wow_change will be NULL for all owners")

for d in digest_records:
    prior = prior_totals.get(d["owner_email"])
    if prior is not None:
        d["wow_change"] = int(d["total_quarantined"] - prior)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 7 — IDEMPOTENT UPSERT INTO DIGESTS TABLE ==========

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, TimestampType,
    DateType, BooleanType,
)

if digest_records:
    spark.sql(f"""
        DELETE FROM {DIGESTS_TABLE}
        WHERE week_starting = DATE('{week_start_date}')
    """)

    composed_at = datetime.utcnow()

    digest_schema = StructType([
        StructField("week_starting",        DateType(),      False),
        StructField("owner_email",          StringType(),    False),
        StructField("owner_name",           StringType(),    True),
        StructField("owner_team",           StringType(),    True),
        StructField("total_quarantined",    LongType(),      True),
        StructField("files_affected",       IntegerType(),   True),
        StructField("rules_triggered",      IntegerType(),   True),
        StructField("top_rejection_reason", StringType(),    True),
        StructField("wow_change",           LongType(),      True),
        StructField("digest_markdown",      StringType(),    True),
        StructField("composed_at",          TimestampType(), False),
        StructField("delivered_to_slack",   BooleanType(),   True),
    ])

    digest_tuples = [(
        d["week_starting"],
        d["owner_email"],
        d["owner_name"],
        d["owner_team"],
        d["total_quarantined"],
        d["files_affected"],
        d["rules_triggered"],
        d["top_rejection_reason"],
        d["wow_change"],
        d["digest_markdown"],
        composed_at,
        False,    # delivered_to_slack — set to True in cell 8 if Slack succeeds
    ) for d in digest_records]

    spark.createDataFrame(digest_tuples, digest_schema) \
         .write.format("delta").mode("append").saveAsTable(DIGESTS_TABLE)

    print(f"Upserted {len(digest_records)} digests for week {week_start_date} into {DIGESTS_TABLE}")
else:
    print("No digests to write.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 8 — OPTIONAL SLACK DELIVERY ==========
# MAGIC
# MAGIC If `slack_webhook` was configured, post each digest to Slack and mark
# MAGIC `delivered_to_slack = TRUE`. Failures are logged and don't fail the notebook —
# MAGIC the Delta digest is the source of truth, Slack is best-effort.

# COMMAND ----------

if SLACK_WEBHOOK and digest_records:
    import urllib.request
    import urllib.error

    delivered_owners = []
    for d in digest_records:
        payload = {
            "text": f"*DQ digest for {d['owner_name'] or d['owner_email']}* "
                    f"— week of {d['week_starting']}",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": d["digest_markdown"]}}
            ]
        }
        try:
            req = urllib.request.Request(
                SLACK_WEBHOOK,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            delivered_owners.append(d["owner_email"])
        except Exception as e:
            print(f"  Slack delivery failed for {d['owner_email']}: {e}")

    if delivered_owners:
        owners_sql = ", ".join(f"'{e}'" for e in delivered_owners)
        spark.sql(f"""
            UPDATE {DIGESTS_TABLE}
            SET delivered_to_slack = TRUE
            WHERE week_starting = DATE('{week_start_date}')
              AND owner_email IN ({owners_sql})
        """)
        print(f"Slack delivery: {len(delivered_owners)} digests delivered.")
elif SLACK_WEBHOOK:
    print("Slack webhook configured but no digests to deliver.")
else:
    print("Slack delivery not configured — Delta table is the only delivery channel.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ========== CELL 9 — RUN SUMMARY ==========

# COMMAND ----------

print("=== Source-owner digest run summary ===\n")
print(f"Week starting:    {week_start_date}")
print(f"Owners notified:  {len(digest_records)}")
print(f"Slack delivered:  {SLACK_WEBHOOK is not None}")
print()

print(f"Latest digests in {DIGESTS_TABLE}:")
spark.sql(f"""
    SELECT week_starting, owner_email, owner_team, total_quarantined,
           files_affected, rules_triggered, wow_change, delivered_to_slack
    FROM {DIGESTS_TABLE}
    WHERE week_starting = DATE('{week_start_date}')
    ORDER BY total_quarantined DESC
""").show(truncate=False)

print("\nSample digest body (first owner):")
sample = (
    spark.table(DIGESTS_TABLE)
         .filter(F.col("week_starting") == F.lit(week_start_date))
         .select("digest_markdown")
         .limit(1)
         .collect()
)
if sample:
    print(sample[0]["digest_markdown"])
else:
    print("(no digest composed for this week)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use
# MAGIC
# MAGIC ### First run
# MAGIC ```
# MAGIC catalog        = primeins
# MAGIC week_starting  = (blank — defaults to current week's Monday)
# MAGIC slack_webhook  = (blank — Delta-table-only delivery)
# MAGIC seed_owners    = auto
# MAGIC ```
# MAGIC The first run seeds the owner mapping, computes digests, writes to the digests
# MAGIC table. Inspect the rows and the sample body in cell 9.
# MAGIC
# MAGIC ### Backfilling a specific week
# MAGIC ```
# MAGIC week_starting = 2026-03-30
# MAGIC ```
# MAGIC Re-running for the same week deletes existing rows and recomputes. Idempotent.
# MAGIC
# MAGIC ### Editing the owner mapping
# MAGIC As the source inventory grows, edit the mapping table by hand:
# MAGIC ```sql
# MAGIC INSERT INTO primeins.silver.dq_source_owners VALUES
# MAGIC   ('new_source', 'owner@example.com', 'New Source Team', 'New Region', 'notes');
# MAGIC ```
# MAGIC Then re-run the digest job — the new owner will start getting their digest.
# MAGIC
# MAGIC ### Slack delivery (optional)
# MAGIC Set `slack_webhook` to a Slack incoming webhook URL. Each digest gets posted as
# MAGIC a separate message. Failures don't fail the notebook.
# MAGIC
# MAGIC ### Genie integration
# MAGIC Add `silver.dq_source_owner_digests` to the compliance Genie space. Sample
# MAGIC questions to seed:
# MAGIC - "Show me the DQ digest for customers_6 owner this week"
# MAGIC - "Which source owner had the biggest quarantine spike this week?"
# MAGIC - "What's the trend for the sales team's quarantine count?"
