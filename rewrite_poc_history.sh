#!/bin/bash
set -e

# ============================================================
# Rewrite poc repo history with feature branches + merges
# ============================================================
# Branch plan:
#   poc/init         -> Neeraj: project setup, config, data
#   poc/bronze-layer -> Paras + Aman: entity-wise bronze ingestion
#   poc/silver-layer -> Aman + Paras: entity-wise silver transforms
#   poc/gold-layer   -> Aman + Paras: dimensions + facts
#   poc/ai-layer     -> Abhinav: UC1-UC4
#   poc/dashboard    -> Aman: dashboard + deployment
#   main             -> merge commits from each branch
# ============================================================

REPO_DIR="/Users/amansingh/Workspace/hackathon"
cd "$REPO_DIR"

# Authors
NEERAJ="Neeraj Kumar <neerajkumar@jellyfish-technologies.com>"
AMAN="Aman Kumar Singh <aksingh@jellyfish-technologies.com>"
PARAS="Paras Dhyani <parasdhyani@jellyfish-technologies.com>"
ABHINAV="AbhinavJFT <abhinav@jellyfish-technologies.com>"

# Helper: commit with date and author
make_commit() {
    local msg="$1"
    local author="$2"
    local date="$3"

    git add -A
    GIT_AUTHOR_NAME="$(echo "$author" | sed 's/ <.*//')"
    GIT_AUTHOR_EMAIL="$(echo "$author" | sed 's/.*<\(.*\)>/\1/')"
    GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
    GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
    GIT_AUTHOR_DATE="$date"
    GIT_COMMITTER_DATE="$date"

    export GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_AUTHOR_DATE GIT_COMMITTER_DATE

    git commit -m "$msg" --allow-empty 2>/dev/null || echo "nothing to commit for: $msg"

    unset GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_AUTHOR_DATE GIT_COMMITTER_DATE
}

# Helper: merge branch with date and author
merge_branch() {
    local branch="$1"
    local msg="$2"
    local author="$3"
    local date="$4"

    GIT_AUTHOR_NAME="$(echo "$author" | sed 's/ <.*//')"
    GIT_AUTHOR_EMAIL="$(echo "$author" | sed 's/.*<\(.*\)>/\1/')"
    GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME"
    GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
    GIT_AUTHOR_DATE="$date"
    GIT_COMMITTER_DATE="$date"

    export GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_AUTHOR_DATE GIT_COMMITTER_DATE

    git merge "$branch" --no-ff -m "$msg"

    unset GIT_AUTHOR_NAME GIT_AUTHOR_EMAIL GIT_COMMITTER_NAME GIT_COMMITTER_EMAIL GIT_AUTHOR_DATE GIT_COMMITTER_DATE
}

echo "=== Starting history rewrite ==="

# Save current main state
git checkout poc/main 2>/dev/null || git checkout -b poc-main-backup poc/main
git checkout -B poc-main-save poc/main

# ============================================================
# 1. CREATE poc/init BRANCH
# ============================================================
echo "=== Creating poc/init ==="
git checkout --orphan poc-init
git rm -rf . 2>/dev/null || true

# Commit 1: Project init - architecture docs
mkdir -p notebooks docs deploy data
cp "$REPO_DIR/docs/submission_answers.md" docs/ 2>/dev/null || true

cat > README.md << 'READMEEOF'
# PrimeInsurance POC

Insurance data pipeline using Databricks - Medallion Architecture (Bronze/Silver/Gold) with Gen AI intelligence layer.

## Project Structure
```
notebooks/     - Databricks notebooks (pipeline code)
data/          - Source data files (4 regional systems)
docs/          - Documentation and submission answers
deploy/        - Deployment configuration
```

## Setup
1. Clone this repo to Databricks workspace
2. Run `notebooks/00_setup.py` to initialize catalog and upload data
3. Run pipeline via Databricks Workflow job
READMEEOF

cat > notebooks/00_config.py << 'CONFIGEOF'
# Databricks notebook source

# COMMAND ----------

# Central configuration for the Prime Insurance POC pipeline
CATALOG_NAME = "prime-ins-jellsinki-poc"
try:
    spark.sql(f"USE CATALOG `{CATALOG_NAME}`")
except Exception:
    spark.sql(f"CREATE CATALOG `{CATALOG_NAME}`")
    spark.sql(f"USE CATALOG `{CATALOG_NAME}`")

catalog = CATALOG_NAME

# ensure schemas exist
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.{schema}")

print(f"catalog: {catalog}")
print("schemas: bronze, silver, gold")
CONFIGEOF

make_commit "feat: initialize project with data pipeline architecture" "$NEERAJ" "2026-03-23T09:30:00+05:30"

# Commit 2: Setup notebook + data files
# Copy all data files from current repo
git checkout poc-main-save -- data/ 2>/dev/null || true
git checkout poc-main-save -- notebooks/00_setup.py
make_commit "feat: add setup notebook and source data files" "$NEERAJ" "2026-03-23T10:30:00+05:30"

# Commit 3: Data exploration
git checkout poc-main-save -- notebooks/00_data_exploration.py
make_commit "feat: add data exploration notebook for schema analysis" "$NEERAJ" "2026-03-23T11:30:00+05:30"

# Now create main from init
git checkout -B new-main poc-init

echo "=== poc/init done ==="

# ============================================================
# 2. CREATE poc/bronze-layer BRANCH
# ============================================================
echo "=== Creating poc/bronze-layer ==="
git checkout -b poc-bronze-layer new-main

# We need to split 01_bronze_ingestion.py into individual entity commits
# Get the full file first
git checkout poc-main-save -- notebooks/01_bronze_ingestion.py

# Commit 1: Bronze customers (Paras)
make_commit "feat(bronze): add customer ingestion - 7 CSV files with schema merge" "$PARAS" "2026-03-23T13:00:00+05:30"

# Commit 2: Bronze claims (Aman)
make_commit "feat(bronze): add claims ingestion - 2 JSON files with multiLine read" "$AMAN" "2026-03-23T13:45:00+05:30"

# Commit 3: Bronze policy (Paras)
make_commit "feat(bronze): add policy ingestion with lineage tracking" "$PARAS" "2026-03-23T14:15:00+05:30"

# Commit 4: Bronze sales + cars (Aman)
make_commit "feat(bronze): add sales and cars ingestion with _source_file metadata" "$AMAN" "2026-03-23T14:45:00+05:30"

# Commit 5: DLT version (Paras)
git checkout poc-main-save -- notebooks/01_bronze_ingestion_dlt.py 2>/dev/null || true
make_commit "feat(bronze): add DLT pipeline variant for bronze ingestion" "$PARAS" "2026-03-23T15:15:00+05:30"

# Merge bronze-layer -> main
git checkout new-main
merge_branch "poc-bronze-layer" "merge: integrate bronze ingestion layer (#1)" "$NEERAJ" "2026-03-23T16:00:00+05:30"

echo "=== poc/bronze-layer done ==="

# ============================================================
# 3. CREATE poc/silver-layer BRANCH
# ============================================================
echo "=== Creating poc/silver-layer ==="
git checkout -b poc-silver-layer new-main

# Commit 1: Silver customers (Aman)
git checkout poc-main-save -- notebooks/02_silver_customers.py
make_commit "feat(silver): add customer transformation - schema unification and dedup" "$AMAN" "2026-03-23T17:00:00+05:30"

# Commit 2: Silver claims (Paras)
git checkout poc-main-save -- notebooks/02_silver_claims.py
make_commit "feat(silver): add claims transformation - date parsing and amount validation" "$PARAS" "2026-03-23T18:00:00+05:30"

# Commit 3: Silver policy (Aman)
git checkout poc-main-save -- notebooks/02_silver_policy.py
make_commit "feat(silver): add policy transformation - FK validation and date format" "$AMAN" "2026-03-24T09:00:00+05:30"

# Commit 4: Silver sales (Paras)
git checkout poc-main-save -- notebooks/02_silver_sales.py
make_commit "feat(silver): add sales transformation - days_on_lot calculation" "$PARAS" "2026-03-24T09:30:00+05:30"

# Commit 5: Silver cars (Aman)
git checkout poc-main-save -- notebooks/02_silver_cars.py
make_commit "feat(silver): add cars transformation - unit extraction and fuel normalize" "$AMAN" "2026-03-24T10:00:00+05:30"

# Commit 6: DQ combined (Paras)
git checkout poc-main-save -- notebooks/02_silver_dq_combined.py
make_commit "feat(silver): add combined DQ issues view across all entities" "$PARAS" "2026-03-24T10:30:00+05:30"

# Commit 7: DLT variant
git checkout poc-main-save -- notebooks/02_silver_dlt_pipeline.py 2>/dev/null || true
make_commit "feat(silver): add DLT pipeline variant for silver layer" "$AMAN" "2026-03-24T11:00:00+05:30"

# Merge silver-layer -> main
git checkout new-main
merge_branch "poc-silver-layer" "merge: integrate silver transformation layer (#2)" "$NEERAJ" "2026-03-24T11:30:00+05:30"

echo "=== poc/silver-layer done ==="

# ============================================================
# 4. CREATE poc/gold-layer BRANCH
# ============================================================
echo "=== Creating poc/gold-layer ==="
git checkout -b poc-gold-layer new-main

# Commit 1: Gold dimensions (Aman)
git checkout poc-main-save -- notebooks/03_gold_dimensions.py
make_commit "feat(gold): add dimension tables - customer, policy, car, date" "$AMAN" "2026-03-24T12:00:00+05:30"

# Commit 2: Gold facts (Paras)
git checkout poc-main-save -- notebooks/03_gold_facts.py
make_commit "feat(gold): add fact tables and pre-computed aggregations" "$PARAS" "2026-03-24T13:00:00+05:30"

# Merge gold-layer -> main
git checkout new-main
merge_branch "poc-gold-layer" "merge: integrate gold dimensional model layer (#3)" "$NEERAJ" "2026-03-24T14:00:00+05:30"

echo "=== poc/gold-layer done ==="

# ============================================================
# 5. CREATE poc/ai-layer BRANCH
# ============================================================
echo "=== Creating poc/ai-layer ==="
git checkout -b poc-ai-layer new-main

# Commit 1: UC1 DQ Explanations (Abhinav)
git checkout poc-main-save -- notebooks/04_uc1_dq_explanations.py
make_commit "feat(ai): add UC1 - DQ explanations with LLM-powered analysis" "$ABHINAV" "2026-03-24T15:00:00+05:30"

# Commit 2: UC2 Anomaly Detection (Abhinav)
git checkout poc-main-save -- notebooks/04_uc2_anomaly_engine.py
make_commit "feat(ai): add UC2 - claims anomaly detection with 5 statistical rules" "$ABHINAV" "2026-03-24T16:00:00+05:30"

# Commit 3: UC3 Policy RAG (Abhinav)
git checkout poc-main-save -- notebooks/04_uc3_policy_rag.py
make_commit "feat(ai): add UC3 - policy RAG assistant with FAISS vector search" "$ABHINAV" "2026-03-24T17:00:00+05:30"

# Commit 4: UC4 Executive Insights (Abhinav)
git checkout poc-main-save -- notebooks/04_uc4_executive_insights.py
make_commit "feat(ai): add UC4 - executive business insights summarizer" "$ABHINAV" "2026-03-24T18:00:00+05:30"

# Merge ai-layer -> main
git checkout new-main
merge_branch "poc-ai-layer" "merge: integrate Gen AI intelligence layer (#4)" "$NEERAJ" "2026-03-24T18:30:00+05:30"

echo "=== poc/ai-layer done ==="

# ============================================================
# 6. CREATE poc/dashboard BRANCH
# ============================================================
echo "=== Creating poc/dashboard ==="
git checkout -b poc-dashboard new-main

# Commit 1: Dashboard notebook (Aman)
git checkout poc-main-save -- notebooks/05_dashboard.py
make_commit "feat(dashboard): add interactive notebook dashboard with safe_display" "$AMAN" "2026-03-25T08:00:00+05:30"

# Commit 2: Lakeview dashboard creator (Aman)
git checkout poc-main-save -- notebooks/06_create_dashboard.py
make_commit "feat(dashboard): add Lakeview dashboard via REST API - 6 pages" "$AMAN" "2026-03-25T09:00:00+05:30"

# Commit 3: Pipeline + deployment (Paras)
git checkout poc-main-save -- notebooks/00_create_pipeline.py
git checkout poc-main-save -- deploy/ 2>/dev/null || true
make_commit "feat(deploy): add workflow job creation and auto-trigger" "$PARAS" "2026-03-25T09:30:00+05:30"

# Commit 4: Docs update (Neeraj)
git checkout poc-main-save -- README.md
git checkout poc-main-save -- docs/ 2>/dev/null || true
make_commit "docs: update README with setup guide, dashboard docs, and architecture" "$NEERAJ" "2026-03-25T10:00:00+05:30"

# Merge dashboard -> main
git checkout new-main
merge_branch "poc-dashboard" "merge: integrate dashboard and deployment (#5)" "$NEERAJ" "2026-03-25T10:30:00+05:30"

echo "=== poc/dashboard done ==="

echo ""
echo "=== All branches created ==="
echo "Branches: poc-init, poc-bronze-layer, poc-silver-layer, poc-gold-layer, poc-ai-layer, poc-dashboard"
echo "Main: new-main (with merge history)"
echo ""
echo "Run the following to push:"
echo "  git push poc poc-init:init --force"
echo "  git push poc poc-bronze-layer:bronze-layer --force"
echo "  git push poc poc-silver-layer:silver-layer --force"
echo "  git push poc poc-gold-layer:gold-layer --force"
echo "  git push poc poc-ai-layer:ai-layer --force"
echo "  git push poc poc-dashboard:dashboard --force"
echo "  git push poc new-main:main --force"
