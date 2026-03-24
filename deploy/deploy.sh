#!/bin/bash
#
# Deploy PrimeInsurance Data Platform to Databricks
#
# Prerequisites:
#   - Databricks CLI installed and authenticated
#     pip install databricks-cli
#     databricks auth login --host https://YOUR_WORKSPACE.cloud.databricks.com
#
# Usage:
#   ./deploy/deploy.sh                          # uses default profile
#   ./deploy/deploy.sh -p my-profile            # uses named profile
#   ./deploy/deploy.sh --workspace-path /Shared/primeins  # custom workspace path

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PROFILE_FLAG=""
WORKSPACE_PATH=""

# parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--profile) PROFILE_FLAG="-p $2"; shift 2 ;;
    --workspace-path) WORKSPACE_PATH="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# detect workspace user if path not provided
if [ -z "$WORKSPACE_PATH" ]; then
  USER_EMAIL=$(databricks current-user me $PROFILE_FLAG --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['userName'])" 2>/dev/null || echo "")
  if [ -z "$USER_EMAIL" ]; then
    echo "Could not detect user. Provide --workspace-path explicitly."
    exit 1
  fi
  WORKSPACE_PATH="/Users/$USER_EMAIL/primeinsurance-hackathon"
fi

echo "=== PrimeInsurance Data Platform Deploy ==="
echo "Workspace path: $WORKSPACE_PATH"
echo ""

# step 1: create workspace folder
echo "[1/4] Creating workspace folder..."
databricks workspace mkdirs "$WORKSPACE_PATH" $PROFILE_FLAG 2>/dev/null || true

# step 2: import all notebooks
echo "[2/4] Importing notebooks..."
for f in "$PROJECT_DIR"/notebooks/*.py; do
  filename=$(basename "$f" .py)
  databricks workspace import \
    "$WORKSPACE_PATH/$filename" \
    --file "$f" \
    --format SOURCE \
    --language PYTHON \
    --overwrite $PROFILE_FLAG 2>/dev/null
  echo "  imported: $filename"
done

# step 3: create job from config
echo "[3/4] Creating pipeline job..."
# substitute workspace path in config
CONFIG=$(cat "$SCRIPT_DIR/job_config.json" | sed "s|{{WORKSPACE_PATH}}|$WORKSPACE_PATH|g")

# check if job already exists
EXISTING_JOB=$(databricks jobs list $PROFILE_FLAG --output json 2>/dev/null | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    if j['settings']['name'] == 'PrimeInsurance - Full Pipeline':
        print(j['job_id'])
        break
" 2>/dev/null || echo "")

if [ -n "$EXISTING_JOB" ]; then
  echo "  updating existing job: $EXISTING_JOB"
  echo "$CONFIG" | databricks jobs reset "$EXISTING_JOB" --json @/dev/stdin $PROFILE_FLAG 2>/dev/null
  JOB_ID=$EXISTING_JOB
else
  echo "  creating new job..."
  JOB_ID=$(echo "$CONFIG" | databricks jobs create --json @/dev/stdin $PROFILE_FLAG --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
fi
echo "  job ID: $JOB_ID"

# step 4: summary
echo ""
echo "[4/4] Deploy complete!"
echo ""
echo "=== What was deployed ==="
echo "  Notebooks: $WORKSPACE_PATH/"
echo "  Job ID: $JOB_ID"
echo ""
echo "=== Next steps ==="
echo "  1. Data files are auto-uploaded by the setup notebook on first run"
echo "  2. Trigger the pipeline:"
echo "     databricks jobs run-now $JOB_ID $PROFILE_FLAG"
echo "  3. Monitor at:"
echo "     $(databricks auth describe $PROFILE_FLAG --output json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('host',''))" 2>/dev/null)/jobs/$JOB_ID"
echo ""
echo "=== Pipeline DAG ==="
echo "  setup -> bronze_ingestion"
echo "              |-> silver_customers  -|"
echo "              |-> silver_claims      |-> silver_dq_combined -> uc1_dq_explanations"
echo "              |-> silver_policy      |"
echo "              |-> silver_sales       |-> gold_dimensions -> gold_facts"
echo "              |-> silver_cars        |                       |-> uc2_anomaly_engine"
echo "                                                             |-> uc3_policy_rag"
echo "                                                             |-> uc4_executive_insights"
