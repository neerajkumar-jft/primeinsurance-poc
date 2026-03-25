#!/bin/bash
#
# Deploy PrimeInsurance Data Platform to Databricks
#
# Usage:
#   ./deploy/deploy.sh                                    # default: /Shared/prime_insurance
#   ./deploy/deploy.sh --workspace-path /Users/me/folder  # custom path
#   ./deploy/deploy.sh -p my-profile                      # named databricks profile

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PROFILE_FLAG=""
WORKSPACE_PATH="/Shared/prime_insurance"

while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--profile) PROFILE_FLAG="-p $2"; shift 2 ;;
    --workspace-path) WORKSPACE_PATH="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

# get host and token from databricks config
DB_HOST=$(grep -A2 "DEFAULT" ~/.databrickscfg | grep host | awk '{print $NF}' | tr -d ' ')
DB_TOKEN=$(grep -A2 "DEFAULT" ~/.databrickscfg | grep token | awk '{print $NF}' | tr -d ' ')

if [ -z "$DB_HOST" ] || [ -z "$DB_TOKEN" ]; then
  echo "Error: Databricks CLI not configured. Run: databricks auth login --host <your-workspace-url>"
  exit 1
fi

echo "=== PrimeInsurance Data Platform Deploy ==="
echo "Host:           $DB_HOST"
echo "Workspace path: $WORKSPACE_PATH"
echo ""

# ─────────────────────────────────────────────
# Step 1: Check/create workspace folder
# ─────────────────────────────────────────────
echo "[1/4] Checking workspace folder..."
if databricks workspace list "$WORKSPACE_PATH" $PROFILE_FLAG >/dev/null 2>&1; then
  echo "  workspace exists - will update notebooks"
else
  echo "  creating workspace: $WORKSPACE_PATH"
  databricks workspace mkdirs "$WORKSPACE_PATH" $PROFILE_FLAG
fi

# ─────────────────────────────────────────────
# Step 2: Import all notebooks
# ─────────────────────────────────────────────
echo "[2/4] Importing notebooks..."
NOTEBOOK_COUNT=0
for f in "$PROJECT_DIR"/notebooks/*.py; do
  filename=$(basename "$f" .py)
  databricks workspace import \
    "$WORKSPACE_PATH/$filename" \
    --file "$f" \
    --format SOURCE \
    --language PYTHON \
    --overwrite $PROFILE_FLAG 2>/dev/null
  echo "  imported: $filename"
  NOTEBOOK_COUNT=$((NOTEBOOK_COUNT + 1))
done
echo "  total: $NOTEBOOK_COUNT notebooks"

# ─────────────────────────────────────────────
# Step 3: Create or update pipeline job
# ─────────────────────────────────────────────
echo "[3/4] Setting up pipeline job..."

# substitute workspace path in config template
CONFIG=$(cat "$SCRIPT_DIR/job_config.json" | sed "s|{{WORKSPACE_PATH}}|$WORKSPACE_PATH|g")

# check if job with this name already exists
EXISTING_JOB=$(curl -s "$DB_HOST/api/2.1/jobs/list" \
  -H "Authorization: Bearer $DB_TOKEN" | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
for j in jobs:
    if j['settings']['name'] == 'Prime Insurance - POC':
        print(j['job_id'])
        break
" 2>/dev/null || echo "")

if [ -n "$EXISTING_JOB" ]; then
  # update existing job via REST API
  echo "  updating existing job: $EXISTING_JOB"
  python3 -c "
import json
config = json.loads('''$(echo "$CONFIG")''')
payload = {'job_id': $EXISTING_JOB, 'new_settings': config}
print(json.dumps(payload))
" > /tmp/primeins_job_reset.json

  HTTP_CODE=$(curl -s -w "%{http_code}" -o /tmp/primeins_job_response.json \
    -X POST "$DB_HOST/api/2.1/jobs/reset" \
    -H "Authorization: Bearer $DB_TOKEN" \
    -H "Content-Type: application/json" \
    -d @/tmp/primeins_job_reset.json)

  if [ "$HTTP_CODE" = "200" ]; then
    JOB_ID=$EXISTING_JOB
    echo "  job updated successfully"
  else
    echo "  update failed (HTTP $HTTP_CODE), creating new job instead"
    EXISTING_JOB=""
  fi
fi

if [ -z "$EXISTING_JOB" ]; then
  # create new job via REST API
  echo "  creating new job..."
  RESPONSE=$(curl -s -X POST "$DB_HOST/api/2.1/jobs/create" \
    -H "Authorization: Bearer $DB_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$CONFIG")

  JOB_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])" 2>/dev/null)

  if [ -z "$JOB_ID" ]; then
    echo "  Error creating job: $RESPONSE"
    exit 1
  fi
  echo "  job created: $JOB_ID"
fi

# ─────────────────────────────────────────────
# Step 4: Summary
# ─────────────────────────────────────────────
echo ""
echo "[4/4] Deploy complete!"
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║  PrimeInsurance Pipeline Ready               ║"
echo "╠══════════════════════════════════════════════╣"
echo "║  Notebooks: $WORKSPACE_PATH/"
echo "║  Job ID:    $JOB_ID"
echo "║  Job URL:   $DB_HOST/jobs/$JOB_ID"
echo "╚══════════════════════════════════════════════╝"
echo ""
echo "Run pipeline:"
echo "  databricks jobs run-now $JOB_ID"
echo ""
echo "Pipeline DAG:"
cat << 'DAGEOF'
  setup -> bronze_ingestion
               ├── silver_customers  ─┐
               ├── silver_claims      ├── silver_dq_combined ── uc1_dq_explanations
               ├── silver_policy      │
               ├── silver_sales       ├── gold_dimensions ── gold_facts
               └── silver_cars        ┘                       ├── uc2_anomaly_engine
                                                              ├── uc3_policy_rag
                                                              └── uc4_executive_insights
DAGEOF
echo ""
echo "Notes:"
echo "  - Setup notebook auto-uploads data files on first run"
echo "  - Notebooks skip if tables already exist (safe to rerun)"
echo "  - All team members can access $WORKSPACE_PATH"
