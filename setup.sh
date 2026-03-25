#!/bin/bash
# setup.sh
# Provisions Unity Catalog infrastructure for PrimeInsurance Data Platform.
# Idempotent: skips anything that already exists.
#
# Prerequisites:
#   databricks auth login
#
# Usage:
#   ./setup.sh

set -e

# ── Project constants (not user-specific) ─────────────────────────────────────
CATALOG="databricks-hackathon-insurance"
SCHEMAS=("bronze" "silver" "gold")
VOLUME_SCHEMA="bronze"
VOLUME_NAME="workshop_data"

# ── Discover from authenticated session ───────────────────────────────────────
echo "=== PrimeInsurance Infrastructure Setup ==="
echo ""
echo "Discovering workspace details..."

AUTH_ENV=$(databricks auth env 2>/dev/null)
DATABRICKS_HOST=$(echo "$AUTH_ENV" | python3 -c "import json,sys; print(json.load(sys.stdin)['env']['DATABRICKS_HOST'])" 2>/dev/null)
DATABRICKS_TOKEN=$(echo "$AUTH_ENV" | python3 -c "import json,sys; print(json.load(sys.stdin)['env']['DATABRICKS_TOKEN'])" 2>/dev/null)

if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
    echo "ERROR: Not authenticated. Run 'databricks auth login' first."
    exit 1
fi

echo "  Host : $DATABRICKS_HOST"
echo "  User : $(databricks current-user me 2>/dev/null | grep user_name | awk '{print $2}')"

# Auto-discover first running SQL warehouse
echo "  Finding SQL warehouse..."
WAREHOUSE_ID=$(databricks warehouses list --output json 2>/dev/null | \
    python3 -c "
import json, sys
warehouses = json.load(sys.stdin)
running = [w for w in warehouses if w.get('state') == 'RUNNING']
all_ws = running if running else warehouses
if all_ws:
    print(all_ws[0]['id'])
")

if [ -z "$WAREHOUSE_ID" ]; then
    echo "ERROR: No SQL warehouse found. Please start a SQL warehouse in your workspace."
    exit 1
fi
echo "  Warehouse: $WAREHOUSE_ID"
echo ""

# ── Helper: run SQL via Statement Execution API ───────────────────────────────
run_sql() {
    local SQL="$1"
    local RESPONSE=$(curl -s -X POST "$DATABRICKS_HOST/api/2.0/sql/statements" \
        -H "Authorization: Bearer $DATABRICKS_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{
            \"statement\": \"$SQL\",
            \"warehouse_id\": \"$WAREHOUSE_ID\",
            \"wait_timeout\": \"30s\"
        }")
    local STATUS=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',{}).get('state','UNKNOWN'))")
    if [ "$STATUS" != "SUCCEEDED" ]; then
        echo "ERROR: SQL failed — $SQL"
        echo "$RESPONSE"
        exit 1
    fi
}

# ── Catalog ───────────────────────────────────────────────────────────────────
echo "[1/3] Catalog: $CATALOG"
run_sql "CREATE CATALOG IF NOT EXISTS \`$CATALOG\` COMMENT 'PrimeInsurance Data Intelligence Platform'"
echo "      done"

# ── Schemas ───────────────────────────────────────────────────────────────────
echo ""
echo "[2/3] Schemas:"
for SCHEMA in "${SCHEMAS[@]}"; do
    printf "      %-10s" "$SCHEMA"
    run_sql "CREATE SCHEMA IF NOT EXISTS \`$CATALOG\`.\`$SCHEMA\`"
    echo "done"
done

# ── Volume ────────────────────────────────────────────────────────────────────
echo ""
echo "[3/3] Volume: $CATALOG.$VOLUME_SCHEMA.$VOLUME_NAME"
run_sql "CREATE VOLUME IF NOT EXISTS \`$CATALOG\`.\`$VOLUME_SCHEMA\`.\`$VOLUME_NAME\` COMMENT 'Raw source files from 6 regional insurance companies'"
echo "      done"

echo ""
echo "=== Setup complete ==="
echo "Next: run ./upload_data.sh to upload source files"
