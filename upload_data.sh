#!/bin/bash
# upload_data.sh
# Uploads source insurance data files to the Databricks Volume via Files API.
# Idempotent: skips files that already exist in the volume.
#
# Prerequisites:
#   - setup.sh must have been run first (volume must exist)
#   - ~/.databrickscfg must have valid token
#
# Usage:
#   ./upload_data.sh

set -e

AUTH_ENV=$(databricks auth env 2>/dev/null)
HOST=$(echo "$AUTH_ENV" | python3 -c "import json,sys; print(json.load(sys.stdin)['env']['DATABRICKS_HOST'])" 2>/dev/null)
TOKEN=$(echo "$AUTH_ENV" | python3 -c "import json,sys; print(json.load(sys.stdin)['env']['DATABRICKS_TOKEN'])" 2>/dev/null)

if [ -z "$HOST" ] || [ -z "$TOKEN" ]; then
    echo "ERROR: Not authenticated. Run 'databricks auth login' first."
    exit 1
fi
VOLUME_BASE="/Volumes/databricks-hackathon-insurance/bronze/workshop_data"
LOCAL_DATA="$(cd "$(dirname "$0")" && pwd)/data/autoinsurancedata"

echo "=== PrimeInsurance Data Upload ==="
echo "Source : $LOCAL_DATA"
echo "Target : $HOST$VOLUME_BASE"
echo ""

# verify local data folder exists
if [ ! -d "$LOCAL_DATA" ]; then
    echo "ERROR: local data folder not found at $LOCAL_DATA"
    exit 1
fi

uploaded=0
skipped=0
failed=0

while IFS= read -r -d '' local_file; do
    rel_path="${local_file#$LOCAL_DATA/}"
    encoded_path=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1]))" "$rel_path")
    url="$HOST/api/2.0/fs/files$VOLUME_BASE/$encoded_path"

    # check if file already exists
    http_check=$(curl -s -o /dev/null -w "%{http_code}" -X HEAD "$url" \
        -H "Authorization: Bearer $TOKEN")

    if [ "$http_check" = "200" ]; then
        echo "  [SKIP]   $rel_path"
        ((skipped++)) || true
    else
        http_code=$(curl -s -o /tmp/upload_resp.txt -w "%{http_code}" -X PUT "$url" \
            -H "Authorization: Bearer $TOKEN" \
            -H "Content-Type: application/octet-stream" \
            --data-binary @"$local_file")

        if [ "$http_code" = "200" ] || [ "$http_code" = "201" ] || [ "$http_code" = "204" ]; then
            echo "  [OK]     $rel_path"
            ((uploaded++)) || true
        else
            echo "  [FAILED] $rel_path — HTTP $http_code: $(cat /tmp/upload_resp.txt)"
            ((failed++)) || true
        fi
    fi
done < <(find "$LOCAL_DATA" -type f -print0)

echo ""
echo "=== Upload Summary ==="
echo "  Uploaded : $uploaded"
echo "  Skipped  : $skipped (already existed)"
echo "  Failed   : $failed"

if [ "$failed" -gt 0 ]; then
    echo "WARNING: $failed files failed. Re-run to retry."
    exit 1
fi

echo ""
echo "Done. Run Bronze ingestion notebooks in Databricks next."
