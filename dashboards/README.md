# Dashboards

Portable Lakeview dashboard definitions for the PrimeInsurance Data Intelligence
Platform. Each JSON has been stripped of workspace-specific metadata
(`dashboard_id`, `path`, `warehouse_id`, `lifecycle_state`, timestamps) so the
file can be imported into any workspace.

## Files

| File | Pages | Drives |
|------|-------|--------|
| `primeinsurance_dashboard.json` | 1 | Executive KPIs: claims, customers, inventory |
| `primeinsurance_business_intelligence.json` | 4 | Customer Registry, Claims Performance, Inventory & Revenue, Customer Risk |

## Importing into a new workspace

Each file is a `lakeview create` request body with `display_name` and
`serialized_dashboard`. At import time you must supply the target warehouse ID
and (optionally) override the parent path:

```bash
databricks lakeview create \
  --json @dashboards/primeinsurance_dashboard.json \
  --warehouse-id <TARGET_WAREHOUSE_ID> \
  --profile <TARGET_PROFILE>
```

Then publish:

```bash
databricks lakeview publish <NEW_DASHBOARD_ID> \
  --warehouse-id <TARGET_WAREHOUSE_ID> \
  --profile <TARGET_PROFILE>
```

The tiles reference tables by three-part name (`primeins.gold.*`,
`primeins.silver.*`) so as long as the target workspace has the same catalog
name, no query edits are needed.

## Re-exporting after edits

When the dashboards are updated in the workspace UI and you want to capture the
changes back into the repo, fetch and re-strip:

```bash
# 1. Get fresh definition from the workspace
databricks lakeview get <DASHBOARD_ID> --output json > /tmp/fresh.json

# 2. Strip workspace-specific fields
python3 -c "
import json
STRIP = ['dashboard_id','path','parent_path','warehouse_id','lifecycle_state',
         'create_time','update_time','etag','revision_create_time']
with open('/tmp/fresh.json') as f: d = json.load(f)
for k in STRIP: d.pop(k, None)
with open('dashboards/primeinsurance_dashboard.json','w') as f:
    json.dump(d, f, indent=2, sort_keys=True); f.write('\n')
"
```
