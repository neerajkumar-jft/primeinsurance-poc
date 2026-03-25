# Databricks notebook source
# MAGIC %md
# MAGIC # Create Lakeview Dashboard via REST API
# MAGIC
# MAGIC Programmatically creates (or updates) the **Prime Insurance - Executive Dashboard**
# MAGIC using the Databricks Lakeview Dashboard API (`POST /api/2.0/lakeview/dashboards`).
# MAGIC
# MAGIC Pages:
# MAGIC 1. Overview — KPI counters from `regulatory_readiness`
# MAGIC 2. Claims Performance — bar charts, SLA table, counters
# MAGIC 3. Customer Identity — dedup stats, regional distribution
# MAGIC 4. Revenue & Inventory — sell-through, aging alerts
# MAGIC 5. Data Quality — DQ issues, AI explanations
# MAGIC 6. AI Insights — executive insights, RAG history

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

CATALOG = "prime-ins-jellsinki-poc"
DASHBOARD_NAME = "Prime Insurance - Executive Dashboard"

print(f"Catalog: {CATALOG}")
print(f"Dashboard: {DASHBOARD_NAME}")

# COMMAND ----------

import json
import requests

# Databricks API context
workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
if not workspace_url:
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

api_base = f"https://{workspace_url}"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

print(f"Workspace: {api_base}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto-detect SQL Warehouse

# COMMAND ----------

def get_warehouse_id():
    """Find a running SQL warehouse, preferring serverless. Returns None for serverless execution."""
    try:
        resp = requests.get(f"{api_base}/api/2.0/sql/warehouses", headers=headers)
        resp.raise_for_status()
        warehouses = resp.json().get("warehouses", [])
        for wh in sorted(warehouses, key=lambda w: (
            0 if w.get("enable_serverless_compute") else 1,
            0 if w.get("state") == "RUNNING" else 1,
        )):
            print(f"Using warehouse: {wh['name']} (id={wh['id']}, state={wh.get('state')})")
            return wh["id"]
    except Exception as e:
        print(f"Warning: could not list warehouses: {e}")
    print("No warehouse found — dashboard will use serverless compute")
    return None

warehouse_id = get_warehouse_id()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Datasets (queryLines format)

# COMMAND ----------

C = f"`{CATALOG}`"

datasets = [
    {
        "name": "ds_readiness",
        "displayName": "Regulatory Readiness",
        "queryLines": [
            f"SELECT DATE_FORMAT(assessment_date, 'MMM dd, yyyy HH:mm') AS assessment_date,\n",
            f"       ROUND(customer_registry_score, 1) AS customer_registry_score,\n",
            f"       ROUND(data_quality_score, 1) AS data_quality_score,\n",
            f"       ROUND(claims_efficiency_score, 1) AS claims_efficiency_score,\n",
            f"       ROUND(overall_readiness_score, 1) AS overall_readiness_score,\n",
            f"       readiness_status\n",
            f"FROM {C}.gold.regulatory_readiness"
        ],
    },
    {
        "name": "ds_claims_by_region",
        "displayName": "Claims by Region",
        "queryLines": [
            f"SELECT incident_state AS region, COUNT(*) AS claim_count,\n",
            f"       ROUND(AVG(total_claim_amount),2) AS avg_claim_amount\n",
            f"FROM {C}.gold.fact_claims\n",
            f"GROUP BY incident_state"
        ],
    },
    {
        "name": "ds_claims_by_severity",
        "displayName": "Claims by Severity",
        "queryLines": [
            f"SELECT incident_severity, COUNT(*) AS claim_count,\n",
            f"       ROUND(AVG(total_claim_amount),2) AS avg_amount\n",
            f"FROM {C}.gold.fact_claims\n",
            f"GROUP BY incident_severity"
        ],
    },
    {
        "name": "ds_claims_sla",
        "displayName": "Claims SLA Monitor",
        "queryLines": [
            f"SELECT region,\n",
            f"       SUM(total_claims) AS total_claims,\n",
            f"       ROUND(AVG(avg_processing_days), 1) AS avg_processing_days,\n",
            f"       SUM(sla_breaches) AS sla_breaches,\n",
            f"       ROUND(SUM(sla_breaches) * 100.0 / SUM(total_claims), 2) AS sla_breach_pct,\n",
            f"       ROUND(SUM(rejected_claims) * 100.0 / SUM(total_claims), 2) AS rejection_rate_pct,\n",
            f"       ROUND(AVG(avg_claim_amount), 2) AS avg_claim_amount\n",
            f"FROM {C}.gold.claims_sla_monitor\n",
            f"WHERE avg_processing_days IS NOT NULL AND avg_processing_days >= 0\n",
            f"GROUP BY region\n",
            f"ORDER BY sla_breach_pct DESC"
        ],
    },
    {
        "name": "ds_claims_kpis",
        "displayName": "Claims KPIs",
        "queryLines": [
            f"SELECT COUNT(*) AS total_claims,\n",
            f"       ROUND(AVG(processing_days),1) AS avg_processing_days,\n",
            f"       ROUND(SUM(CASE WHEN sla_breach=true THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS sla_breach_pct\n",
            f"FROM {C}.gold.fact_claims"
        ],
    },
    {
        "name": "ds_customer_kpis",
        "displayName": "Customer KPIs",
        "queryLines": [
            f"SELECT COUNT(*) AS unique_customers,\n",
            f"       SUM(CASE WHEN regional_id_count>1 THEN 1 ELSE 0 END) AS duplicates_found,\n",
            f"       ROUND(SUM(CASE WHEN regional_id_count>1 THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS dedup_rate_pct\n",
            f"FROM {C}.gold.regulatory_customer_registry"
        ],
    },
    {
        "name": "ds_customers_by_region",
        "displayName": "Customers by Region",
        "queryLines": [
            f"SELECT region, COUNT(*) AS customer_count\n",
            f"FROM {C}.gold.dim_customer\n",
            f"GROUP BY region\n",
            f"ORDER BY customer_count DESC"
        ],
    },
    {
        "name": "ds_top_dupes",
        "displayName": "Top Duplicated Customers",
        "queryLines": [
            f"SELECT * FROM {C}.gold.regulatory_customer_registry\n",
            f"WHERE regional_id_count > 1\n",
            f"ORDER BY regional_id_count DESC\n",
            f"LIMIT 20"
        ],
    },
    {
        "name": "ds_sales_kpis",
        "displayName": "Sales KPIs",
        "queryLines": [
            f"SELECT COUNT(*) AS total_listings,\n",
            f"       SUM(CASE WHEN is_sold=true THEN 1 ELSE 0 END) AS sold,\n",
            f"       SUM(CASE WHEN is_sold=false THEN 1 ELSE 0 END) AS unsold,\n",
            f"       ROUND(SUM(CASE WHEN is_sold=true THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS sell_through_pct\n",
            f"FROM {C}.gold.fact_sales"
        ],
    },
    {
        "name": "ds_sales_by_region",
        "displayName": "Sales by Region",
        "queryLines": [
            f"SELECT region, COUNT(*) AS listings,\n",
            f"       SUM(CASE WHEN is_sold=true THEN 1 ELSE 0 END) AS sold\n",
            f"FROM {C}.gold.fact_sales\n",
            f"GROUP BY region\n",
            f"ORDER BY sold DESC"
        ],
    },
    {
        "name": "ds_aging",
        "displayName": "Inventory Aging Alerts",
        "queryLines": [
            f"SELECT * FROM {C}.gold.inventory_aging_alerts\n",
            f"ORDER BY days_on_lot DESC\n",
            f"LIMIT 30"
        ],
    },
    {
        "name": "ds_dq_summary",
        "displayName": "DQ Issues by Entity",
        "queryLines": [
            f"SELECT entity, COUNT(*) AS issue_count,\n",
            f"       SUM(records_affected) AS records_affected,\n",
            f"       ROUND(AVG(affected_ratio)*100,2) AS avg_affected_pct\n",
            f"FROM {C}.silver.dq_issues\n",
            f"GROUP BY entity\n",
            f"ORDER BY avg_affected_pct DESC"
        ],
    },
    {
        "name": "ds_dq_explain",
        "displayName": "AI DQ Explanations",
        "queryLines": [
            f"SELECT * FROM {C}.gold.dq_explanation_report\n",
            f"LIMIT 20"
        ],
    },
    {
        "name": "ds_ai_insights",
        "displayName": "Executive AI Insights",
        "queryLines": [
            f"SELECT * FROM {C}.gold.ai_business_insights"
        ],
    },
    {
        "name": "ds_rag",
        "displayName": "RAG Query History",
        "queryLines": [
            f"SELECT * FROM {C}.gold.rag_query_history\n",
            f"LIMIT 20"
        ],
    },
]

print(f"Defined {len(datasets)} datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget Builder Functions (Lakeview v2/v3 format)

# COMMAND ----------

def counter_widget(name, dataset_name, field, title, x, y, width=3, height=2):
    """Counter widget — spec.version=2, frame inside spec, SUM expression."""
    expr_name = f"sum({field})"
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": expr_name,
                                "expression": f"SUM(`{field}`)"
                            }
                        ],
                        "disaggregated": False
                    }
                }
            ],
            "spec": {
                "version": 2,
                "frame": {
                    "showTitle": True,
                    "title": title,
                    "headerAlignment": "center"
                },
                "widgetType": "counter",
                "encodings": {
                    "value": {
                        "fieldName": expr_name
                    }
                }
            },
            "specExtensions": {
                "widgetBorderColor": {
                    "light": "#008EFF"
                }
            }
        },
        "position": {
            "x": x,
            "y": y,
            "width": width,
            "height": height
        }
    }


def bar_widget(name, dataset_name, x_field, x_label, y_field, y_label, x, y, width=6, height=6):
    """Bar chart — spec.version=3, frame inside spec, SUM for y-axis."""
    y_expr_name = f"sum({y_field})"
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": x_field,
                                "expression": f"`{x_field}`"
                            },
                            {
                                "name": y_expr_name,
                                "expression": f"SUM(`{y_field}`)"
                            }
                        ],
                        "disaggregated": False
                    }
                }
            ],
            "spec": {
                "version": 3,
                "frame": {
                    "showTitle": True,
                    "title": f"{y_label} by {x_label}"
                },
                "widgetType": "bar",
                "encodings": {
                    "x": {
                        "fieldName": x_field,
                        "displayName": x_label,
                        "scale": {
                            "type": "categorical"
                        }
                    },
                    "y": {
                        "fieldName": y_expr_name,
                        "displayName": y_label,
                        "scale": {
                            "type": "quantitative"
                        }
                    },
                    "label": {
                        "show": True
                    }
                }
            },
            "specExtensions": {
                "widgetBorderColor": {
                    "light": "#008EFF"
                }
            }
        },
        "position": {
            "x": x,
            "y": y,
            "width": width,
            "height": height
        }
    }


def table_widget(name, dataset_name, columns, x, y, width=6, height=6):
    """Table widget — spec.version=2, frame inside spec, disaggregated=true.

    columns: list of (field_name, display_name) tuples
    """
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": "main_query",
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {
                                "name": col[0],
                                "expression": f"`{col[0]}`"
                            }
                            for col in columns
                        ],
                        "disaggregated": True
                    }
                }
            ],
            "spec": {
                "version": 2,
                "headerHeight": 43,
                "frame": {
                    "showTitle": True,
                    "title": name.replace("w_", "").replace("_", " ").title(),
                    "headerAlignment": "center"
                },
                "widgetType": "table",
                "encodings": {
                    "columns": [
                        {
                            "fieldName": col[0],
                            "displayName": col[1]
                        }
                        for col in columns
                    ]
                }
            },
            "specExtensions": {
                "widgetBorderColor": {
                    "light": "#008EFF"
                }
            }
        },
        "position": {
            "x": x,
            "y": y,
            "width": width,
            "height": height
        }
    }


def filter_widget(name, dataset_name, field, title, x, y, width=2, height=1):
    """Multi-select filter widget matching Lakeview format from dpdp reference."""
    query_name = f"datasets/{dataset_name}_{field}"
    return {
        "widget": {
            "name": name,
            "queries": [
                {
                    "name": query_name,
                    "query": {
                        "datasetName": dataset_name,
                        "fields": [
                            {"name": field, "expression": f"`{field}`"},
                            {"name": f"{field}_associativity", "expression": "COUNT_IF(`associative_filter_predicate_group`)"}
                        ],
                        "disaggregated": False
                    }
                }
            ],
            "spec": {
                "version": 2,
                "frame": {
                    "showTitle": True,
                    "title": title
                },
                "widgetType": "filter-multi-select",
                "encodings": {
                    "fields": [
                        {"fieldName": field, "queryName": query_name}
                    ]
                }
            }
        },
        "position": {"x": x, "y": y, "width": width, "height": height}
    }


def header_widget(name, title, x, y, width=6, height=2):
    """Markdown header banner widget."""
    return {
        "widget": {
            "name": name,
            "multilineTextboxSpec": {
                "lines": [
                    f"<span style=\"text-align:center;display:block\"><span style=\"font-size: 36px;line-height: 1.5;color: #E8ECF0;\">{title}</span></span>"
                ]
            },
            "specExtensions": {
                "widgetBackgroundColor": {
                    "light": "#008FFF"
                },
                "widgetBorderColor": {
                    "light": "#96C6ED"
                }
            }
        },
        "position": {
            "x": x,
            "y": y,
            "width": width,
            "height": height
        }
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 1: Overview

# COMMAND ----------

page1_layout = [
    header_widget("overview-header", "Prime Insurance — Executive Overview", x=0, y=0, width=6, height=2),
    counter_widget("w-total-claims", "ds_claims_kpis", "total_claims", "Total Claims", x=0, y=2, width=2),
    counter_widget("w-avg-proc-days", "ds_claims_kpis", "avg_processing_days", "Avg Processing Days", x=2, y=2, width=2),
    counter_widget("w-unique-customers", "ds_customer_kpis", "unique_customers", "Total Customers", x=4, y=2, width=2),
    counter_widget("w-sell-through", "ds_sales_kpis", "sell_through_pct", "Sell-Through %", x=0, y=4, width=2),
    counter_widget("w-sla-breach-overview", "ds_claims_kpis", "sla_breach_pct", "SLA Breach %", x=2, y=4, width=2),
    counter_widget("w-dedup-rate-overview", "ds_customer_kpis", "dedup_rate_pct", "Dedup Rate %", x=4, y=4, width=2),
    table_widget(
        "w-readiness-table", "ds_readiness",
        [
            ("assessment_date", "Assessment Date"),
            ("customer_registry_score", "Customer Registry"),
            ("data_quality_score", "Data Quality"),
            ("claims_efficiency_score", "Claims Efficiency"),
            ("overall_readiness_score", "Overall Score"),
            ("readiness_status", "Status"),
        ],
        x=0, y=6, width=6, height=6,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 2: Claims Performance

# COMMAND ----------

page2_layout = [
    header_widget("claims-header", "Claims Performance", x=0, y=0, width=6, height=2),
    filter_widget("f-claims-region", "ds_claims_by_region", "region", "Filter by Region", x=0, y=2, width=2),
    filter_widget("f-claims-severity", "ds_claims_by_severity", "incident_severity", "Filter by Severity", x=2, y=2, width=2),
    counter_widget("w-total-claims-p2", "ds_claims_kpis", "total_claims", "Total Claims", x=0, y=3, width=2),
    counter_widget("w-avg-proc-days-p2", "ds_claims_kpis", "avg_processing_days", "Avg Processing Days", x=2, y=3, width=2),
    counter_widget("w-sla-breach-pct", "ds_claims_kpis", "sla_breach_pct", "SLA Breach %", x=4, y=3, width=2),
    bar_widget(
        "w-claims-by-region", "ds_claims_by_region",
        "region", "Region", "claim_count", "Claims",
        x=0, y=4, width=3, height=6,
    ),
    bar_widget(
        "w-claims-by-severity", "ds_claims_by_severity",
        "incident_severity", "Severity", "claim_count", "Claims",
        x=3, y=4, width=3, height=6,
    ),
    table_widget(
        "w-sla-monitor-table", "ds_claims_sla",
        [
            ("region", "Region"),
            ("total_claims", "Total Claims"),
            ("avg_processing_days", "Avg Days"),
            ("sla_breaches", "SLA Breaches"),
            ("sla_breach_pct", "Breach %"),
            ("rejection_rate_pct", "Rejection %"),
            ("avg_claim_amount", "Avg Amount"),
        ],
        x=0, y=10, width=6, height=6,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 3: Customer Identity

# COMMAND ----------

page3_layout = [
    header_widget("customer-header", "Customer Identity & Deduplication", x=0, y=0, width=6, height=2),
    counter_widget("w-uniq-cust", "ds_customer_kpis", "unique_customers", "Unique Customers", x=0, y=2, width=2),
    counter_widget("w-dups-found", "ds_customer_kpis", "duplicates_found", "Duplicates Found", x=2, y=2, width=2),
    counter_widget("w-dedup-rate", "ds_customer_kpis", "dedup_rate_pct", "Dedup Rate %", x=4, y=2, width=2),
    bar_widget(
        "w-customers-by-region", "ds_customers_by_region",
        "region", "Region", "customer_count", "Customers",
        x=0, y=4, width=3, height=6,
    ),
    table_widget(
        "w-top-duplicates", "ds_top_dupes",
        [
            ("master_customer_id", "Master Customer ID"),
            ("regional_id_count", "Regional ID Count"),
            ("regional_ids", "Original IDs"),
            ("source_files", "Source Files"),
        ],
        x=3, y=4, width=3, height=6,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 4: Revenue & Inventory

# COMMAND ----------

page4_layout = [
    header_widget("revenue-header", "Revenue & Inventory", x=0, y=0, width=6, height=2),
    filter_widget("f-sales-region", "ds_sales_by_region", "region", "Filter by Region", x=0, y=2, width=2),
    counter_widget("w-total-listings", "ds_sales_kpis", "total_listings", "Total Listings", x=0, y=3, width=1),
    counter_widget("w-sold-count", "ds_sales_kpis", "sold", "Sold", x=1, y=2, width=1),
    counter_widget("w-unsold-count", "ds_sales_kpis", "unsold", "Unsold", x=2, y=2, width=1),
    counter_widget("w-sell-through-p4", "ds_sales_kpis", "sell_through_pct", "Sell-Through %", x=3, y=2, width=1),
    bar_widget(
        "w-sales-by-region", "ds_sales_by_region",
        "region", "Region", "sold", "Sold Units",
        x=0, y=4, width=3, height=6,
    ),
    table_widget(
        "w-aging-alerts", "ds_aging",
        [
            ("region", "Region"),
            ("car_name", "Car"),
            ("model", "Model"),
            ("fuel", "Fuel"),
            ("days_on_lot", "Days on Lot"),
            ("original_selling_price", "Price"),
            ("aging_flag", "Alert"),
            ("fast_sell_region", "Redistribution Target"),
        ],
        x=3, y=4, width=3, height=6,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 5: Data Quality

# COMMAND ----------

page5_layout = [
    header_widget("dq-header", "Data Quality", x=0, y=0, width=6, height=2),
    bar_widget(
        "w-dq-by-entity", "ds_dq_summary",
        "entity", "Entity", "issue_count", "Issues",
        x=0, y=2, width=3, height=6,
    ),
    table_widget(
        "w-dq-summary-table", "ds_dq_summary",
        [
            ("entity", "Entity"),
            ("issue_count", "Issues"),
            ("records_affected", "Records Affected"),
            ("avg_affected_pct", "Avg Affected %"),
        ],
        x=3, y=2, width=3, height=6,
    ),
    table_widget(
        "w-dq-explanations", "ds_dq_explain",
        [
            ("entity", "Entity"),
            ("rule_name", "Rule"),
            ("severity", "Severity"),
            ("what_was_found", "What Was Found"),
            ("why_it_matters", "Why It Matters"),
            ("how_to_prevent", "How to Prevent"),
        ],
        x=0, y=8, width=6, height=6,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Page 6: AI Insights

# COMMAND ----------

page6_layout = [
    header_widget("ai-header", "AI Insights & RAG", x=0, y=0, width=6, height=2),
    table_widget(
        "w-ai-insights", "ds_ai_insights",
        [
            ("domain", "Domain"),
            ("headline", "Headline"),
            ("key_findings", "Key Findings"),
            ("alerts", "Alerts"),
            ("recommendations", "Recommendations"),
        ],
        x=0, y=2, width=6, height=6,
    ),
    table_widget(
        "w-rag-history", "ds_rag",
        [
            ("question", "Question"),
            ("answer", "Answer"),
            ("cited_policies", "Cited Policies"),
            ("confidence_level", "Confidence"),
            ("confidence_score", "Score"),
        ],
        x=0, y=8, width=6, height=6,
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assemble Dashboard Payload

# COMMAND ----------

pages = [
    {
        "name": "page_overview",
        "displayName": "Overview",
        "layout": page1_layout,
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "page_claims",
        "displayName": "Claims Performance",
        "layout": page2_layout,
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "page_customer",
        "displayName": "Customer Identity",
        "layout": page3_layout,
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "page_revenue",
        "displayName": "Revenue & Inventory",
        "layout": page4_layout,
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "page_dq",
        "displayName": "Data Quality",
        "layout": page5_layout,
        "pageType": "PAGE_TYPE_CANVAS"
    },
    {
        "name": "page_ai",
        "displayName": "AI Insights",
        "layout": page6_layout,
        "pageType": "PAGE_TYPE_CANVAS"
    },
]

serialized_dashboard = json.dumps({"pages": pages, "datasets": datasets})

dashboard_payload = {
    "display_name": DASHBOARD_NAME,
    "serialized_dashboard": serialized_dashboard,
}

if warehouse_id:
    dashboard_payload["warehouse_id"] = warehouse_id

print(f"Payload built: {len(pages)} pages, {len(datasets)} datasets")
print(f"Payload size: {len(serialized_dashboard)} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update Dashboard

# COMMAND ----------

def find_existing_dashboard(name):
    """Search for an existing dashboard by display name."""
    try:
        resp = requests.get(
            f"{api_base}/api/2.0/lakeview/dashboards",
            headers=headers,
            params={"page_size": 100},
        )
        resp.raise_for_status()
        for d in resp.json().get("dashboards", []):
            if d.get("display_name") == name:
                print(f"Found existing dashboard: {d['dashboard_id']}")
                return d["dashboard_id"]
    except Exception as e:
        print(f"Warning: search failed: {e}")
    return None


def create_or_update_dashboard(payload):
    """Create a new dashboard or update an existing one."""
    existing_id = find_existing_dashboard(payload["display_name"])

    if existing_id:
        print(f"Updating existing dashboard {existing_id}...")
        resp = requests.patch(
            f"{api_base}/api/2.0/lakeview/dashboards/{existing_id}",
            headers=headers,
            json=payload,
        )
    else:
        print("Creating new dashboard...")
        resp = requests.post(
            f"{api_base}/api/2.0/lakeview/dashboards",
            headers=headers,
            json=payload,
        )

    if resp.status_code in (200, 201):
        result = resp.json()
        dashboard_id = result.get("dashboard_id", existing_id or "unknown")
        print(f"Success! Dashboard ID: {dashboard_id}")
        return dashboard_id
    else:
        print(f"Error {resp.status_code}: {resp.text[:500]}")
        raise RuntimeError(f"Dashboard API returned {resp.status_code}: {resp.text[:500]}")


dashboard_id = create_or_update_dashboard(dashboard_payload)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish Dashboard

# COMMAND ----------

def publish_dashboard(dash_id):
    """Publish the dashboard so it is viewable by others."""
    publish_payload = {}
    if warehouse_id:
        publish_payload["warehouse_id"] = warehouse_id
    publish_payload["embed_credentials"] = False

    try:
        resp = requests.post(
            f"{api_base}/api/2.0/lakeview/dashboards/{dash_id}/published",
            headers=headers,
            json=publish_payload,
        )
        if resp.status_code in (200, 201):
            print("Dashboard published successfully")
        else:
            print(f"Publish warning ({resp.status_code}): {resp.text[:300]}")
    except Exception as e:
        print(f"Publish warning: {e}")

publish_dashboard(dashboard_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard URL

# COMMAND ----------

draft_url = f"{api_base}/sql/dashboardsv3/{dashboard_id}"
published_url = f"{api_base}/dashboardsv3/{dashboard_id}/published"

print("=" * 70)
print("PRIME INSURANCE - EXECUTIVE DASHBOARD")
print("=" * 70)
print(f"Draft URL:     {draft_url}")
print(f"Published URL: {published_url}")
print("=" * 70)

displayHTML(f"""
<div style="padding:20px; background:#f0f8ff; border-radius:8px; font-family:sans-serif;">
  <h2 style="margin-top:0;">Prime Insurance - Executive Dashboard</h2>
  <p><strong>Dashboard ID:</strong> {dashboard_id}</p>
  <p><a href="{draft_url}" target="_blank" style="font-size:16px;">Open Draft Dashboard</a></p>
  <p><a href="{published_url}" target="_blank" style="font-size:16px;">Open Published Dashboard</a></p>
</div>
""")
