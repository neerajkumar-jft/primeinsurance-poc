#!/usr/bin/env python3
"""Generate architecture document with diagrams for Q9."""

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.colors import HexColor, Color, white, black
from reportlab.lib.units import mm, cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Image, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.lib import colors
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon, Circle, Group
from reportlab.graphics.charts.textlabels import Label
from reportlab.graphics import renderPDF
import os

OUTPUT_PATH = os.path.expanduser("~/Workspace/hackathon/docs/architecture_document.pdf")

# Use landscape A4 for diagrams
doc = SimpleDocTemplate(
    OUTPUT_PATH,
    pagesize=landscape(A4),
    leftMargin=15*mm,
    rightMargin=15*mm,
    topMargin=15*mm,
    bottomMargin=15*mm,
)

styles = getSampleStyleSheet()

# Colors
BLUE_DARK = HexColor('#1a5276')
BLUE_MED = HexColor('#2980b9')
BLUE_LIGHT = HexColor('#d6eaf8')
GREEN_DARK = HexColor('#1e8449')
GREEN_LIGHT = HexColor('#d5f5e3')
ORANGE_DARK = HexColor('#e67e22')
ORANGE_LIGHT = HexColor('#fdebd0')
RED_DARK = HexColor('#c0392b')
RED_LIGHT = HexColor('#fadbd8')
PURPLE_DARK = HexColor('#7d3c98')
PURPLE_LIGHT = HexColor('#e8daef')
GRAY_DARK = HexColor('#2c3e50')
GRAY_MED = HexColor('#7f8c8d')
GRAY_LIGHT = HexColor('#ecf0f1')
TEAL_DARK = HexColor('#117a65')
TEAL_LIGHT = HexColor('#d1f2eb')
YELLOW_LIGHT = HexColor('#fef9e7')
YELLOW_DARK = HexColor('#b7950b')

# Custom styles
styles.add(ParagraphStyle(name='DocTitle', parent=styles['Title'], fontSize=22,
    textColor=BLUE_DARK, spaceAfter=4, fontName='Helvetica-Bold', alignment=TA_CENTER))
styles.add(ParagraphStyle(name='DocSubTitle', parent=styles['Normal'], fontSize=12,
    textColor=GRAY_MED, spaceAfter=20, fontName='Helvetica', alignment=TA_CENTER))
styles.add(ParagraphStyle(name='SectionTitle', parent=styles['Heading1'], fontSize=16,
    textColor=BLUE_DARK, spaceAfter=8, spaceBefore=12, fontName='Helvetica-Bold'))
styles.add(ParagraphStyle(name='SubSection', parent=styles['Heading2'], fontSize=12,
    textColor=GRAY_DARK, spaceAfter=6, spaceBefore=8, fontName='Helvetica-Bold'))
styles.add(ParagraphStyle(name='Body', parent=styles['Normal'], fontSize=9,
    textColor=GRAY_DARK, spaceAfter=4, fontName='Helvetica', leading=12))
styles.add(ParagraphStyle(name='DiagramTitle', parent=styles['Normal'], fontSize=14,
    textColor=BLUE_DARK, spaceAfter=8, spaceBefore=4, fontName='Helvetica-Bold',
    alignment=TA_CENTER))
styles.add(ParagraphStyle(name='Caption', parent=styles['Normal'], fontSize=8,
    textColor=GRAY_MED, spaceAfter=6, fontName='Helvetica-Oblique', alignment=TA_CENTER))

story = []

def hr():
    return HRFlowable(width="100%", thickness=0.5, color=HexColor('#bdc3c7'),
                       spaceBefore=4, spaceAfter=4)

def rounded_rect(d, x, y, w, h, fill, stroke=None, r=6):
    """Draw a rounded rectangle on drawing d."""
    if stroke is None:
        stroke = fill
    d.add(Rect(x, y, w, h, rx=r, ry=r, fillColor=fill, strokeColor=stroke, strokeWidth=0.5))

def box_with_text(d, x, y, w, h, text, fill, text_color=black, font_size=7, bold=False):
    """Draw a box with centered text."""
    rounded_rect(d, x, y, w, h, fill)
    font_name = 'Helvetica-Bold' if bold else 'Helvetica'
    lines = text.split('\n')
    line_height = font_size + 2
    total_height = len(lines) * line_height
    start_y = y + h/2 + total_height/2 - line_height/2
    for i, line in enumerate(lines):
        s = String(x + w/2, start_y - i*line_height, line,
                   fontSize=font_size, fillColor=text_color,
                   textAnchor='middle', fontName=font_name)
        d.add(s)

def arrow_right(d, x1, y, x2, color=GRAY_MED, text=None):
    """Draw a right arrow from x1,y to x2,y."""
    d.add(Line(x1, y, x2-5, y, strokeColor=color, strokeWidth=1.5))
    d.add(Polygon(points=[x2-8, y+4, x2, y, x2-8, y-4],
                  fillColor=color, strokeColor=color, strokeWidth=0.5))
    if text:
        d.add(String((x1+x2)/2, y+6, text, fontSize=6, fillColor=GRAY_MED,
                      textAnchor='middle', fontName='Helvetica'))

def arrow_down(d, x, y1, y2, color=GRAY_MED, text=None):
    """Draw a down arrow from x,y1 to x,y2."""
    d.add(Line(x, y1, x, y2+5, strokeColor=color, strokeWidth=1.5))
    d.add(Polygon(points=[x-4, y2+8, x, y2, x+4, y2+8],
                  fillColor=color, strokeColor=color, strokeWidth=0.5))
    if text:
        d.add(String(x+8, (y1+y2)/2, text, fontSize=6, fillColor=GRAY_MED,
                      textAnchor='start', fontName='Helvetica'))


# ========================================
# TITLE PAGE
# ========================================
story.append(Spacer(1, 60*mm))
story.append(Paragraph("PrimeInsurance POC", styles['DocTitle']))
story.append(Paragraph("Architecture Document", styles['DocSubTitle']))
story.append(Spacer(1, 15*mm))
story.append(Paragraph("Catalog: prime-ins-jellsinki-poc", styles['DocSubTitle']))
story.append(Paragraph("Model: databricks-gpt-oss-20b | Embedding: all-MiniLM-L6-v2", styles['DocSubTitle']))
story.append(Spacer(1, 10*mm))

# Table of contents
toc_data = [
    ['Section', 'Page'],
    ['1. Data Flow Diagram', '2'],
    ['2. Component Map', '3'],
    ['3. Data Quality Strategy', '4-5'],
    ['4. Gen AI Integration', '6-7'],
]
toc = Table(toc_data, colWidths=[200, 60])
toc.setStyle(TableStyle([
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ('FONTSIZE', (0, 0), (-1, -1), 10),
    ('TEXTCOLOR', (0, 0), (-1, 0), BLUE_DARK),
    ('TEXTCOLOR', (0, 1), (-1, -1), GRAY_DARK),
    ('LINEBELOW', (0, 0), (-1, 0), 1, BLUE_MED),
    ('ALIGN', (1, 0), (1, -1), 'CENTER'),
    ('TOPPADDING', (0, 0), (-1, -1), 6),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
]))
story.append(toc)
story.append(PageBreak())


# ========================================
# 1. DATA FLOW DIAGRAM
# ========================================
story.append(Paragraph("1. Data Flow Diagram", styles['SectionTitle']))
story.append(Paragraph("End-to-end pipeline: from source files to business user", styles['Caption']))
story.append(hr())

d = Drawing(750, 420)

# Background layer labels (vertical on left)
layers = [
    (360, 'SOURCE', GRAY_LIGHT),
    (280, 'BRONZE', HexColor('#eaf2f8')),
    (200, 'SILVER', GREEN_LIGHT),
    (120, 'GOLD', ORANGE_LIGHT),
    (40, 'AI + SERVING', PURPLE_LIGHT),
]

for y_pos, label, bg in layers:
    d.add(Rect(0, y_pos, 750, 70, fillColor=bg, strokeColor=HexColor('#ddd'), strokeWidth=0.3))
    d.add(String(8, y_pos + 30, label, fontSize=7, fillColor=GRAY_MED,
                 fontName='Helvetica-Bold', textAnchor='start'))

# SOURCE layer
box_with_text(d, 60, 370, 90, 45, 'Insurance 1\n(CSV)', GRAY_LIGHT, font_size=7)
box_with_text(d, 160, 370, 90, 45, 'Insurance 2\n(CSV)', GRAY_LIGHT, font_size=7)
box_with_text(d, 260, 370, 90, 45, 'Insurance 3\n(CSV)', GRAY_LIGHT, font_size=7)
box_with_text(d, 360, 370, 90, 45, 'Insurance 4-6\n(CSV + JSON)', GRAY_LIGHT, font_size=7)
box_with_text(d, 480, 370, 110, 45, 'Unity Catalog\nVolume\n(source_files)', BLUE_LIGHT, BLUE_DARK, font_size=7, bold=True)

arrow_right(d, 150, 392, 160, GRAY_MED)
arrow_right(d, 250, 392, 260, GRAY_MED)
arrow_right(d, 350, 392, 360, GRAY_MED)
arrow_right(d, 450, 392, 480, BLUE_MED, '14 files')

# BRONZE layer
box_with_text(d, 80, 290, 110, 50, 'bronze.customers\n(7 CSV merged)\nunionByName', HexColor('#d6eaf8'), font_size=7)
box_with_text(d, 210, 290, 100, 50, 'bronze.claims\n(2 JSON)\nmultiLine read', HexColor('#d6eaf8'), font_size=7)
box_with_text(d, 330, 290, 90, 50, 'bronze.policy\n(CSV)', HexColor('#d6eaf8'), font_size=7)
box_with_text(d, 440, 290, 80, 50, 'bronze.sales\n(CSV)', HexColor('#d6eaf8'), font_size=7)
box_with_text(d, 540, 290, 80, 50, 'bronze.cars\n(CSV)', HexColor('#d6eaf8'), font_size=7)

# Metadata note
box_with_text(d, 640, 295, 100, 40, '_ingested_at\n_source_file\nDelta + lineage', HexColor('#eaf2f8'), GRAY_MED, font_size=6)

arrow_down(d, 535, 370, 340, BLUE_MED)

# SILVER layer
box_with_text(d, 60, 210, 120, 50, 'silver.customers\n- coalesce 3 ID variants\n- region standardization\n- dedup (MD5 hash)', GREEN_LIGHT, font_size=6)
box_with_text(d, 195, 210, 110, 50, 'silver.claims\n- date parsing\n- amount validation\n- type standardization', GREEN_LIGHT, font_size=6)
box_with_text(d, 320, 210, 90, 50, 'silver.policy\n- date format\n- premium check\n- FK validation', GREEN_LIGHT, font_size=6)
box_with_text(d, 425, 210, 85, 50, 'silver.sales\n- date parse\n- days_on_lot\n- price check', GREEN_LIGHT, font_size=6)
box_with_text(d, 525, 210, 85, 50, 'silver.cars\n- unit extract\n- fuel normalize\n- km validate', GREEN_LIGHT, font_size=6)

# Quarantine + DQ
box_with_text(d, 630, 215, 110, 40, 'quarantine_*\n(failed records)\ndq_issues\n(rule summaries)', RED_LIGHT, RED_DARK, font_size=6)

# Arrows bronze -> silver
for x in [135, 260, 375, 480, 580]:
    arrow_down(d, x, 290, 260, GREEN_DARK)

# GOLD layer
box_with_text(d, 60, 125, 100, 55, 'dim_customer\ndim_policy\ndim_car\ndim_date\n(Star Schema)', ORANGE_LIGHT, font_size=7, bold=True)
box_with_text(d, 175, 125, 100, 55, 'fact_claims\n(grain: 1 claim)\nSKs + measures\nprocessing_days', ORANGE_LIGHT, font_size=7, bold=True)
box_with_text(d, 290, 125, 100, 55, 'fact_sales\n(grain: 1 listing)\ncar_sk + dates\ndays_on_lot', ORANGE_LIGHT, font_size=7, bold=True)
box_with_text(d, 410, 125, 120, 55, 'claims_sla_monitor\ninventory_aging_alerts\nregulatory_readiness\nregulatory_customer_registry', YELLOW_LIGHT, font_size=6)
box_with_text(d, 555, 125, 100, 55, 'Gold Aggregations\n(pre-computed)\nCREATE OR REPLACE\non every run', YELLOW_LIGHT, YELLOW_DARK, font_size=6)

# Arrows silver -> gold
for x in [120, 250, 370, 470, 567]:
    arrow_down(d, x, 210, 180, ORANGE_DARK)

# AI + SERVING layer
box_with_text(d, 50, 48, 95, 50, 'UC1: DQ Explain\ndq_issues ->\ndq_explanation\n_report', PURPLE_LIGHT, font_size=6)
box_with_text(d, 155, 48, 95, 50, 'UC2: Anomaly\nclaims ->\nclaim_anomaly\n_explanations', PURPLE_LIGHT, font_size=6)
box_with_text(d, 260, 48, 95, 50, 'UC3: RAG\ndim_policy ->\nrag_query\n_history', PURPLE_LIGHT, font_size=6)
box_with_text(d, 365, 48, 95, 50, 'UC4: Executive\nall Gold ->\nai_business\n_insights', PURPLE_LIGHT, font_size=6)

box_with_text(d, 480, 48, 90, 50, 'Lakeview\nDashboard\n(6 pages)\nREST API', TEAL_LIGHT, TEAL_DARK, font_size=7, bold=True)
box_with_text(d, 580, 48, 80, 50, 'SQL\nWarehouse\n(serving)', BLUE_LIGHT, BLUE_DARK, font_size=7, bold=True)
box_with_text(d, 670, 48, 75, 50, 'Business\nUsers\nOps / Exec\nCompliance', HexColor('#fef9e7'), font_size=7, bold=True)

# Arrows
arrow_right(d, 570, 73, 580, TEAL_DARK)
arrow_right(d, 660, 73, 670, BLUE_MED)

# Gold -> AI
for x in [97, 202, 307, 412]:
    arrow_down(d, x, 125, 98, PURPLE_DARK)

# Gold -> Dashboard
arrow_down(d, 525, 125, 98, TEAL_DARK)

story.append(d)
story.append(Spacer(1, 4*mm))
story.append(Paragraph("Figure 1: End-to-end data flow from 4 regional source systems through medallion architecture to business users. "
    "14 source files (CSV + JSON) land in Unity Catalog Volumes, flow through Bronze (raw Delta), Silver (cleaned + quarantined), "
    "Gold (star schema + aggregations), and finally to AI use cases and Lakeview dashboard served via SQL Warehouse.", styles['Caption']))

story.append(PageBreak())


# ========================================
# 2. COMPONENT MAP
# ========================================
story.append(Paragraph("2. Component Map", styles['SectionTitle']))
story.append(Paragraph("Every tool and feature used, organized by layer and role", styles['Caption']))
story.append(hr())

comp_data = [
    ['Layer', 'Component / Tool', 'Role', 'Notebook(s)'],
    ['Landing', 'Unity Catalog Volumes', 'Governed file storage for 14 raw CSV/JSON files', '00_setup.py'],
    ['Landing', 'Git Repo (GitHub)', 'Source version control, Databricks Repos clone', '00_setup.py'],
    ['Landing', 'shutil.copy2()', 'File upload from cloned repo to Volume (serverless compatible)', '00_setup.py'],
    ['Bronze', 'PySpark CSV/JSON reader', 'Read source files with inferSchema, multiLine, header options', '01_bronze_ingestion.py'],
    ['Bronze', 'unionByName(allowMissing)', 'Merge files with different schemas into one Delta table', '01_bronze_ingestion.py'],
    ['Bronze', 'Delta Lake (overwriteSchema)', 'Schema evolution - new columns auto-added', '01_bronze_ingestion.py'],
    ['Bronze', '_ingested_at, _source_file', 'Lineage metadata columns on every Bronze row', '01_bronze_ingestion.py'],
    ['Silver', 'PySpark coalesce()', 'Unify 3+ column name variants per attribute', '02_silver_customers.py'],
    ['Silver', 'Quality rules (PySpark)', 'Per-entity boolean flag columns (_fail_*)', '02_silver_*.py'],
    ['Silver', 'Quarantine tables', 'Failed records with _quarantine_reason + timestamp', '02_silver_*.py'],
    ['Silver', 'DQ issue summaries', 'Rule-level stats (affected_ratio, severity, suggested_fix)', '02_silver_*.py'],
    ['Silver', 'MD5 hash dedup', 'Cross-region customer identity resolution', '02_silver_customers.py'],
    ['Silver', 'Region standardization', 'Map abbreviations (E->East, W->West) to canonical values', '02_silver_customers.py'],
    ['Gold', 'Star schema (Kimball)', 'dim_customer, dim_policy, dim_car, dim_date + 2 facts', '03_gold_dimensions.py'],
    ['Gold', 'Surrogate keys (row_number)', 'Integer SKs for performant joins', '03_gold_*.py'],
    ['Gold', 'Pre-computed aggregations', 'claims_sla_monitor, inventory_aging, regulatory_readiness', '03_gold_facts.py'],
    ['Gold', 'Delta Lake (CTAS)', 'CREATE OR REPLACE TABLE for idempotent refreshes', '03_gold_facts.py'],
    ['AI', 'databricks-gpt-oss-20b', 'Foundation Model endpoint via OpenAI SDK', '04_uc*.py'],
    ['AI', 'all-MiniLM-L6-v2', 'Sentence-transformer for policy document embeddings (384d)', '04_uc3.py'],
    ['AI', 'FAISS IndexFlatIP', 'In-memory vector index for cosine similarity search', '04_uc3.py'],
    ['AI', 'Tenacity retry', '5 attempts, random exponential backoff (2-60s)', '04_uc1,2,3.py'],
    ['AI', 'Pydantic validation', 'Structured output enforcement (min_length, field cleaning)', '04_uc1,2,3.py'],
    ['AI', 'extract_text() helper', 'Handles list-of-dicts response format from gpt-oss-20b', '04_uc*.py'],
    ['AI', 'PII redaction (regex)', 'Strips emails, phones, SSNs from LLM outputs', '04_uc1,2.py'],
    ['AI', 'MLflow autolog', 'Experiment tracking for prompt versions and token usage', '04_uc1,2,3.py'],
    ['Serving', 'Lakeview Dashboard', '6-page dashboard via REST API (counters, bars, tables, filters)', '06_create_dashboard.py'],
    ['Serving', 'SQL Warehouse', 'Low-latency concurrent query serving for dashboards', 'Databricks config'],
    ['Serving', 'Genie Space', 'Natural language query interface over Gold tables', 'Databricks config'],
    ['Infra', 'Databricks Workflows', 'Orchestrated job: setup->bronze->silver->gold->AI->dashboard', '00_create_pipeline.py'],
    ['Infra', 'Unity Catalog', 'Governance: catalog/schema/table/volume access control', '00_config.py'],
    ['Infra', 'Serverless compute', 'Notebook tasks run on serverless (no cluster management)', 'Job config'],
]

t = Table(comp_data, colWidths=[50, 150, 280, 130])
t.setStyle(TableStyle([
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ('FONTSIZE', (0, 0), (-1, 0), 8),
    ('FONTSIZE', (0, 1), (-1, -1), 7),
    ('TEXTCOLOR', (0, 0), (-1, 0), white),
    ('BACKGROUND', (0, 0), (-1, 0), BLUE_DARK),
    ('TEXTCOLOR', (0, 1), (-1, -1), GRAY_DARK),
    ('GRID', (0, 0), (-1, -1), 0.3, HexColor('#ddd')),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('TOPPADDING', (0, 0), (-1, -1), 3),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ('LEFTPADDING', (0, 0), (-1, -1), 4),
    # Color rows by layer
    ('BACKGROUND', (0, 1), (0, 3), GRAY_LIGHT),
    ('BACKGROUND', (0, 4), (0, 7), BLUE_LIGHT),
    ('BACKGROUND', (0, 8), (0, 13), GREEN_LIGHT),
    ('BACKGROUND', (0, 14), (0, 17), ORANGE_LIGHT),
    ('BACKGROUND', (0, 18), (0, 25), PURPLE_LIGHT),
    ('BACKGROUND', (0, 26), (0, 28), TEAL_LIGHT),
    ('BACKGROUND', (0, 29), (0, 31), GRAY_LIGHT),
]))
story.append(t)
story.append(Spacer(1, 4*mm))
story.append(Paragraph("Table 1: Complete component inventory. 32 components across 7 layers. "
    "Color coding: gray=landing, blue=bronze, green=silver, orange=gold, purple=AI, teal=serving, gray=infrastructure.",
    styles['Caption']))

story.append(PageBreak())


# ========================================
# 3. DATA QUALITY STRATEGY
# ========================================
story.append(Paragraph("3. Data Quality Strategy", styles['SectionTitle']))
story.append(Paragraph("What checks run at which layer, and what happens when they fail", styles['Caption']))
story.append(hr())

# DQ Strategy diagram
d2 = Drawing(750, 350)

# Layer boxes
layers_dq = [
    (290, 'BRONZE - Preserve & Track', BLUE_LIGHT, BLUE_DARK),
    (200, 'SILVER - Validate & Quarantine', GREEN_LIGHT, GREEN_DARK),
    (110, 'GOLD - Verify & Monitor', ORANGE_LIGHT, ORANGE_DARK),
    (20, 'AI - Explain & Alert', PURPLE_LIGHT, PURPLE_DARK),
]

for y_pos, label, bg, text_c in layers_dq:
    d2.add(Rect(0, y_pos, 750, 80, fillColor=bg, strokeColor=HexColor('#ccc'), strokeWidth=0.5))
    d2.add(String(10, y_pos + 65, label, fontSize=10, fillColor=text_c,
                  fontName='Helvetica-Bold', textAnchor='start'))

# BRONZE checks
box_with_text(d2, 20, 295, 160, 40, 'Schema merging\nunionByName(allowMissing)\n=> superset schema, NULLs for gaps', white, BLUE_DARK, font_size=7)
box_with_text(d2, 200, 295, 160, 40, 'Lineage tracking\n_ingested_at + _source_file\n=> every row traceable to origin', white, BLUE_DARK, font_size=7)
box_with_text(d2, 380, 295, 160, 40, 'Schema evolution\noverwriteSchema=true\n=> new columns auto-added', white, BLUE_DARK, font_size=7)
box_with_text(d2, 560, 295, 170, 40, 'NO transforms at Bronze\n=> raw data preserved exactly\n=> Bronze = source of truth', white, BLUE_DARK, font_size=7)

# SILVER checks
box_with_text(d2, 20, 205, 140, 55, 'Per-entity rules:\ncustomer_id_null\nregion_invalid\nstate_null\nclaim_id_null\npremium_positive ...', white, GREEN_DARK, font_size=6)
box_with_text(d2, 175, 205, 120, 55, 'Quarantine routing:\n_quality_passed = F\n=> quarantine_*\nwith _quarantine_reason\n+ _quarantined_at', white, GREEN_DARK, font_size=6)
box_with_text(d2, 310, 205, 130, 55, 'DQ summaries:\ndq_issues_{entity}\n=> rule_name, severity,\nrecords_affected,\naffected_ratio,\nsuggested_fix', white, GREEN_DARK, font_size=6)
box_with_text(d2, 455, 205, 130, 55, 'Standardization:\ncoalesce() for variants\nregion map (E->East)\ninitcap/upper/trim\ndate parsing\nunit extraction', white, GREEN_DARK, font_size=6)
box_with_text(d2, 600, 205, 135, 55, 'Deduplication:\nMD5(state|city|job|\nmarital|balance)\n=> master_customer_id\n=> resolution_audit\n~20% overlap found', white, GREEN_DARK, font_size=6)

# GOLD checks
box_with_text(d2, 20, 115, 160, 55, 'Join verification:\ncustomer match rate\npolicy match rate\ncar match rate\n=> logged per fact table', white, ORANGE_DARK, font_size=6)
box_with_text(d2, 195, 115, 150, 55, 'SLA monitoring:\nclaims_sla_monitor\navg_processing_days\nrejection_rate\nsla_breach_pct\nby region', white, ORANGE_DARK, font_size=6)
box_with_text(d2, 360, 115, 150, 55, 'Regulatory readiness:\n0-100 composite score\ncustomer completeness 40%\nDQ score 30%\nclaims efficiency 30%', white, ORANGE_DARK, font_size=6)
box_with_text(d2, 525, 115, 140, 55, 'Inventory aging:\nunsold >60 days flagged\ncross-region comparison\nredistribution signals\ndays_on_lot tracking', white, ORANGE_DARK, font_size=6)

# AI checks
box_with_text(d2, 20, 25, 170, 55, 'UC1: DQ Explanations\nsilver.dq_issues => LLM\n=> plain English reports\nfor compliance team\n5-field structured output', white, PURPLE_DARK, font_size=6)
box_with_text(d2, 205, 25, 170, 55, 'UC2: Anomaly Detection\n5 statistical rules (PySpark)\n=> 0-100 anomaly score\nHIGH/MEDIUM flagging\n=> LLM investigation briefs', white, PURPLE_DARK, font_size=6)
box_with_text(d2, 390, 25, 170, 55, 'Pydantic validation\nmin_length per field\nN/A replacement\nPII redaction (regex)\nJSON schema enforcement', white, PURPLE_DARK, font_size=6)
box_with_text(d2, 575, 25, 160, 55, 'MLflow tracking\nprompt versions\ntoken usage\nsuccess/failure rates\nper-run experiment logs', white, PURPLE_DARK, font_size=6)

story.append(d2)
story.append(Spacer(1, 4*mm))
story.append(Paragraph("Figure 2: Data quality strategy across all four layers. Bronze preserves raw data with lineage. "
    "Silver validates, quarantines failures, and standardizes. Gold verifies join integrity and computes monitoring KPIs. "
    "AI layer generates human-readable explanations and anomaly detection with Pydantic-validated outputs.",
    styles['Caption']))

story.append(Spacer(1, 6*mm))

# DQ rules table
story.append(Paragraph("Detailed quality rules by entity", styles['SubSection']))
dq_table_data = [
    ['Entity', 'Rule Name', 'What Triggers It', 'Severity', 'Action'],
    ['customers', 'customer_id_null', 'CustomerID/Customer_ID/cust_id all NULL', 'HIGH', 'Quarantine'],
    ['customers', 'region_invalid', 'Region NOT IN (East,West,North,South,Central)', 'HIGH', 'Quarantine'],
    ['customers', 'state_null', 'State IS NULL or empty', 'MEDIUM', 'Quarantine'],
    ['customers', 'cross_region_dup', 'MD5 hash matches across regions', 'INFO', 'Dedup + audit'],
    ['claims', 'claim_id_null', 'claimid IS NULL or empty', 'HIGH', 'Quarantine'],
    ['claims', 'policy_id_null', 'policyid IS NULL or empty', 'HIGH', 'Quarantine'],
    ['claims', 'incident_date_invalid', 'Unparseable date (Excel serial)', 'MEDIUM', 'Quarantine'],
    ['claims', 'negative_amounts', 'injury < 0 OR property < 0 OR vehicle < 0', 'HIGH', 'Quarantine'],
    ['policy', 'policy_number_null', 'policy_number IS NULL', 'HIGH', 'Quarantine'],
    ['policy', 'premium_positive', 'policy_annual_premium <= 0', 'MEDIUM', 'Quarantine'],
    ['policy', 'customer_id_null', 'customer_id IS NULL (FK broken)', 'HIGH', 'Quarantine'],
    ['sales', 'sales_id_null', 'sales_id IS NULL', 'HIGH', 'Quarantine'],
    ['sales', 'price_positive', 'original_selling_price <= 0', 'MEDIUM', 'Quarantine'],
    ['sales', 'days_on_lot_neg', 'Calculated days_on_lot < 0', 'MEDIUM', 'Quarantine'],
    ['cars', 'car_id_null', 'car_id IS NULL', 'HIGH', 'Quarantine'],
    ['cars', 'km_not_negative', 'km_driven < 0', 'MEDIUM', 'Quarantine'],
    ['cars', 'fuel_invalid', 'fuel NOT IN (Diesel,Petrol,CNG,LPG,Electric)', 'LOW', 'Quarantine'],
]

dq_t = Table(dq_table_data, colWidths=[65, 110, 250, 60, 70])
dq_t.setStyle(TableStyle([
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ('FONTSIZE', (0, 0), (-1, 0), 7),
    ('FONTSIZE', (0, 1), (-1, -1), 7),
    ('TEXTCOLOR', (0, 0), (-1, 0), white),
    ('BACKGROUND', (0, 0), (-1, 0), GREEN_DARK),
    ('TEXTCOLOR', (0, 1), (-1, -1), GRAY_DARK),
    ('GRID', (0, 0), (-1, -1), 0.3, HexColor('#ddd')),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('TOPPADDING', (0, 0), (-1, -1), 2),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ('LEFTPADDING', (0, 0), (-1, -1), 3),
    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [white, HexColor('#f9f9f9')]),
]))
story.append(dq_t)
story.append(Paragraph("Table 2: 17 quality rules across 5 entities. All failed records go to quarantine tables with _quarantine_reason and _quarantined_at.",
    styles['Caption']))

story.append(PageBreak())


# ========================================
# 4. GEN AI INTEGRATION
# ========================================
story.append(Paragraph("4. Gen AI Integration", styles['SectionTitle']))
story.append(Paragraph("Where each use case sits, what goes in, and what comes out", styles['Caption']))
story.append(hr())

# Gen AI diagram
d3 = Drawing(750, 380)

# Central model box
box_with_text(d3, 290, 200, 170, 50, 'databricks-gpt-oss-20b\n(Foundation Model Endpoint)\nOpenAI SDK | Serverless', PURPLE_LIGHT, PURPLE_DARK, font_size=8, bold=True)

# UC1
box_with_text(d3, 20, 310, 120, 55, 'INPUT: UC1\nsilver.dq_issues\n- rule_name\n- affected_ratio\n- severity\n- suggested_fix', BLUE_LIGHT, BLUE_DARK, font_size=7)
box_with_text(d3, 20, 120, 120, 55, 'OUTPUT: UC1\ngold.dq_explanation\n_report\n- what_was_found\n- why_it_matters\n- how_to_prevent', GREEN_LIGHT, GREEN_DARK, font_size=7)

# UC2
box_with_text(d3, 155, 310, 120, 55, 'INPUT: UC2\nsilver.claims\n+ PySpark rules\n- anomaly_score\n- 5 rule flags\n- priority tier', BLUE_LIGHT, BLUE_DARK, font_size=7)
box_with_text(d3, 155, 120, 120, 55, 'OUTPUT: UC2\ngold.claim_anomaly\n_explanations\n- what_is_suspicious\n- risk_factors\n- recommended_action', GREEN_LIGHT, GREEN_DARK, font_size=7)

# UC3
box_with_text(d3, 475, 310, 120, 55, 'INPUT: UC3\ngold.dim_policy\n=> NL documents\n=> FAISS index\n=> user question\n=> top-5 retrieval', BLUE_LIGHT, BLUE_DARK, font_size=7)
box_with_text(d3, 475, 120, 120, 55, 'OUTPUT: UC3\ngold.rag_query\n_history\n- answer\n- cited_policies\n- confidence_score', GREEN_LIGHT, GREEN_DARK, font_size=7)

# UC4
box_with_text(d3, 610, 310, 130, 55, 'INPUT: UC4\nall Gold tables\n- claims KPIs\n- customer KPIs\n- sales KPIs\n- by region/severity', BLUE_LIGHT, BLUE_DARK, font_size=7)
box_with_text(d3, 610, 120, 130, 55, 'OUTPUT: UC4\ngold.ai_business\n_insights\n- headline + findings\n- alerts + actions\n- est. impact (JSON)', GREEN_LIGHT, GREEN_DARK, font_size=7)

# Arrows to model
arrow_down(d3, 80, 310, 255, BLUE_MED)
arrow_down(d3, 215, 310, 255, BLUE_MED)
arrow_down(d3, 535, 310, 255, BLUE_MED)
arrow_down(d3, 675, 310, 255, BLUE_MED)

# Arrows from model
arrow_down(d3, 80, 200, 175, GREEN_DARK)
arrow_down(d3, 215, 200, 175, GREEN_DARK)
arrow_down(d3, 535, 200, 175, GREEN_DARK)
arrow_down(d3, 675, 200, 175, GREEN_DARK)

# Middleware boxes
box_with_text(d3, 20, 260, 120, 30, 'Persona: DQ Analyst\n15 yrs P&C insurance', HexColor('#fef9e7'), font_size=6)
box_with_text(d3, 155, 260, 120, 30, 'Persona: Fraud Investigator\n20 yrs SIU experience', HexColor('#fef9e7'), font_size=6)
box_with_text(d3, 475, 260, 120, 30, 'RAG: retrieve top-5\nall-MiniLM-L6-v2 embed', HexColor('#fef9e7'), font_size=6)
box_with_text(d3, 610, 260, 130, 30, 'Domain KPI summaries\n3 business domains', HexColor('#fef9e7'), font_size=6)

# Guardrails box
box_with_text(d3, 280, 140, 190, 45, 'Guardrails Layer\n- Pydantic validation (UC1-3)\n- PII redaction regex (UC1-2)\n- Tenacity retry 5x (UC1-3)\n- Manual retry 3x (UC4)', RED_LIGHT, RED_DARK, font_size=7, bold=True)

# MLflow
box_with_text(d3, 310, 50, 130, 35, 'MLflow Experiment Tracking\nprompt versions | token usage\nsuccess rates | per-run logs', TEAL_LIGHT, TEAL_DARK, font_size=7)

# Serving
box_with_text(d3, 20, 30, 130, 50, 'Serving:\nLakeview Dashboard\nSQL Warehouse\nGenie Space\n=> business users', TEAL_LIGHT, TEAL_DARK, font_size=7, bold=True)

# Arrow from outputs to serving
arrow_right(d3, 20, 120, 20, TEAL_DARK)
d3.add(Line(20, 90, 20, 55, strokeColor=TEAL_DARK, strokeWidth=1.5))

story.append(d3)
story.append(Spacer(1, 4*mm))
story.append(Paragraph("Figure 3: Gen AI integration architecture. All 4 use cases share the same model endpoint but use different "
    "personas, prompt structures, and output schemas. Inputs come from Silver (UC1-2) and Gold (UC3-4). "
    "All outputs write back to Gold tables. Pydantic validation, PII redaction, retry logic, and MLflow tracking "
    "form a common guardrails layer.", styles['Caption']))

story.append(Spacer(1, 8*mm))

# UC comparison table
story.append(Paragraph("Use case comparison", styles['SubSection']))
uc_data = [
    ['', 'UC1: DQ Explain', 'UC2: Anomaly', 'UC3: RAG', 'UC4: Executive'],
    ['Input', 'silver.dq_issues', 'silver.claims', 'gold.dim_policy', 'All Gold tables'],
    ['Output', 'gold.dq_explanation_report', 'gold.claim_anomaly_\nexplanations', 'gold.rag_query_history', 'gold.ai_business_insights'],
    ['Persona', 'DQ Analyst (15 yrs)', 'Fraud Investigator (20 yrs)', 'Policy Assistant', 'Senior Data Analyst'],
    ['Output fields', '5 (what/why/cause/\ndone/prevent)', '3 (suspicious/risks/\naction)', '3 (answer/policies/\nconfidence)', '5 (headline/findings/\nalerts/actions/impact)'],
    ['Retry', 'Tenacity 5x exp backoff', 'Tenacity 5x exp backoff', 'Tenacity 5x exp backoff', 'Manual 3x loop'],
    ['Validation', 'Pydantic DQExplanation', 'Pydantic InvestigationBrief', 'Pydantic RAGAnswer', 'JSON brace extraction'],
    ['PII redact', 'Yes (email/phone/SSN)', 'Yes (email/phone/SSN)', 'No (policy data only)', 'No (aggregated KPIs)'],
    ['Embedding', 'N/A', 'N/A', 'all-MiniLM-L6-v2 384d', 'N/A'],
    ['Special', 'Hash-based change detect', 'MAD z-score + IQR rules', 'FAISS cosine similarity', 'Temperature 0.3'],
]

uc_t = Table(uc_data, colWidths=[75, 135, 135, 135, 135])
uc_t.setStyle(TableStyle([
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
    ('FONTSIZE', (0, 0), (-1, -1), 7),
    ('TEXTCOLOR', (0, 0), (-1, 0), white),
    ('TEXTCOLOR', (0, 1), (0, -1), PURPLE_DARK),
    ('BACKGROUND', (0, 0), (-1, 0), PURPLE_DARK),
    ('BACKGROUND', (0, 1), (0, -1), PURPLE_LIGHT),
    ('TEXTCOLOR', (1, 1), (-1, -1), GRAY_DARK),
    ('GRID', (0, 0), (-1, -1), 0.3, HexColor('#ddd')),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('TOPPADDING', (0, 0), (-1, -1), 3),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ('LEFTPADDING', (0, 0), (-1, -1), 3),
    ('ROWBACKGROUNDS', (1, 1), (-1, -1), [white, HexColor('#f9f9f9')]),
]))
story.append(uc_t)
story.append(Paragraph("Table 3: Side-by-side comparison of all 4 Gen AI use cases. Same model, different prompt engineering, "
    "different output schemas, different guardrails.", styles['Caption']))


# Build PDF
doc.build(story)
print(f"PDF generated: {OUTPUT_PATH}")
