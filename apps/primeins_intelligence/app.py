"""
PrimeInsurance Data Intelligence — Databricks App

Panel demo target. Three things happen in one screen:

1. LIVE KPIs from the Lakebase serving layer (not Gold Delta directly).
   A compliance officer can open this and immediately see the deduplicated
   customer count, claim backlog, and aging inventory counts without
   navigating to a dashboard.

2. NATURAL LANGUAGE Q&A via the PrimeInsurance Genie Space.
   The user types a question in plain English. The app calls the Genie API,
   waits for the answer, and renders both the natural language response AND
   the underlying SQL result as a data table.

3. ERROR HANDLING when Genie can't answer. If the response contains a
   clarification prompt instead of data, the app shows the clarification with
   guidance on how to rephrase. If the API call itself fails, the app shows
   a friendly message instead of a stack trace.

Target end users: compliance officer, claims manager, sales head — none of
whom know SQL. The point is to make the Gold layer talk back to them.
"""

import os
import uuid
import pandas as pd
import psycopg2
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal

# ============================================================
# CONFIG
# ============================================================

GENIE_SPACE_ID = os.environ.get(
    "GENIE_SPACE_ID", "01f133db605b1751ae383aa34ce99dce"
)
# Databricks Apps with a `database` resource binding get these env vars
# injected automatically. Local defaults only for dev.
LAKEBASE_INSTANCE = os.environ.get(
    "LAKEBASE_INSTANCE_NAME", "primeins-lakebase"
)
LAKEBASE_HOST = os.environ.get(
    "LAKEBASE_HOST",
    "ep-late-tooth-d8dc2eg6.database.us-east-2.cloud.databricks.com",
)
LAKEBASE_DB = os.environ.get("LAKEBASE_DATABASE", "primeins")

# Databricks Apps inject DATABRICKS_HOST / CLIENT_ID / CLIENT_SECRET when the
# app has a workspace identity. We use OAuth M2M directly so the SP token is
# scoped correctly for the database resource binding.
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_CLIENT_ID = os.environ.get("DATABRICKS_CLIENT_ID", "")
DATABRICKS_CLIENT_SECRET = os.environ.get("DATABRICKS_CLIENT_SECRET", "")

st.set_page_config(
    page_title="PrimeInsurance Data Intelligence",
    page_icon="🏢",
    layout="wide",
)

# ============================================================
# CACHED CLIENTS
# ============================================================


@st.cache_resource
def get_workspace_client() -> WorkspaceClient:
    """SDK client authenticated via the app's built-in identity."""
    return WorkspaceClient()


def _fresh_lakebase_connection():
    """Open a NEW psycopg2 connection to Lakebase using a database-scoped
    credential generated specifically for this instance.

    The generic OAuth M2M access token (from cfg.authenticate()) is rejected
    by Lakebase because it's a workspace-API token, not a database-scoped
    token. The Databricks Database API has a dedicated endpoint for
    generating credentials that Lakebase Postgres will accept as passwords:
    `POST /api/2.0/database/instances/credentials`.

    When the app has a `database` resource binding with CAN_CONNECT_AND_CREATE
    permission, the app's SP is authorized to call this endpoint for the
    bound instance. The returned token is what Postgres accepts.
    """
    if not DATABRICKS_CLIENT_ID or not DATABRICKS_CLIENT_SECRET:
        raise RuntimeError(
            "DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET not present in env. "
            "This app isn't running inside Databricks Apps, or the Lakebase "
            "resource binding is missing."
        )

    host_url = DATABRICKS_HOST
    if not host_url.startswith("http"):
        host_url = f"https://{host_url}"

    # Create a SP-authenticated WorkspaceClient explicitly. Auto-discovery
    # from env vars should do the same, but being explicit avoids any
    # ambiguity about which identity is making the call.
    cfg = Config(
        host=host_url,
        client_id=DATABRICKS_CLIENT_ID,
        client_secret=DATABRICKS_CLIENT_SECRET,
    )
    w = WorkspaceClient(config=cfg)

    cred = w.database.generate_database_credential(
        instance_names=[LAKEBASE_INSTANCE],
        request_id=f"app-{uuid.uuid4().hex[:8]}",
    )
    db_token = cred.token

    # Decode the JWT payload (no verification — just inspection) so we can
    # surface sub/aud/scope in the diagnostic panel if auth fails.
    import base64, json as _json
    try:
        parts = db_token.split(".")
        if len(parts) >= 2:
            pad = "=" * (4 - len(parts[1]) % 4)
            jwt_payload = _json.loads(base64.urlsafe_b64decode(parts[1] + pad))
            st.session_state["_jwt_payload"] = jwt_payload
    except Exception:
        pass

    try:
        conn = psycopg2.connect(
            host=LAKEBASE_HOST,
            port=5432,
            dbname=LAKEBASE_DB,
            user=DATABRICKS_CLIENT_ID,
            password=db_token,
            sslmode="require",
        )
    except Exception:
        # Attach diagnostic info to the exception chain via session_state
        st.session_state["_attempted_user"] = DATABRICKS_CLIENT_ID
        raise
    conn.autocommit = True
    return conn, DATABRICKS_CLIENT_ID


def query_lakebase(sql: str) -> pd.DataFrame:
    """Run a read-only SQL query against the Lakebase serving layer."""
    conn, _ = _fresh_lakebase_connection()
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


# ============================================================
# HEADER & KPI STRIP (from Lakebase — the serving layer)
# ============================================================

st.title("🏢 PrimeInsurance Data Intelligence")
st.caption(
    "Live view over the Gold layer via the Lakebase serving layer. "
    "Ask questions in plain English below."
)

try:
    kpi_sql = """
        SELECT
          (SELECT COUNT(*) FROM public.dim_customer)                 AS customers,
          (SELECT COUNT(*) FROM public.fact_claims)                  AS claims,
          (SELECT COUNT(*) FROM public.fact_claims WHERE is_rejected) AS rejected,
          (SELECT COUNT(*) FROM public.fact_sales_enriched
             WHERE aging_bucket IN ('Stale','Critical'))            AS aging_inventory
    """
    kpi = query_lakebase(kpi_sql).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Unique customers", f"{kpi['customers']:,}", help="Deduplicated from 3,605 raw rows across 7 source files")
    c2.metric("Total claims", f"{kpi['claims']:,}")
    c3.metric("Rejected claims", f"{kpi['rejected']:,}", help=f"{round(kpi['rejected']*100/max(kpi['claims'],1),1)}% rejection rate")
    c4.metric("Aging inventory", f"{kpi['aging_inventory']:,}", help="Stale (60–90d) + Critical (>90d) unsold listings from fact_sales_enriched")
except Exception as exc:
    import traceback
    with st.expander("⚠️  Lakebase KPIs unavailable — click for diagnostic details", expanded=True):
        st.warning(
            f"Connection error: `{exc}`\n\n"
            "The app can still answer Genie questions — Genie queries the SQL warehouse directly."
        )
        try:
            w = get_workspace_client()
            me = w.current_user.me()
            st.caption(
                f"me.user_name: `{me.user_name}` · "
                f"me.id: `{getattr(me,'id','?')}` · "
                f"me.display_name: `{getattr(me,'display_name','?')}` · "
                f"me.active: `{getattr(me,'active','?')}`"
            )
        except Exception:
            pass

        # Show the JWT payload of the DB credential token (if generated)
        jwt_payload = st.session_state.get("_jwt_payload")
        if jwt_payload:
            st.write("**JWT payload from `generate_database_credential`:**")
            st.json(jwt_payload)
            st.caption(
                f"psycopg2 user= parameter sent: `{st.session_state.get('_attempted_user','?')}`"
            )
            st.caption(
                "If `sub` in the JWT does not match the `user=` parameter, "
                "that is the auth mismatch."
            )

        # Dump env vars related to Databricks / Lakebase
        import os
        interesting_keys = sorted([
            k for k in os.environ.keys()
            if any(k.startswith(p) for p in
                   ("DATABRICKS", "PG", "DB_", "LAKEBASE",
                    "DATABASE", "APP_", "POSTGRES"))
        ])
        st.write("**Injected environment variables visible to the app:**")
        env_rows = []
        for k in interesting_keys:
            v = os.environ[k]
            disp = v if len(v) <= 80 else v[:40] + "…" + v[-20:]
            env_rows.append({"var": k, "value": disp})
        if env_rows:
            st.dataframe(pd.DataFrame(env_rows), use_container_width=True, hide_index=True)
        else:
            st.caption("(none found with these prefixes)")

        st.code(traceback.format_exc(), language="python")

st.divider()

# ============================================================
# GENIE Q&A
# ============================================================

st.subheader("Ask a question in plain English")

SAMPLE_QUESTIONS = [
    "Which region has the highest claim volume?",
    "What is the claim rejection rate by incident severity?",
    "How many customers do we have?",
    "Show me customers with more than 5 claims",
    "Which cars are aging in inventory?",
    "Which car models sell fastest?",
    "How many policies are in each premium tier?",
    "Show me claims over $10,000",
]

# Session state to remember the conversation
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None
if "history" not in st.session_state:
    st.session_state.history = []

col_q, col_reset = st.columns([5, 1])
with col_q:
    sample = st.selectbox(
        "Quick starters (or type your own below)",
        options=[""] + SAMPLE_QUESTIONS,
        index=0,
    )
    question = st.text_input(
        "Your question",
        value=sample if sample else "",
        placeholder="e.g. Which region has the highest claim volume?",
    )
with col_reset:
    st.write("")
    st.write("")
    if st.button("🔄 Reset conversation"):
        st.session_state.conversation_id = None
        st.session_state.history = []
        st.rerun()

ask = st.button("Ask Genie", type="primary")

# ============================================================
# GENIE CALL + RENDERING
# ============================================================


def render_message(msg_resp, is_first: bool):
    """Render a Genie message response as text answer + data table + SQL."""
    msg = msg_resp.message if hasattr(msg_resp, "message") else msg_resp
    # Attachments can hold either a free-text answer or a SQL query result
    text_block = None
    sql_block = None
    query_attachment_id = None
    attachments = msg.attachments or []
    for att in attachments:
        if att.text and att.text.content:
            text_block = att.text.content
        if att.query and att.query.query:
            sql_block = att.query.query
            query_attachment_id = att.attachment_id

    # Natural language answer
    if text_block:
        st.markdown(text_block)
    elif not sql_block:
        st.info("Genie didn't return an answer — try rephrasing the question.")
        return

    # Underlying data (query result)
    if query_attachment_id and sql_block:
        try:
            w = get_workspace_client()
            conv_id = (
                st.session_state.conversation_id
                or getattr(msg_resp, "conversation_id", None)
                or getattr(msg, "conversation_id", None)
            )
            result = w.genie.get_message_attachment_query_result(
                space_id=GENIE_SPACE_ID,
                conversation_id=conv_id,
                message_id=msg.id,
                attachment_id=query_attachment_id,
            )
            sr = result.statement_response
            cols = [c.name for c in sr.manifest.schema.columns]
            rows = sr.result.data_array or []
            if rows:
                df = pd.DataFrame(rows, columns=cols)
                st.dataframe(df, use_container_width=True, hide_index=True)
                st.caption(f"{len(df):,} rows returned")
            else:
                st.caption("(Query executed but returned no rows.)")
        except Exception as exc:
            st.warning(f"Could not fetch query result: {exc}")

        with st.expander("Show SQL used by Genie"):
            st.code(sql_block, language="sql")


if ask and question:
    w = get_workspace_client()
    with st.spinner("Genie is thinking..."):
        try:
            if st.session_state.conversation_id is None:
                # Start a new conversation
                resp = w.genie.start_conversation_and_wait(
                    space_id=GENIE_SPACE_ID,
                    content=question,
                )
                st.session_state.conversation_id = resp.conversation_id
            else:
                # Continue existing conversation
                resp = w.genie.create_message_and_wait(
                    space_id=GENIE_SPACE_ID,
                    conversation_id=st.session_state.conversation_id,
                    content=question,
                )
            st.session_state.history.append(
                {"q": question, "resp": resp}
            )
        except Exception as exc:
            st.error(
                f"Genie call failed: {exc}\n\n"
                "Try a simpler question, or click 'Reset conversation' and start over."
            )
            resp = None

# Render the history newest-first
for i, entry in enumerate(reversed(st.session_state.history)):
    with st.container(border=True):
        st.markdown(f"**🗣 {entry['q']}**")
        render_message(entry["resp"], is_first=(i == 0))

# ============================================================
# FOOTER
# ============================================================

st.divider()
st.caption(
    f"Genie Space: `{GENIE_SPACE_ID}` · "
    f"Serving layer: Lakebase `{LAKEBASE_INSTANCE}` · "
    f"Built on the PrimeInsurance Gold layer · Phase 2 demo"
)
