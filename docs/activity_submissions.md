# PrimeInsurance POC — Activity Submissions

This document contains the write-ups for all three linked activities:
- **Activity A**: Activating the Gold Layer (serving layer)
- **Activity B**: Databricks Application integration
- **Activity C**: Genie Space + integration

Each section maps directly to a text-area field in the submission.

---

## Activity A — Activating the Gold Layer

### Q: What additional context would make the data more useful to the business user? What column did you add, and why did you choose it for your use case?

The column we added is **`aging_bucket`** in the view
`primeins_lakebase.primeins.public.fact_sales_enriched`. It bucketizes every
unsold car listing by `days_listed` into four business-meaningful states:

| `days_listed` | `aging_bucket` | Business meaning |
|---|---|---|
| `< 30` | Fresh | Normal, no action |
| `30–59` | Watch | Monitor, consider minor reprice |
| `60–89` | Stale | Price cut, cross-region promotion |
| `≥ 90` | Critical | Must act — carrying cost compounding |
| `NULL` | Unknown | Data quality issue |

We chose it because the problem statement explicitly named the **sales head
monitoring aging inventory** as one of the three stuck users. In the raw Gold
layer she would have to run SQL like
`SELECT … WHERE days_listed > 60 AND sold_on IS NULL ORDER BY days_listed DESC`
— that's a technical ask she can't do. With `aging_bucket` as a first-class
column, a Databricks App (or a Genie question) can show her the Critical
bucket immediately. On the current data the split is:

- Fresh: 1,572
- Watch: 100
- Stale: 9
- Critical: 167

That's 176 cars she can see within seconds of opening the app.

### Q: How did you add it — walk through what you did in the serving layer?

Because the synced table is managed by Databricks and cannot be altered
directly, we added the column via a **Postgres view** on top of the synced
`public.fact_sales` table:

```sql
CREATE OR REPLACE VIEW public.fact_sales_enriched AS
SELECT
  *,
  CASE
    WHEN days_listed IS NULL THEN 'Unknown'
    WHEN days_listed < 30     THEN 'Fresh'
    WHEN days_listed < 60     THEN 'Watch'
    WHEN days_listed < 90     THEN 'Stale'
    ELSE                           'Critical'
  END AS aging_bucket
FROM public.fact_sales;
```

The view is created from the `40_lakebase_direct_load.py` notebook (cells 5–6)
immediately after the base tables are loaded. It runs on every refresh, so
if the underlying `days_listed` values change, the bucket labels recompute
automatically at query time.

### Q: What does this tell you about how the serving layer behaves differently from your Gold Delta tables?

Three concrete differences we hit while building this:

1. **Writability model.** The Gold Delta tables are managed by DLT and
   materialized views — we cannot add a column to them without modifying the
   pipeline. Lakebase is a real Postgres instance, so we get the full
   DDL toolbox: views, indexes, stored procedures, grants per role. The view
   approach is only possible because Lakebase supports standard SQL.

2. **Primary-key constraints are enforced.** When we loaded `fact_sales`
   directly into Lakebase, the write failed with
   `UniqueViolation: duplicate key value violates unique constraint
   "fact_sales_pkey"`. The Gold table has 1,849 rows but only 1,848
   unique `sales_id`s (one duplicate across source files). Delta doesn't
   enforce primary keys so this was invisible in Gold. Postgres does,
   which surfaced a real data-quality issue we now have to resolve.

3. **Connection semantics.** The Gold Delta tables are queryable via SQL
   warehouses but not via a standard Postgres driver — no `psycopg2`, no
   JDBC from a Node app, no connection pooling tailored to OLTP. Lakebase
   speaks the Postgres wire protocol, so any language's Postgres client
   (psycopg2, JDBC, pgx, Rails, Prisma) can connect directly. This is what
   makes it suitable as the "serving layer between Gold and a live app".

---

## Activity A — Additional write-up: serving layer choice and sync

### Which serving layer you identified within Databricks and why

**Lakebase** — Databricks' managed Postgres-compatible OLTP database. It sits
natively inside the workspace, is governed by Unity Catalog, and provides the
one thing Delta tables cannot: **a standard Postgres connection string** that
an application can talk to with any off-the-shelf Postgres driver. It also
supports transactional row-level operations and sub-second point lookups, the
patterns that a live app fires repeatedly as users interact with screens.

### How we synced data from Gold into Lakebase

Initial attempt was **Databricks managed synced tables** via
`databricks database create-synced-database-table`. That's the "correct"
production answer: incremental sync via Delta Change Data Feed, managed
pipeline, no manual orchestration. It failed in our workspace because the
sync pipeline's provisioned cluster hit a Unity Catalog deployment error
(error class INVAL… for all 6 tables). We also discovered that DLT
materialized views can only sync in SNAPSHOT mode, not TRIGGERED or CONTINUOUS.

We pivoted to a **direct-load notebook** (`40_lakebase_direct_load.py`) that
uses pure Python with `psycopg2`, reads each Gold table via Spark, and bulk-
inserts with `execute_values`. For our POC scale (1,000–2,500 rows per table,
~8,000 total) the whole load takes ~30 seconds. The notebook also creates
the `fact_sales_enriched` view in the same transaction.

### How we would keep the sync current

Short-term (what we have now): schedule
`PrimeInsurance_Serving_Lakebase_Load` as a downstream task of
`primeins_end_to_end` so every Gold refresh triggers a serving-layer
refresh. Full overwrite each run, idempotent, ~30 seconds.

Medium-term: when the underlying synced-tables UC issue is resolved, swap
to managed synced tables in TRIGGERED mode. We can do this by pointing the
sync at regular Delta copies of the Gold MVs (which we already created under
`primeins.serving.*` as part of this activity).

Long-term: migrate the `fact_sales` primary-key dedup upstream to Silver so
the full Gold MV loads without any row loss. Today Lakebase has 1,848 rows
vs Gold's 1,849 because of the duplicate `sales_id`.

---

## Activity B — Integration Approach

### Q: Did you embed the Genie UI or call the Genie API? Why?

We **called the Genie API** from a Databricks App built with Streamlit. The
decision:

- **Embedding the Genie UI in an iframe** is possible and simpler, but it
  surfaces the full Genie chrome to the end user. For a compliance officer
  or sales head, we want a branded, focused experience that hides the Genie
  interface behind a single text box.
- **Calling the API directly** lets us control the layout: we can show our
  own KPI strip at the top (from Lakebase), render the Genie answer as
  formatted Markdown, and then show the underlying data as a clean
  `st.dataframe` — instead of the default Genie panel layout.
- Calling the API also lets us handle errors gracefully. When Genie returns
  a clarification prompt ("Did you mean X or Y?"), we show it as an inline
  hint. When Genie throws, we show a friendly message instead of a stack
  trace.

### Q: What steps did you follow to set up the integration?

1. Created the Databricks App shell via
   `databricks apps create --name primeins-intelligence`.
2. Wrote a Streamlit app (`apps/primeins_intelligence/app.py`) with
   three sections: KPI strip, question box + sample dropdown, and a
   conversation history panel.
3. The app authenticates to the workspace via the built-in App identity —
   the `databricks-sdk` `WorkspaceClient()` automatically picks up the
   credentials. No secrets, no tokens in code.
4. The app generates a short-lived OAuth token for Lakebase on first
   request (cached with `@st.cache_resource(ttl=2400)`) and uses it to
   open a `psycopg2` connection for the KPI queries.
5. For Genie, the app calls `w.genie.start_conversation_and_wait()` for
   the first turn and `w.genie.create_message_and_wait()` for follow-ups,
   preserving the conversation ID in `st.session_state` so multi-turn
   dialogue works.
6. Deployed via `databricks apps deploy primeins-intelligence
   --source-code-path /Workspace/.../primeins-intelligence --mode SNAPSHOT`.
7. Granted the app's service principal `CAN_USE` on the SQL warehouse,
   `USE CATALOG + USE SCHEMA + SELECT` on `primeins.gold`, `CAN_RUN` on
   the Genie space, and created a matching Postgres role in Lakebase with
   `SELECT` on `public.*`.

### Q: How did you handle errors when Genie couldn't answer?

Two paths:

1. **Soft failures** (Genie returns a clarification like "Did you mean X
   instead of Y?"): the app renders the clarification text prominently,
   along with any SQL Genie did generate. The user sees both the question
   Genie is asking back AND the draft SQL, so they can refine and re-ask.
2. **Hard failures** (API exception, timeout, auth error): wrapped in a
   `try/except` that shows `st.error()` with a plain-English message and
   tells the user to either rephrase or click "Reset conversation". No
   stack trace reaches the user.

The KPI strip also has its own try/except: if Lakebase is unreachable, it
shows a warning but does not block Genie from working.

### Q: What was the hardest part of the integration?

**The Lakebase sync pipeline failing** was the single biggest time sink. We
burned ~45 minutes debugging synced tables that wouldn't provision, then
pivoted to the direct-load approach. The hard part wasn't the Python code —
that took 15 minutes — it was recognizing when to stop fighting the managed
feature and switch to a workaround that reliably worked.

The second hardest was **primary-key enforcement** in Postgres exposing a
data-quality issue (duplicate `sales_id` in Gold) that was invisible on
Delta. We fixed it with a `dropDuplicates` at load time and documented
the dedup in the serving layer write-up. That's actually a feature, not a
bug — the serving layer flagged something the analytical layer couldn't.

---

## Activity C — Decision Rationale (table selection)

### Q: Which tables did you choose and why?

We exposed **5 tables** to the Genie Space:

1. **`primeins.gold.dim_customer`** — the regulatory story (1,604
   deduplicated from 3,605). Any question about "how many customers"
   resolves here.
2. **`primeins.gold.dim_policy`** — coverage tiers, premium bands,
   umbrella coverage. Foreign key target for fact_claims.
3. **`primeins.gold.fact_claims`** — the backlog story (claims,
   severity, rejection rate, fraud amounts).
4. **`primeins.gold.fact_sales`** — the inventory story (unsold cars,
   aging, selling velocity).
5. **`primeins.gold.dim_car`** — model/make reference so sales questions
   can say "which **models** are aging" instead of "which car_ids".

### Q: Which tables did you exclude and why?

1. **`dim_region`** — 5-row reference table. Region semantics already live
   on `dim_customer` and `fact_sales`. Including it added noise without
   teaching Genie anything new.
2. **`mv_rejection_rate_by_policy` / `mv_claims_by_severity` /
   `mv_unsold_inventory` / `mv_claims_by_region`** — these are
   pre-aggregated MVs that Genie can compute itself from the base fact
   tables. Exposing them creates duplicate answer paths (Genie might pick
   the MV for one question and the base table for a similar question) and
   confuses table selection.
3. **AI output tables** (`claim_anomaly_explanations`,
   `dq_explanation_report`, `ai_business_insights`,
   `rag_query_history`, `rag_query_history_vs`) — these are derived
   insight artifacts, not canonical data. If Genie picks them, users get
   meta-answers about model output instead of business answers.
4. **Silver quality tables** (`dq_issues`, `quarantine_*`,
   `dq_*_metrics`) — these belong to a compliance audience and would be
   the right tables for a **separate Regulator Genie Space** (on the
   Phase 2 roadmap). Mixing them with the business tables confuses both
   audiences.
5. **Phase 1.5 gold quality rollups** (`dq_metrics_daily`,
   `dq_quarantine_by_table`, `dq_resolutions_summary`) — same reason.
   Compliance-domain. Belongs in the future regulator space.

### Q: How did you decide what would work best for natural language queries?

Three heuristics:

1. **One table per business concept.** For "customers", "policies",
   "claims", "inventory", "vehicles" — pick the one canonical source of
   truth and exclude the rest.
2. **Prefer base facts over pre-aggregated views.** Genie can aggregate;
   Genie cannot un-aggregate. Giving it the base data is more flexible.
3. **Match the persona to the table.** The three personas from the problem
   statement (compliance, claims, sales) map cleanly to
   dim_customer + fact_claims + fact_sales. Everything else is noise for
   those personas.

We also wrote **12 example SQL queries** in the instructions — those
teach Genie the exact join pattern for each question style, and are more
effective than text instructions at steering answer generation.

### Q: If you had to do it again, would you make the same choices?

Yes, with one small tweak: the initial 3 questions (Q1, Q4, Q6) returned
**clarification prompts** instead of direct answers ("Would you like X
instead of Y?"). That's technically graceful behavior — Genie asks when
intent is ambiguous — but for the demo we want the common questions to
answer directly on first try. The refinement is to add more specific
example SQL titles for those question patterns, so the embedding match is
stronger and the path from question to answer is less ambiguous. Details in
the Testing & Accuracy section below.

---

## Activity C — Testing & Accuracy

### Q: What were your 10 test questions?

| # | Question | Expected behavior |
|---|---|---|
| 1 | Which region has the highest claim volume? | Group fact_claims by region (via dim_policy + dim_customer), COUNT, ORDER BY DESC |
| 2 | What is the claim rejection rate by incident severity? | Direct aggregate, no joins |
| 3 | How many customers do we have? | `COUNT(*)` from `dim_customer` = 1,604 |
| 4 | Show me customers with more than 5 claims | Three-way join + HAVING |
| 5 | Which cars are aging in inventory? | fact_sales + dim_car join, sold_on IS NULL, days_listed > 60 |
| 6 | Which region has the worst aging inventory problem? | fact_sales GROUP BY region with Critical count |
| 7 | How many policies are in each premium tier? | dim_policy with the Basic/Standard/Premium CASE buckets |
| 8 | Show me claims over $10,000 | Direct filter on fact_claims |
| 9 | Which car models sell fastest? | fact_sales + dim_car, sold_on IS NOT NULL, AVG(days_listed) |
| 10 | How long do claims take to process? | TRICK — dates are corrupted; should return canned refusal |

### Q: How many did Genie answer correctly on the first try?

**All 10 returned COMPLETED status** within an average of 12 seconds.
However, **3 of the 10** (Q1, Q4, Q6) returned **clarification prompts**
rather than direct answers on first attempt:

- **Q1** → "Would you like to see which region has the highest total claim
  amount instead of the highest number of claims?" (Genie wasn't sure
  whether "highest volume" meant count or dollar value)
- **Q4** → "Would you like to see customers with 5 or more claims instead
  of strictly more than 5?" (off-by-one ambiguity)
- **Q6** → "Would you prefer to see the worst aging inventory by average
  days listed or total unsold listings instead?" (metric ambiguity)

The other 7 (Q2, Q3, Q5, Q7, Q8, Q9, Q10) returned correct answers
directly. Notable wins:

- **Q3** → "1,604 customers... deduplicated customer records" (the
  regulatory headline, correct)
- **Q7** → "153 Basic, 692 Standard, 154 Premium" (correct thresholds
  from instructions)
- **Q9** → "Nissan 10 days, Lexus 11, Fiat 11.8, BMW 12.4, Volkswagen
  12.5" (correct AVG days_to_sell)
- **Q10** → **Genie refused correctly** with the canned response from our
  text instructions: *"Claim processing dates are corrupted at the source
  — only time portions survived. The platform cannot compute real
  processing duration from fact_claims..."* That's the trick question
  working as intended.

**Score: 7/10 direct answers, 3/10 clarifications. 0/10 wrong answers.**

### Q: For questions that failed, what was the issue?

None of the 10 failed outright. The 3 clarifications are actually a
feature, not a failure — Genie asked rather than guessed. But for the
demo we'd prefer direct answers. Root cause analysis for each:

- **Q1** ("highest claim volume"): "volume" is overloaded in English —
  could mean count or dollar amount. Our example SQL used count, but the
  example title didn't exactly match "volume".
- **Q4** (">5 claims"): Genie second-guessed whether the user wanted
  inclusive or exclusive. Our example SQL used strict `>` but the title
  said "more than 5" which is inclusive in casual English.
- **Q6** ("worst aging inventory problem"): "worst" is unspecified — by
  count, by average, by total?

### Q: How did you improve Genie's accuracy?

Two concrete refinements based on the test results:

1. **Renamed example SQL for Q1** from "Which region has the highest
   claim volume?" to
   **"Which region has the most claims (count of claims by customer region)?"**
   — using the same aggregation terms the SQL actually computes. This
   should make the embedding match crisper when the user asks about
   "claim volume".

2. **Renamed example SQL for Q6** from "Which region has the worst aging
   inventory problem?" to
   **"Which region has the most unsold cars over 90 days (Critical bucket)?"**
   — explicit metric and explicit threshold so there's nothing left to
   clarify.

After these edits we re-ran Q1 and Q6. Both returned direct answers on
second attempt. Full retest: 9/10 direct, 1/10 clarification (Q4 still
clarifies the off-by-one, which is legitimately ambiguous and we
deliberately kept the clarification).

---
