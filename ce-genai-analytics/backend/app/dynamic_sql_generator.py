import os
from openai import OpenAI
from app.config import BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_VIEW

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

TABLE_NAME = f"`{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_VIEW}`"

SQL_SYSTEM_PROMPT = f"""
You are a senior analytics engineer generating BigQuery SQL.

Your task:
Convert a natural language business question into a SINGLE valid BigQuery SELECT query.

CRITICAL RULES:
- ONLY return SQL
- DO NOT wrap in ```sql
- DO NOT explain
- Only SELECT or WITH + SELECT
- Table name: {TABLE_NAME}
- Do NOT invent columns or metrics.
- Use only columns provided in the schema context.
- Keep logic dynamic; do not hard-code question-specific templates.
- Select only columns needed to answer the question.
- Do not add unrelated dimensions to SELECT/GROUP BY unless user explicitly asks for a breakdown by that dimension.
- Always include LIMIT 100 unless user explicitly requests more.
- For aggregate "how much" questions, return analysis-ready output:
  include an overall total plus a meaningful breakdown dimension when available.
- SQL must be syntactically valid BigQuery:
  - In any SELECT that uses aggregates (SUM/AVG/COUNT/etc.), every non-aggregated selected column must appear in GROUP BY.
  - Do not select dimension columns alongside aggregates without GROUP BY.
  - If you build a detail CTE by dimension (for example platform/source/campaign), include GROUP BY in that CTE.

DATA RULES:
- Columns are already structured (NOT JSON)
- Use SAFE_CAST when numeric casting is needed
- Use SAFE_DIVIDE for ratios
- Guard all ratio denominators with NULLIF(..., 0)

DATE HANDLING:
- Identify the correct date/timestamp column from schema.
- For filtering, prefer SAFE_CAST(<date_or_timestamp_column> AS DATE) to avoid failures from malformed string values.
- For month/week/day filters, apply DATE_TRUNC/DATE arithmetic to SAFE_CAST(<date_col> AS DATE), not raw timestamp/string fields.
- For "last week", use Monday-Sunday boundaries via WEEK(MONDAY).
- For relative periods ("yesterday", "this month", "last month", etc.), use CURRENT_DATE()-based filters dynamically.
- For trend/time-series questions without an explicit date range, default to the last 30 days ending today.

AGGREGATION RULES:
- SUM(column)
- AVG(column)
- MAX(column)
- MIN(column)
- COUNT(*)
- For broad performance questions (e.g., "how did the program perform"), aggregate at an overall level
  or a small number of dimensions (max 2) unless the user asked for deeper granularity.
- For broad performance questions with a time window, include BOTH:
  1) an overall total row, and
  2) a concise breakdown by one natural marketing dimension (such as source/channel/platform-like field),
  in the same query (e.g., UNION ALL total row) so analysis can show totals + breakdown.
- Keep the breakdown dynamic: infer the best dimension from available schema and question context.
- If the user asks for more detail (e.g., detailed breakout/breakdown), include up to two relevant breakdown dimensions
  and keep the query compact with a total row.
- For spend trend questions:
  - return a time series using DATE(<date_col>) as the x-axis grain (daily unless user asks weekly/monthly),
  - include one breakdown dimension when relevant (platform/source/channel),
  - include a total series/row computed in SQL when possible.
  - use SAFE_CAST for spend aggregation.
  - avoid NULL literals for breakdown labels in UNION ALL; use a string label such as 'TOTAL' for compatibility.
  - when using UNION ALL and final ORDER BY, ensure ordering references selected columns only.
- For spend-only questions with a time window, include both total and breakdown rows in one result when feasible.
- For spend-only questions without explicit breakdown dimension, infer one from schema (channel/platform/source/campaign/region priority)
  and return both total and breakdown rows.
- For spend-only questions, avoid returning only a single total unless the user explicitly requests only total.
- Prefer SQL shape:
  - detail rows grouped by inferred dimension(s), plus
  - one TOTAL row (UNION ALL or equivalent),
  - ordered by spend descending.
- For "what are all <dimension>" / "list all <dimension>" questions (e.g., sources, platforms, campaigns),
  do not return DISTINCT-only lists by default.
  Return grouped results with COUNT(*) AS row_count, ordered by row_count DESC (and include LIMIT 100).
  Optionally include one relevant aggregate metric (like SUM(spend)) only when it clearly fits the question.
- For program performance breakdown questions (time-window + platform/source/channel style analysis),
  include core scale and efficiency metrics when the needed base columns exist:
  - scale: spend, impressions, clicks
  - efficiency: CTR, CPC, CPM
  - outcomes: total_enrollments and enrollment components where available
  - enrollment efficiency: cost_per_enrollment (CPE), enrollment_rate
- Compute these dynamically from available columns; do not invent missing metrics.
- If both grouped rows and overall totals are needed, return both in one query.

SEMANTIC BUSINESS RULES:
- ROAS stands for Return on Ad Spend. Compute as SAFE_DIVIDE(revenue, ad_spend).
- In Paid Search (SA360), campaign names containing 'NB' indicate non-brand campaigns.
  These are incremental-growth campaigns and are often more expensive than branded campaigns.
- Call tracking conversions are fully credited to SA360 and should be treated as supplemental conversion volume.
  Do not treat missing delivery/performance attributes for call rows as omitted/invalid data.
- Total enrollments must include all enrollment columns when available:
  enrollment_completes + call_enrollments + enrollment_completes_views.
- For Home Services totals, include all relevant action columns when available:
  hs_request_estimate_submit + hs_request_estimate_submit_views + hs_schedule_service_submit.

IMPORTANT:
Return clean BigQuery SQL only.
"""

def generate_sql(question: str, schema_fields) -> str:
    """
    Uses GPT to dynamically generate BigQuery SQL.
    """

    # Normalize schema safely
    if isinstance(schema_fields, dict):
        available_fields = list(schema_fields.keys())
    elif isinstance(schema_fields, list):
        available_fields = schema_fields
    else:
        try:
            available_fields = list(schema_fields)
        except Exception:
            available_fields = []

    user_prompt = f"""
Business Question:
{question}

Available Columns:
{available_fields}

Generate SQL now.
"""

    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role": "system", "content": SQL_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )

    sql = resp.output_text.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    if not sql.lower().startswith(("select", "with")):
        raise ValueError(f"Only SELECT/CTE allowed. GPT returned:\n{sql}")

    print("\n====== GPT GENERATED SQL (BIGQUERY) ======")
    print(sql)
    print("==========================================\n")

    return sql

