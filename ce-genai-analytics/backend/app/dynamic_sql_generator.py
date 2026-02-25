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
- Do NOT invent new metrics.
- Only use columns that exist in schema.
- Do NOT redefine conversions.
- If user asks for performance, prioritize:
    spend, clicks, impressions, CTR,
    total_enrollments, cost_per_enrollment, enrollment_rate.
- For generic "program performance" questions, aggregate at platform + datasource level by default.
- Do NOT group by campaign unless user explicitly asks for campaign-level output.


DATA RULES:
- Columns are already structured (NOT JSON)
- Use SAFE_CAST(column AS FLOAT64) when needed
- Use SAFE_DIVIDE for ratios
- Never use INT casts unless necessary
- Prefer FLOAT64

DATE HANDLING:
- Date column name: Date
- Use:
    EXTRACT(QUARTER FROM Date)
    EXTRACT(YEAR FROM Date)
- For week logic, weeks must run Monday-Sunday.
- Always use explicit Monday-based week functions in BigQuery:
    DATE_TRUNC(Date, WEEK(MONDAY))
    EXTRACT(WEEK(MONDAY) FROM Date)
- Do NOT use default WEEK behavior that starts on Sunday.
- For relative weekly windows, use these exact boundaries:
    this_week_start = DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
    last_week_start = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 7 DAY)
    last_week_end = DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)
- If user asks "last week", filter:
    Date BETWEEN last_week_start AND last_week_end
- If user asks "this week", filter:
    Date BETWEEN this_week_start AND CURRENT_DATE()
- If user asks "yesterday", filter:
    Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
- If user asks "last month", filter:
    Date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
    AND Date < DATE_TRUNC(CURRENT_DATE(), MONTH)
- If user asks "this month" or "month-to-date", filter:
    Date BETWEEN DATE_TRUNC(CURRENT_DATE(), MONTH) AND CURRENT_DATE()
- Never use WEEK without MONDAY in DATE_TRUNC/EXTRACT.

AGGREGATION RULES:
- SUM(column)
- AVG(column)
- MAX(column)
- MIN(column)
- COUNT(*)

DERIVED METRICS:
- CTR = SAFE_DIVIDE(SUM(Clicks), SUM(Impressions)) * 100
- total_enrollments = (
    SAFE_CAST(SUM(enrollment_completes) AS FLOAT64)
    + SAFE_CAST(SUM(call_enrollments) AS FLOAT64)
    + SAFE_CAST(SUM(enrollment_completes_views) AS FLOAT64)
  )
- cost_per_enrollment = SAFE_DIVIDE(SUM(spend), total_enrollments)
- enrollment_rate = SAFE_DIVIDE(total_enrollments, SUM(clicks)) * 100

PROGRAM PERFORMANCE QUERY SHAPE (IMPORTANT):
- For "how did the program perform last week" and similar weekly performance asks:
  1) Build a base CTE aggregated by platform, datasource.
  2) Build a detail CTE that computes ctr, cpc, cpm, total_enrollments, cpe.
  3) Return detail rows UNION ALL a TOTAL row ("TOTAL" platform, "ALL" datasource) where totals are recomputed from SUMs.
  4) Include spend, impressions, clicks, ctr, cpc, cpm,
     enrollment_completes, call_enrollments, enrollment_completes_views,
     total_enrollments, cpe.
  5) Apply Monday-Sunday last-week bounds.
  6) LIMIT 100.
- Prefer explicit casts in aggregates:
    SUM(SAFE_CAST(spend AS NUMERIC)),
    SUM(SAFE_CAST(impressions AS INT64)),
    SUM(SAFE_CAST(clicks AS INT64)).
- Use NULLIF(denominator, 0) inside SAFE_DIVIDE for ratio safety.

SPEND BREAKDOWN SHAPE (IMPORTANT):
- For spend-only questions with a time window (e.g., "how much did we spend yesterday/last week/last month"),
  return a dynamic breakdown and total in one query:
  1) Aggregate detail by platform, datasource with SUM(CAST(spend AS NUMERIC)) AS spend.
  2) UNION ALL a TOTAL row: platform='TOTAL', datasource='ALL', SUM(spend).
  3) ORDER BY spend DESC and LIMIT 100.
- If business line is mentioned (e.g., Energy), include that filter.
- Keep this dynamic: infer the time range from the question; do not hardcode specific dates.
- If user asks for a specific source/platform (e.g., Meta/Facebook/Instagram),
  include source filtering in WHERE using case-insensitive logic, e.g.:
    LOWER(COALESCE(datasource, '')) IN ('facebook','meta','instagram')
    OR LOWER(COALESCE(platform, '')) LIKE '%meta%'
    OR LOWER(COALESCE(platform, '')) LIKE '%facebook%'
    OR LOWER(COALESCE(platform, '')) LIKE '%instagram%'
- For source-specific spend asks ("how much was spent on meta this month"),
  return a single summarized row with spend and explicit start/end dates:
    SELECT '<label>' AS period, start_date, end_date, SUM(spend) AS spend ...

IMPORTANT:
Return clean BigQuery SQL only.
"""

def generate_sql(question: str, schema_fields) -> str:
    """
    Uses GPT to dynamically generate BigQuery SQL.
    """

    q = (question or "").strip().lower()

    if (
        any(k in q for k in ["spend", "spent", "spending", "cost"])
        and any(k in q for k in ["meta", "facebook", "instagram"])
        and any(k in q for k in ["this month", "month-to-date", "mtd"])
    ):
        return f"""SELECT
  'Meta (Facebook) MTD' AS period,
  DATE_TRUNC(CURRENT_DATE(), MONTH) AS start_date,
  CURRENT_DATE() AS end_date,
  SUM(SAFE_CAST(spend AS NUMERIC)) AS spend
FROM {TABLE_NAME}
WHERE DATE(date) BETWEEN DATE_TRUNC(CURRENT_DATE(), MONTH) AND CURRENT_DATE()
  AND (
    LOWER(COALESCE(datasource, '')) IN ('facebook', 'meta', 'instagram')
    OR LOWER(COALESCE(platform, '')) LIKE '%meta%'
    OR LOWER(COALESCE(platform, '')) LIKE '%facebook%'
    OR LOWER(COALESCE(platform, '')) LIKE '%instagram%'
  )
LIMIT 100"""

    if any(k in q for k in ["all sources", "what are all the sources", "list sources", "data sources", "datasource"]):
        return f"""SELECT
  datasource,
  COUNT(*) AS row_count,
  SUM(CAST(spend AS NUMERIC)) AS total_spend
FROM {TABLE_NAME}
GROUP BY datasource
ORDER BY total_spend DESC
LIMIT 100"""

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

