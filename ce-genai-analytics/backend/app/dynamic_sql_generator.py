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
- If user asks for performance, return spend, clicks, impressions, and CTR only.


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

AGGREGATION RULES:
- SUM(column)
- AVG(column)
- MAX(column)
- MIN(column)
- COUNT(*)

DERIVED METRICS:
- CTR = SAFE_DIVIDE(SUM(Clicks), SUM(Impressions)) * 100

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
