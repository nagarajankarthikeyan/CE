import os
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SQL_SYSTEM_PROMPT = """
You are a senior analytics engineer generating SQL Server queries
over semi-structured JSON data.

Your task:
Convert a natural language business question into a SINGLE valid SQL Server SELECT query.

CRITICAL RULES (ALWAYS FOLLOW):
- ONLY return SQL
- DO NOT wrap in ```sql
- DO NOT return explanations
- Only SELECT or WITH + SELECT (CTE) statements
- Table name: DataLakeRaw
- All business fields are inside JSON column: RawJson
- Access fields using JSON_VALUE(RawJson, '$.<field>')

DATA TYPE RULES (VERY IMPORTANT):
- JSON numeric values may look like '0.0', '612.0', '133085.47'
- NEVER CAST JSON values to INT
- ALWAYS use:
    CAST(... AS FLOAT)
    OR
    CAST(... AS DECIMAL(18,2))
    OR
    TRY_CAST(... AS FLOAT)

SAFE AGGREGATION PATTERNS:
- SUM(TRY_CAST(JSON_VALUE(...) AS FLOAT))
- AVG(TRY_CAST(JSON_VALUE(...) AS FLOAT))
- MAX(TRY_CAST(JSON_VALUE(...) AS FLOAT))
- MIN(TRY_CAST(JSON_VALUE(...) AS FLOAT))

COUNTS:
- Use COUNT(*) for record counts
- Do NOT cast JSON to INT for counts

DERIVED METRICS:
- CTR = (SUM(link_clicks) / NULLIF(SUM(impressions),0)) * 100
  Use FLOAT/DECIMAL casts

DATE HANDLING:
- date is stored as string in JSON: $.date
- Always CAST to DATE when filtering/grouping by date

EXAMPLES:

Q: Total enrollments by campaign in Q4
GOOD:
SELECT
  JSON_VALUE(RawJson, '$.campaign') AS Campaign,
  SUM(TRY_CAST(JSON_VALUE(RawJson, '$.actions_enrollments_v2_') AS FLOAT)) AS Total_Enrollments
FROM DataLakeRaw
WHERE DATEPART(QUARTER, CAST(JSON_VALUE(RawJson, '$.date') AS DATE)) = 4
GROUP BY JSON_VALUE(RawJson, '$.campaign')
ORDER BY Total_Enrollments DESC;

BAD (DO NOT DO THIS):
CAST(JSON_VALUE(...) AS INT)

REMEMBER:
JSON numeric fields are NOT guaranteed to be integers.
"""


def generate_sql(question: str, json_fields) -> str:
    """
    Uses GPT to dynamically generate SQL Server query.
    Works whether json_fields is a list or dict.
    """

    # Normalize schema fields safely
    if isinstance(json_fields, dict):
        available_fields = list(json_fields.keys())
    elif isinstance(json_fields, list):
        available_fields = json_fields
    else:
        try:
            available_fields = list(json_fields)
        except Exception:
            available_fields = []

    user_prompt = f"""
Business Question:
{question}

Available JSON fields (may vary by row):
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

    # Safety cleanup
    sql = sql.replace("```sql", "").replace("```", "").strip()

    if not sql.lower().startswith(("select", "with")):
        raise ValueError(f"Only SELECT/CTE allowed. GPT returned:\n{sql}")

    print("\n====== GPT GENERATED SQL (DYNAMIC) ======")
    print(sql)
    print("========================================\n")

    return sql

   
