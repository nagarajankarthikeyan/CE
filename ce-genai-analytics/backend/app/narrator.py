from app.gpt_client import stream_chat_completion
import json
from datetime import date, datetime


def json_safe(obj):
    """
    Makes BigQuery date/datetime JSON serializable.
    """
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return str(obj)


async def stream_narrative(question: str, rows: list, render_spec: dict):
    """
    Async generator.
    MUST NOT buffer full response.
    Streams tokens progressively.
    BigQuery-safe JSON serialization.
    """

    # Convert sample rows safely (avoid date serialization crash)
    safe_rows = json.loads(json.dumps(rows[:10], default=json_safe))
    safe_render = json.loads(json.dumps(render_spec, default=json_safe))

    prompt = f"""
User question:
{question}

Structured result:
{json.dumps(safe_render, indent=2)}

Sample rows:
{json.dumps(safe_rows, indent=2)}

Write a short definition-style interpretation.
Return at least 2 short sentences (max 40 words total).
Never repeat or reference exact numbers already shown in table/KPI/chart.
Explain what the metrics represent in plain business terms.
Do NOT add recommendations, caveats, future steps, or extra detail.
"""

    async for token in stream_chat_completion(prompt):
        yield token
