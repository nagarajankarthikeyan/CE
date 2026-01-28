from app.gpt_client import stream_chat_completion
import json


async def stream_narrative(question: str, rows: list, render_spec: dict):
    """
    Async generator.
    MUST NOT buffer full response.
    Streams tokens progressively.
    """

    prompt = f"""
User question:
{question}

Structured result:
{json.dumps(render_spec, indent=2)}

Sample rows:
{json.dumps(rows[:10], indent=2)}

Write a concise executive explanation.
Do NOT repeat raw numbers verbatim if already shown.
Focus on insights and interpretation.
"""

    async for token in stream_chat_completion(prompt):
        yield token
