from app.gpt_client import chat_completion

SYSTEM_PROMPT = """
You are a BI query parser.
Convert user questions into STRICT JSON ONLY.
Do NOT add explanations.
Do NOT use markdown.
Return JSON ONLY.

Schema:
{
  "metrics": [],
  "dimensions": [],
  "filters": {},
  "time_range": {"period": null, "start": null, "end": null},
  "comparison": {"enabled": false, "previous_period": null},
  "ranking": {"order_by": null, "limit": null},
  "narrative": true
}
"""

def parse_user_question(question: str) -> str:
    prompt = f"""
User question:
{question}

Return ONLY valid JSON.
"""
    raw = chat_completion(SYSTEM_PROMPT, prompt)

    print("====== GPT INTENT RAW ======")
    print(raw)
    print("============================")

    return raw
