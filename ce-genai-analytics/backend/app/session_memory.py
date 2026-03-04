SESSION_MEMORY = {}


def _key(user_id, conversation_id):
    return f"{str(user_id)}::{str(conversation_id)}"

def extract_filters_from_sql(sql: str):
    import re

    # Capture only the WHERE segment, stopping before GROUP/ORDER/LIMIT/HAVING.
    match = re.search(
        r"\bWHERE\b(?P<body>.*?)(?=\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|\bHAVING\b|$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []

    conditions = match.group("body")

    parts = re.split(r"\bAND\b", conditions, flags=re.IGNORECASE)

    cleaned = []
    for p in parts:
        p = p.strip().rstrip(";")
        # Defensive cleanup in case malformed SQL slips through.
        p = re.sub(r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b.*$", "", p, flags=re.IGNORECASE | re.DOTALL).strip()
        if p:
            cleaned.append(p)

    return cleaned

def get_session_filters(user_id, conversation_id):
    key = _key(user_id, conversation_id)
    entry = SESSION_MEMORY.get(key, {})
    filters = entry.get("filters", []) if isinstance(entry, dict) else []
    return _sanitize_conditions(filters)


def _sanitize_conditions(conditions):
    import re
    invalid_temporal_patterns = [
        r"\bTIMESTAMP\s*\(\s*''\s*\)",
        r"\bDATE\s*\(\s*''\s*\)",
        r"\bDATETIME\s*\(\s*''\s*\)",
        r"\bTIMESTAMP\s*\(\s*\"\"\s*\)",
        r"\bDATE\s*\(\s*\"\"\s*\)",
        r"\bDATETIME\s*\(\s*\"\"\s*\)",
        r"\bPARSE_TIMESTAMP\s*\([^,]+,\s*['\"]\s*['\"]\s*\)",
        r"\bPARSE_DATE\s*\([^,]+,\s*['\"]\s*['\"]\s*\)",
        r"\bPARSE_DATETIME\s*\([^,]+,\s*['\"]\s*['\"]\s*\)",
        r"\bBETWEEN\s*''\s*AND\s*''",
        r"\b(date|time|timestamp)\b.*=\s*''",
        r"\b(date|time|timestamp)\b.*=\s*\"\"",
        r"=\s*''.*\b(date|time|timestamp)\b",
        r"=\s*\"\".*\b(date|time|timestamp)\b",
    ]
    if not isinstance(conditions, list):
        return []
    out = []
    for c in conditions:
        if not isinstance(c, str):
            continue
        s = c.strip().rstrip(";")
        if not s:
            continue
        # Drop anything that looks like a clause tail, not a condition.
        if re.search(r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b", s, re.IGNORECASE):
            s = re.sub(r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b.*$", "", s, flags=re.IGNORECASE | re.DOTALL).strip()
        if any(re.search(p, s, re.IGNORECASE) for p in invalid_temporal_patterns):
            continue
        if s:
            out.append(s)
    return out

def store_session_filters(user_id, conversation_id, conditions):
    """
    Store ONLY clean condition strings.
    """
    key = _key(user_id, conversation_id)

    if isinstance(conditions, str):
        conditions = [conditions]

    if not isinstance(conditions, list):
        return

    # Ensure only strings are stored
    clean = _sanitize_conditions(conditions)
    entry = SESSION_MEMORY.get(key, {})
    if not isinstance(entry, dict):
        entry = {}
    entry["filters"] = clean
    SESSION_MEMORY[key] = entry


def get_last_question(user_id, conversation_id):
    key = _key(user_id, conversation_id)
    entry = SESSION_MEMORY.get(key, {})
    if not isinstance(entry, dict):
        return None
    q = entry.get("last_question")
    if isinstance(q, str) and q.strip():
        return q.strip()
    return None


def store_last_question(user_id, conversation_id, question: str):
    if not isinstance(question, str) or not question.strip():
        return
    key = _key(user_id, conversation_id)
    entry = SESSION_MEMORY.get(key, {})
    if not isinstance(entry, dict):
        entry = {}
    entry["last_question"] = question.strip()
    SESSION_MEMORY[key] = entry


def get_sql_context(user_id, conversation_id):
    key = _key(user_id, conversation_id)
    entry = SESSION_MEMORY.get(key, {})
    if not isinstance(entry, dict):
        return {
            "history": [],
            "last_sql": None,
            "schema": None,
            "last_result_columns": [],
            "last_rows": [],
        }

    history = entry.get("history", [])
    if not isinstance(history, list):
        history = []
    clean_history = []
    for m in history:
        if not isinstance(m, dict):
            continue
        role = m.get("role")
        content = m.get("content")
        if role in {"user", "assistant"} and isinstance(content, str) and content.strip():
            clean_history.append({"role": role, "content": content.strip()})

    last_sql = entry.get("last_sql")
    if not isinstance(last_sql, str) or not last_sql.strip():
        last_sql = None

    schema = entry.get("schema")
    if not isinstance(schema, (dict, list)):
        schema = None

    last_result_columns = entry.get("last_result_columns", [])
    if not isinstance(last_result_columns, list):
        last_result_columns = []
    clean_cols = [str(c).strip() for c in last_result_columns if str(c).strip()]
    last_rows = entry.get("last_rows", [])
    if not isinstance(last_rows, list):
        last_rows = []

    return {
        "history": clean_history,
        "last_sql": last_sql,
        "schema": schema,
        "last_result_columns": clean_cols,
        "last_rows": last_rows,
    }


def store_sql_turn(
    user_id,
    conversation_id,
    user_message: str,
    assistant_message: str,
    sql: str,
    schema,
    rows: list | None = None,
    max_history_messages: int = 12,
):
    key = _key(user_id, conversation_id)
    entry = SESSION_MEMORY.get(key, {})
    if not isinstance(entry, dict):
        entry = {}

    history = entry.get("history", [])
    if not isinstance(history, list):
        history = []

    if isinstance(user_message, str) and user_message.strip():
        history.append({"role": "user", "content": user_message.strip()})
    if isinstance(assistant_message, str) and assistant_message.strip():
        history.append({"role": "assistant", "content": assistant_message.strip()})

    if isinstance(max_history_messages, int) and max_history_messages >= 0:
        if len(history) > max_history_messages:
            history = history[-max_history_messages:]

    entry["history"] = history
    if isinstance(sql, str) and sql.strip():
        entry["last_sql"] = sql.strip()
    if isinstance(schema, (dict, list)):
        entry["schema"] = schema
    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
        entry["last_result_columns"] = [str(k) for k in rows[0].keys()]
        entry["last_rows"] = rows[:100]
    SESSION_MEMORY[key] = entry



def clear_session(user_id, conversation_id):
    key = _key(user_id, conversation_id)
    SESSION_MEMORY.pop(key, None)
