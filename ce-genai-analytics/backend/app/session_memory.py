SESSION_MEMORY = {}

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
    key = f"{user_id}_{conversation_id}"
    filters = SESSION_MEMORY.get(key, [])
    return _sanitize_conditions(filters)


def _sanitize_conditions(conditions):
    import re
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
        if s:
            out.append(s)
    return out

def store_session_filters(user_id, conversation_id, conditions):
    """
    Store ONLY clean condition strings.
    """
    key = f"{user_id}_{conversation_id}"

    if isinstance(conditions, str):
        conditions = [conditions]

    if not isinstance(conditions, list):
        return

    # Ensure only strings are stored
    clean = _sanitize_conditions(conditions)
    SESSION_MEMORY[key] = clean



def clear_session(user_id, conversation_id):
    key = f"{user_id}_{conversation_id}"
    SESSION_MEMORY.pop(key, None)
