import re

def validate_sql(sql: str):
    s = sql.strip().lower()

    # =========================
    # Allow SELECT or CTE (WITH ... SELECT)
    # =========================
    if not (s.startswith("select") or s.startswith("with")):
        raise ValueError("Only SELECT/CTE statements are allowed")

    # =========================
    # Block destructive statements
    # =========================
    forbidden = [
        "insert", "update", "delete",
        "drop", "alter", "truncate",
        "merge", "create", "grant", "revoke"
    ]

    for f in forbidden:
        # Block whole words only
        if re.search(rf"\b{f}\b", s):
            raise ValueError(f"Forbidden SQL keyword detected: {f}")

    return True
