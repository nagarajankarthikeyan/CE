import re

def validate_sql(sql: str):
    """
    Validates SQL to ensure only SELECT/CTE queries are allowed.
    Blocks all destructive operations.
    
    Raises:
        ValueError: If forbidden operations are detected
    """
    s = sql.strip().lower()

    # =========================
    # Allow SELECT or CTE (WITH ... SELECT)
    # =========================
    if not (s.startswith("select") or s.startswith("with")):
        raise ValueError("Only SELECT/CTE statements are allowed. Write queries are not permitted.")

    # =========================
    # Block destructive statements
    # =========================
    forbidden_keywords = {
        "insert": "INSERT operations are not allowed",
        "update": "UPDATE operations are not allowed",
        "delete": "DELETE operations are not allowed",
        "drop": "DROP operations are not allowed",
        "alter": "ALTER operations are not allowed",
        "truncate": "TRUNCATE operations are not allowed",
        "merge": "MERGE operations are not allowed",
        "create": "CREATE operations are not allowed",
        "grant": "GRANT operations are not allowed",
        "revoke": "REVOKE operations are not allowed",
        "exec": "EXEC operations are not allowed",
        "execute": "EXECUTE operations are not allowed"
    }

    for keyword, error_msg in forbidden_keywords.items():
        # Block whole words only
        if re.search(rf"\b{keyword}\b", s):
            raise ValueError(f"⛔ Operation blocked: {error_msg}")

    return True
