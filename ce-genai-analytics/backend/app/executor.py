from typing import List, Dict, Any
from app.db import run_query


def execute_sql(sql: str, params: dict) -> List[Dict[str, Any]]:
    """
    Executes SQL and returns normalized list of dict rows.
    Handles empty results safely.
    """
    try:
        rows = run_query(sql, params)

        if not rows:
            return []

        # Normalize Decimal / None if needed
        normalized = []
        for row in rows:
            clean_row = {}
            for k, v in row.items():
                if v is None:
                    clean_row[k] = None
                else:
                    try:
                        clean_row[k] = float(v) if hasattr(v, "as_integer_ratio") else v
                    except Exception:
                        clean_row[k] = v
            normalized.append(clean_row)

        return normalized

    except Exception as ex:
        # In production, log this to Serilog / AppInsights / Datadog
        raise RuntimeError(f"SQL execution failed: {str(ex)}")
