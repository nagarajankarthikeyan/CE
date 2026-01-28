from app.sql_builder import build_sql
from app.executor import execute_sql
from app.performance_bundle import PERFORMANCE_METRICS


def build_period_intent(base_intent: dict, start: str, end: str, extra_filters: dict):
    intent = base_intent.copy()

    intent["metrics"] = PERFORMANCE_METRICS
    intent["dimensions"] = []
    intent["filters"] = extra_filters or {}
    intent["time_range"] = {"start": start, "end": end}

    return intent


def compare_periods(
    base_intent: dict,
    current_start: str,
    current_end: str,
    previous_start: str,
    previous_end: str,
    filters: dict
):
    # -------- Current Period --------
    curr_intent = build_period_intent(base_intent, current_start, current_end, filters)
    curr_sql, curr_params = build_sql_from_dict(curr_intent)
    curr_rows = execute_sql(curr_sql, curr_params)[0]

    # -------- Previous Period --------
    prev_intent = build_period_intent(base_intent, previous_start, previous_end, filters)
    prev_sql, prev_params = build_sql_from_dict(prev_intent)
    prev_rows = execute_sql(prev_sql, prev_params)[0]

    # -------- Delta Calculation --------
    deltas = {}

    for k in PERFORMANCE_METRICS:
        curr_val = curr_rows.get(k) or 0
        prev_val = prev_rows.get(k) or 0

        if prev_val == 0:
            pct_change = None
        else:
            pct_change = ((curr_val - prev_val) / prev_val) * 100

        deltas[k] = {
            "current": curr_val,
            "previous": prev_val,
            "delta": curr_val - prev_val,
            "pct_change": pct_change
        }

    return {
        "current": curr_rows,
        "previous": prev_rows,
        "deltas": deltas
    }


# Helper: reuse sql_builder but from dict
from app.sql_builder import build_sql

def build_sql_from_dict(intent_dict: dict):
    import json
    return build_sql(json.dumps(intent_dict))
