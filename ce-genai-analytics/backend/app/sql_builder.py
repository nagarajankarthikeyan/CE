import json
from app.semantic_layer import METRICS, DIMENSIONS


def safe_json_loads(text: str) -> dict:
    if not text or not text.strip():
        raise ValueError("GPT returned empty intent JSON")

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end >= 0:
            return json.loads(text[start:end+1])

        raise ValueError(f"Invalid JSON from GPT:\n{text}")


def build_sql(intent_json: str):
    intent = safe_json_loads(intent_json)

    metrics = intent.get("metrics", [])
    dimensions = intent.get("dimensions", [])
    filters = intent.get("filters", {})
    time_range = intent.get("time_range", {})
    ranking = intent.get("ranking", {})

    select_parts = []
    group_by_parts = []
    params = {}
    where_parts = []

    # =========================
    # Metrics (Aggregates)
    # =========================
    # =========================
# Metrics (Aggregates)
# =========================
    seen_metric_expr = set()

    for m in metrics:
        if m not in METRICS:
            raise ValueError(f"Unknown metric: {m}")

        expr = METRICS[m]

        # Skip None metrics (like performance placeholder)
        if not expr:
            continue

        # Deduplicate same SQL expressions
        if expr in seen_metric_expr:
            continue

    seen_metric_expr.add(expr)

    # SQL-safe alias (no spaces)
    safe_alias = m.replace(" ", "_")

    select_parts.append(f"{expr} AS [{safe_alias}]")


    # =========================
    # Dimensions (Group By)
    # =========================
    for d in dimensions:
        if d not in DIMENSIONS:
            raise ValueError(f"Unknown dimension: {d}")

        expr = DIMENSIONS[d]
        select_parts.append(f"{expr} AS {d}")
        group_by_parts.append(expr)

    # =========================
    # Filters (JSON)
    # =========================
    for k, v in filters.items():
        if k in DIMENSIONS:
            where_parts.append(f"{DIMENSIONS[k]} = :{k}")
            params[k] = v
        else:
            raise ValueError(f"Unknown filter dimension: {k}")

    # =========================
    # Time Range (date in JSON)
    # =========================
    date_expr = DIMENSIONS["date"]

    if time_range.get("start"):
        where_parts.append(f"{date_expr} >= :start")
        params["start"] = time_range["start"]

    if time_range.get("end"):
        where_parts.append(f"{date_expr} <= :end")
        params["end"] = time_range["end"]

    if not select_parts:
        raise ValueError("No valid metrics or dimensions detected.")

    sql = f"""
    SELECT
        {', '.join(select_parts)}
    FROM DataLakeRaw
    """

    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    if group_by_parts:
        sql += " GROUP BY " + ", ".join(group_by_parts)

    if ranking.get("order_by"):
        if ranking["order_by"] not in metrics and ranking["order_by"] not in dimensions:
            raise ValueError(f"Invalid order_by: {ranking['order_by']}")
        sql += f" ORDER BY {ranking['order_by']} DESC"

    if ranking.get("limit"):
        sql += f" OFFSET 0 ROWS FETCH NEXT {ranking['limit']} ROWS ONLY"

    print("====== GENERATED SQL (JSON AWARE) ======")
    print(sql)
    print("====== PARAMS ======")
    print(params)

    return sql, params
