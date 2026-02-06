from datetime import date, datetime


# =========================
# Helpers
# =========================

def looks_like_date(val):
    if isinstance(val, (date, datetime)):
        return True
    try:
        s = str(val)
        return len(s) >= 8 and s[:4].isdigit() and "-" in s
    except:
        return False


def is_numeric(v):
    try:
        return v is not None and float(v) == float(v)
    except:
        return False


def prettify_label(col: str) -> str:
    if not col:
        return col
    return col.replace("_", " ").strip().title()


def prettify_value(val):
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    if isinstance(val, str):
        return val.replace("_", " ").strip()
    return val


# =========================
# Semantic Detection
# =========================

def is_currency_column(col_name: str) -> bool:
    if not col_name:
        return False

    name = col_name.lower()
    keywords = [
        "spend", "cost", "amount", "revenue",
        "budget", "price", "sales", "value"
    ]
    return any(k in name for k in keywords)


def is_percent_column(col_name: str) -> bool:
    if not col_name:
        return False

    name = col_name.lower()
    keywords = ["ctr", "rate", "percent", "percentage"]
    return any(k in name for k in keywords)


# =========================
# Formatting
# =========================

def format_value(val, col_name: str = ""):
    try:
        # BigQuery date safety
        if isinstance(val, (date, datetime)):
            return val.isoformat()

        # Percent
        if is_percent_column(col_name) and isinstance(val, (int, float)):
            return f"{round(float(val), 2)}%"

        # Currency
        if is_currency_column(col_name) and isinstance(val, (int, float)):
            return f"${round(float(val), 2):,.2f}"

        # Other numbers
        if isinstance(val, (int, float)):
            if float(val).is_integer():
                return int(val)
            return round(float(val), 2)

        # Strings
        if isinstance(val, str):
            return prettify_value(val)

        return val
    except:
        return val


def round_numeric(val):
    try:
        if isinstance(val, (int, float)):
            return round(float(val), 2)
        return val
    except:
        return val


# =========================
# Core Render Builder
# =========================

def build_render_spec(question: str, rows: list):

    if not rows:
        return {
            "render_type": "narrative",
            "title": question,
            "kpis": [],
            "table": {"columns": [], "rows": []},
            "chart": {},
            "ranked_list": [],
            "bullets": [],
            "narrative": "No data found for this query."
        }

    columns = list(rows[0].keys())

    # =========================
    # 1. KPI
    # =========================
    if len(columns) == 1 and len(rows) == 1:
        key = columns[0]
        return {
            "render_type": "kpi",
            "title": question,
            "kpis": [{
                "label": prettify_label(key),
                "value": format_value(rows[0][key], key)
            }],
            "table": {"columns": [], "rows": []},
            "chart": {},
            "ranked_list": [],
            "bullets": [],
            "narrative": ""
        }

    # =========================
    # 2. Time Series → Line Chart
    # =========================
    if len(columns) == 2:
        c1, c2 = columns
        first_val = rows[0].get(c1)

        is_date_dim = (
            "date" in c1.lower()
            or looks_like_date(first_val)
        )

        if is_date_dim and is_numeric(rows[0].get(c2)):

            y_numeric = [round_numeric(r[c2]) for r in rows]

            # 🔥 IMPORTANT: currency formatting preserved here
            y_formatted = [format_value(r[c2], c2) for r in rows]

            return {
                "render_type": "chart",
                "title": question,
                "kpis": [],
                "table": {
                    "columns": [prettify_label(c1), prettify_label(c2)],
                    "rows": [
                        [prettify_value(r[c1]), format_value(r[c2], c2)]
                        for r in rows
                    ]
                },
                "chart": {
                    "type": "line",
                    "x": [prettify_value(r[c1]) for r in rows],
                    "y": y_numeric,                # numeric for scaling
                    "y_formatted": y_formatted,    # formatted for display
                    "series": []
                },
                "ranked_list": [],
                "bullets": [],
                "narrative": ""
            }

    # =========================
    # 3. Ranking → Bar Chart
    # =========================
    if len(columns) == 2:
        dim, metric = columns

        if is_numeric(rows[0].get(metric)):

            ranked = []
            for r in rows:
                raw_val = round_numeric(r.get(metric))
                ranked.append({
                    "label": prettify_value(r.get(dim)),
                    "value": format_value(raw_val, metric),
                    "_raw_value": raw_val
                })

            ranked = ranked[:20]

            return {
                "render_type": "ranked_list",
                "title": question,
                "kpis": [],
                "table": {"columns": [], "rows": []},
                "chart": {
                    "type": "bar",
                    "x": [r["label"] for r in ranked],
                    "y": [r["_raw_value"] for r in ranked],
                    "y_formatted": [r["value"] for r in ranked],
                    "series": []
                },
                "ranked_list": [
                    {"label": r["label"], "value": r["value"]}
                    for r in ranked
                ],
                "bullets": [],
                "narrative": ""
            }

    # =========================
    # 4. Mixed Summary
    # =========================
    if is_summary_question(question):

        kpis = []
        for col in columns:
            val = rows[0].get(col)
            if is_numeric(val):
                kpis.append({
                    "label": prettify_label(col),
                    "value": format_value(val, col)
                })

        return {
            "render_type": "mixed",
            "title": question,
            "kpis": kpis[:6],
            "table": {
                "columns": [prettify_label(c) for c in columns],
                "rows": [
                    [format_value(r.get(c), c) for c in columns]
                    for r in rows[:10]
                ]
            },
            "chart": {},
            "ranked_list": [],
            "bullets": [],
            "narrative": "Here is a high-level summary of performance."
        }

    # =========================
    # 5. Default → Table
    # =========================
    return {
        "render_type": "table",
        "title": question,
        "kpis": [],
        "table": {
            "columns": [prettify_label(c) for c in columns],
            "rows": [
                [format_value(r.get(c), c) for c in columns]
                for r in rows
            ]
        },
        "chart": {},
        "ranked_list": [],
        "bullets": [],
        "narrative": ""
    }


# =========================
# Summary Detection
# =========================

def is_summary_question(q: str):
    q = q.lower()
    keywords = [
        "summarize", "summary", "overview",
        "overall performance", "executive",
        "how did", "what happened"
    ]
    return any(k in q for k in keywords)
