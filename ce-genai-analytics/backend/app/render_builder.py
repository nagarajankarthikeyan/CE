from datetime import date, datetime


# =========================
# Helpers
# =========================

def looks_like_date(val):
    try:
        if isinstance(val, (date, datetime)):
            return True
        s = str(val)
        return (
            len(s) >= 8
            and s[4] == "-"
            and s[:4].isdigit()
        )
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


def format_currency(val):
    try:
        if isinstance(val, (int, float)):
            return f"${val:,.2f}"
        return val
    except:
        return val


def round_number(val):
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
            "kpis": [
                {
                    "label": prettify_label(key),
                    "value": format_currency(rows[0][key])
                }
            ],
            "table": {"columns": [], "rows": []},
            "chart": {},
            "ranked_list": [],
            "bullets": [],
            "narrative": ""
        }

    # =========================
    # 2. Time Series (Chart)
    # =========================
    if len(columns) == 2:
        c1, c2 = columns
        first_val = rows[0].get(c1)

        is_date_dim = (
            "date" in c1.lower()
            or "day" in c1.lower()
            or "dt" in c1.lower()
            or looks_like_date(first_val)
        )

        if is_date_dim and is_numeric(rows[0].get(c2)):

            y_numeric = [round_number(r[c2]) for r in rows]
            y_formatted = [format_currency(r[c2]) for r in rows]

            return {
                "render_type": "chart",
                "title": question,
                "kpis": [],
                "table": {
                    "columns": [prettify_label(c1), prettify_label(c2)],
                    "rows": [
                        [str(r[c1]), format_currency(r[c2])]
                        for r in rows
                    ]
                },
                "chart": {
                    "type": "line",
                    "x": [str(r[c1]) for r in rows],
                    "y": y_numeric,                 # ✅ numeric, 2 decimals
                    "y_formatted": y_formatted,     # ✅ $ + 2 decimals for UI
                    "series": []
                },
                "ranked_list": [],
                "bullets": [],
                "narrative": ""
            }

    # =========================
    # 3. Ranking
    # =========================
    if len(columns) == 2:
        dim, metric = columns

        if is_numeric(rows[0].get(metric)):

            ranked = []
            for r in rows:
                raw_val = round_number(r.get(metric))
                ranked.append({
                    "label": prettify_label(str(r.get(dim))),
                    "value": format_currency(raw_val),
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
                    "y": [r["_raw_value"] for r in ranked],   # numeric
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
    # 4. Mixed / Executive Summary
    # =========================
    if is_summary_question(question):

        kpis = []
        for col in columns:
            val = rows[0].get(col)
            if is_numeric(val):
                kpis.append({
                    "label": prettify_label(col),
                    "value": format_currency(val)
                })

        return {
            "render_type": "mixed",
            "title": question,
            "kpis": kpis[:6],
            "table": {
                "columns": [prettify_label(c) for c in columns],
                "rows": [
                    [format_currency(r.get(c)) for c in columns]
                    for r in rows[:10]
                ]
            },
            "chart": {},
            "ranked_list": [],
            "bullets": [],
            "narrative": "Here is a high-level summary of performance for the selected period."
        }

    # =========================
    # 5. Default = Table
    # =========================
    return {
        "render_type": "table",
        "title": question,
        "kpis": [],
        "table": {
            "columns": [prettify_label(c) for c in columns],
            "rows": [
                [format_currency(r.get(c)) for c in columns]
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
        "summarize", "summary", "overview", "high level",
        "overall performance", "executive", "performance",
        "how did", "what happened"
    ]
    return any(k in q for k in keywords)
