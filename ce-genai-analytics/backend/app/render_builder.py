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

def looks_like_currency(val):
    if not isinstance(val, (int, float)):
        return False

    v = float(val)

    # Ignore small integers like 1, 4 (quarter)
    if float(v).is_integer():
        return False

    # Currency often > 1 and has decimals
    if v > 1 and round(v, 2) != round(v, 0):
        return True

    return False



def looks_like_percent(val):
    if not isinstance(val, (int, float)):
        return False

    # Between 0 and 100 but not a whole number like year/quarter
    if 0 <= float(val) <= 100 and not float(val).is_integer():
        return True

    return False




# =========================
# Formatting
# =========================

def format_value(val, col_name="", col_format="default"):
    try:
        if col_format == "percent":
            return f"{round(float(val), 2)}%"

        if col_format == "currency":
            return f"${round(float(val), 2):,.2f}"

        if isinstance(val, (int, float)):
            if float(val).is_integer():
                return int(val)
            return round(float(val), 2)

        if isinstance(val, str):
            return val.replace("_", " ").strip()

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

    column_formats = {
    col: detect_column_format(rows, col)
    for col in columns
}



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
                "value": format_value(rows[0][key], key, column_formats.get(0))
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

            # IMPORTANT: currency formatting preserved here
            y_formatted = [format_value(r[c2], c2, column_formats.get(c2)) for r in rows]

            return {
                "render_type": "chart",
                "title": question,
                "kpis": [],
                "table": {
                    "columns": [prettify_label(c1), prettify_label(c2)],
                    "rows": [
                        [prettify_value(r[c1]), format_value(r[c2], c2, column_formats.get(c2))]
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
    # 3. Ranking (ONLY if exactly 2 columns)
    # =========================
    if len(columns) == 2:
        dim = columns[0]
        metric = columns[1]

        if is_numeric(rows[0].get(metric)):

            column_formats = {
                col: detect_column_format(rows, col)
                for col in columns
            }

            ranked = []

            for r in rows:
                raw_val = r.get(metric)

                ranked.append({
                    "label": prettify_value(str(r.get(dim))),
                    "value": format_value(
                        raw_val,
                        metric,
                        column_formats.get(metric, "default")
                    ),
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
                    "y_formatted": [
                        format_value(
                            r["_raw_value"],
                            metric,
                            column_formats.get(metric, "default")
                        )
                        for r in ranked
                    ],
                    "series": []
                },
                "ranked_list": [
                    {
                        "label": r["label"],
                        "value": format_value(
                            r["_raw_value"],
                            metric,
                            column_formats.get(metric, "default")
                        )
                    }
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
                    "value": format_value(val, col, column_formats.get(col))
                })

        return {
            "render_type": "mixed",
            "title": question,
            "kpis": kpis[:6],
            "table": {
                "columns": [prettify_label(c) for c in columns],
                "rows": [
                    [format_value(r.get(c), c, column_formats.get(c)) for c in columns]
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
                [format_value(r.get(c), c, column_formats.get(c)) for c in columns]
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

# =========================
# Analyze column
# =========================
def analyze_column(rows, column):
    """
    Analyze numeric distribution of a column.
    Returns metadata about the column.
    """
    values = []

    for r in rows:
        v = r.get(column)
        if isinstance(v, (int, float)):
            values.append(float(v))

    if not values:
        return {"type": "non_numeric"}

    min_val = min(values)
    max_val = max(values)
    avg_val = sum(values) / len(values)

    # Year detection
    if all(1900 <= int(v) <= 2100 for v in values):
        return {"type": "year"}

    # Percentage detection (0–100 range)
    if 0 <= min_val and max_val <= 100:
        return {"type": "percentage"}

    # Currency detection:
    # Large magnitude and decimals present
    has_decimals = any(not float(v).is_integer() for v in values)

    if max_val > 1000 and has_decimals:
        return {"type": "currency"}

    return {"type": "number"}

def detect_column_format(rows, column):
    numeric_values = [
        r.get(column) for r in rows
        if isinstance(r.get(column), (int, float))
    ]

    if not numeric_values:
        return "default"

    # Percent detection (all values small decimals)
    if all(0 <= float(v) <= 100 for v in numeric_values) and any(
        not float(v).is_integer() for v in numeric_values
    ):
        return "percent"

    # Currency detection (any value has decimals)
    if any(not float(v).is_integer() for v in numeric_values):
        return "currency"

    return "default"
