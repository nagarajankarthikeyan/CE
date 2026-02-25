import re
from datetime import datetime, timedelta
from app.platform_mapping import find_platform_match

def _get_string_columns(schema_fields):
    if not schema_fields:
        return []

    cols = []
    for field in schema_fields:
        if not isinstance(field, dict):
            continue
        name = field.get("name")
        dtype = str(field.get("type", "")).upper()
        if name and dtype == "STRING":
            cols.append(str(name))
    return cols


def _tokenize_column(col_name: str):
    return [p for p in re.split(r"[^a-z0-9]+", col_name.lower()) if p]


def _pluralize(term: str):
    if term.endswith("y") and len(term) > 1 and term[-2] not in "aeiou":
        return term[:-1] + "ies"
    if term.endswith("s"):
        return term
    return term + "s"


def _dimension_aliases(col_name: str):
    tokens = _tokenize_column(col_name)
    if not tokens:
        return set()

    ignored = {"id", "name", "key", "code"}
    meaningful = [t for t in tokens if t not in ignored]
    if not meaningful:
        return set()

    phrase = " ".join(meaningful)
    aliases = {phrase, _pluralize(phrase)}

    generic_last = {"line", "category", "type", "group", "level", "value", "business"}
    for token in meaningful:
        if len(token) >= 4 and token not in generic_last:
            aliases.add(token)
            aliases.add(_pluralize(token))

    return aliases


def _extract_dynamic_dimension_filters(message: str, schema_fields):
    string_columns = _get_string_columns(schema_fields)
    if not string_columns:
        return []

    def classify_dimension(col_name: str):
        joined = " ".join(_tokenize_column(col_name))
        if "business" in joined and "line" in joined:
            return "business"
        if "campaign" in joined:
            return "campaign"
        if "creative" in joined or "ad" in joined:
            return "ad"
        return None

    hierarchy = {"business": None, "campaign": None, "ad": None}
    for c in sorted(string_columns):
        d = classify_dimension(c)
        if d and hierarchy[d] is None:
            hierarchy[d] = c

    msg = message.lower()
    matches = []
    noise_tokens = {
        "show", "list", "give", "display", "fetch", "get",
        "all", "any", "every", "overall",
        "metric", "metrics", "performance", "compare", "comparison",
        "spend", "cost", "click", "clicks", "impression", "impressions", "ctr",
        "total", "sum", "avg", "average",
    }
    quarter_tokens = {"q1", "q2", "q3", "q4", "quarter", "quarters", "vs", "versus", "compared"}

    def is_valid_value(value: str) -> bool:
        v = (value or "").strip().lower()
        if not v:
            return False
        if len(v) < 2:
            return False
        tokens = [t for t in re.split(r"[^a-z0-9]+", v) if t]
        if not tokens:
            return False
        # Guardrail: avoid binding analytical phrasing as a dimension value
        # (e.g., "q4 home services spend and performance compare to q3").
        if any(t in noise_tokens for t in tokens):
            return False
        if any(t in quarter_tokens for t in tokens):
            return False
        if any(re.fullmatch(r"(19|20)\d{2}", t) for t in tokens):
            return False
        # "all campaigns", "show campaigns", etc. should not bind as a filter value.
        if all(t in noise_tokens for t in tokens):
            return False
        if tokens[0] in noise_tokens:
            return False
        return True

    for col in string_columns:
        for alias in sorted(_dimension_aliases(col), key=len, reverse=True):
            alias_re = re.escape(alias)
            value_stop = r"(?=\s+(?:today|yesterday|tomorrow|last|this|by|for|in|on|with|where)\b|[?.!,]|$)"
            # Keep extraction tight to reduce accidental capture of query clauses.
            value_chunk = r"([a-z0-9]+(?:[ /&\-][a-z0-9]+){0,2})"
            patterns = [
                rf"\bon\s+{value_chunk}\s+{alias_re}\b",
                rf"\bof\s+{value_chunk}\s+{alias_re}\b",
                rf"\b{alias_re}\s*(?:is|=|:)\s*{value_chunk}{value_stop}",
                rf"\b{value_chunk}\s+{alias_re}\b{value_stop}",
            ]

            for idx, pattern in enumerate(patterns):
                m = re.search(pattern, msg, re.IGNORECASE)
                if not m:
                    continue
                value = m.group(1).strip()
                if not is_valid_value(value):
                    continue
                word_count = len(value.split())
                matches.append((idx, word_count, m.start(), -len(alias), col, value, alias))

    if not matches:
        return []

    matches.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    _, _, _, _, matched_col, matched_value, _ = matches[0]

    safe_value = matched_value.replace("'", "''")
    dim = classify_dimension(matched_col)
    parent_col = None
    if dim == "ad":
        parent_col = hierarchy.get("campaign")
    elif dim == "campaign":
        parent_col = hierarchy.get("business")

    if parent_col:
        condition = (
            f"(LOWER({matched_col}) = LOWER('{safe_value}') "
            f"OR LOWER({parent_col}) = LOWER('{safe_value}'))"
        )
        return [{
            "column": condition,
            "value": "",
            "raw": True
        }]

    return [{
        "column": f"LOWER({matched_col})",
        "value": f"LOWER('{safe_value}')",
        "raw": True
    }]


def resolve_filters(message: str, schema_fields=None):
    """
    Extract dynamic filters from user message.
    Returns list of dict:
    [
        {"column": "...", "value": "...", "raw": False},
        {"column": "...", "value": "DATE_SUB(...)", "raw": True}
    ]
    """

    msg = message.lower()
    filters = []

    # ---------------------------
    # Dynamic dimension filters
    # ---------------------------
    filters.extend(_extract_dynamic_dimension_filters(message, schema_fields))

    # ---------------------------
    # Platform (if extracted earlier, skip here)
    # ---------------------------
    platform, _ = find_platform_match(msg)
    if platform:
        filters.append({
            "column": "platform",
            "value": platform,
            "raw": False
        })

    # ---------------------------
    # Yesterday
    # ---------------------------
    if "yesterday" in msg:
        filters.append({
            "column": "DATE(date)",
            "value": "DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)",
            "raw": True
        })

    # ---------------------------
    # Today
    # ---------------------------
    if "today" in msg:
        filters.append({
            "column": "DATE(date)",
            "value": "CURRENT_DATE()",
            "raw": True
        })

    return filters
