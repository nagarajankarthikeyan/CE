import re
from datetime import datetime, timedelta
from app.platform_mapping import find_platform_match, get_platform_aliases

def _is_phrase_present(message: str, phrase: str) -> bool:
    if not message or not phrase:
        return False
    tokens = [t for t in re.split(r"\s+", phrase.strip()) if t]
    if not tokens:
        return False
    pattern = r"\b" + r"\W+".join(re.escape(t) for t in tokens) + r"\b"
    return re.search(pattern, message, flags=re.IGNORECASE) is not None


def _has_explicit_platform_intent(message: str) -> bool:
    msg = (message or "").lower()
    platform_terms = [
        r"\bmeta\b",
        r"\bfacebook\b",
        r"\binstagram\b",
        r"\bdv360\b",
        r"\bsa360\b",
        r"\bsearch ads 360\b",
        r"\bdisplay\s*&?\s*video\s*360\b",
        r"\bgoogle ads?\b",
        r"\byoutube\b",
        r"\btiktok\b",
        r"\blinkedin\b",
    ]
    return any(re.search(p, msg, re.IGNORECASE) for p in platform_terms)

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


def _build_platform_condition(schema_fields, canonical_platform: str, matched_phrase: str | None) -> str:
    candidate_cols = {"platform", "datasource", "source", "channel"}
    schema_cols = {str(c).lower(): c for c in _get_string_columns(schema_fields)}
    present_cols = [schema_cols[c] for c in candidate_cols if c in schema_cols]

    # Fallback to `platform` if schema isn't available.
    if not present_cols:
        present_cols = ["platform"]

    aliases = get_platform_aliases(canonical_platform)
    if matched_phrase:
        mp = re.sub(r"\s+", " ", matched_phrase.strip().lower())
        if mp:
            aliases.append(mp)
    aliases.append((canonical_platform or "").strip().lower())
    aliases = [a for a in dict.fromkeys(aliases) if a]

    if not aliases:
        safe = (canonical_platform or "").replace("'", "''")
        return f"LOWER(platform) = LOWER('{safe}')"

    alias_literals = ", ".join("'" + a.replace("'", "''") + "'" for a in aliases)
    like_terms = [
        "'" + ("%" + a.replace("'", "''") + "%") + "'"
        for a in aliases
        if len(a) >= 3
    ]
    col_exprs = []
    for col in present_cols:
        expr = f"LOWER({col}) IN ({alias_literals})"
        if like_terms:
            expr += " OR " + " OR ".join(f"LOWER({col}) LIKE {lt}" for lt in like_terms)
        col_exprs.append(f"({expr})")
    return "(" + " OR ".join(col_exprs) + ")"


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
        "by", "for", "in", "on", "at",
        "last", "this", "next", "previous",
        "week", "month", "quarter", "year", "ytd", "mtd",
        "today", "yesterday", "tomorrow",
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
                # "by <dimension>" indicates grouping intent, not a filter value.
                if idx == 3 and re.search(rf"\bby\s+{alias_re}\b", msg, re.IGNORECASE):
                    continue
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
    matched_pattern_idx, _, _, _, matched_col, matched_value, _ = matches[0]

    safe_value = matched_value.replace("'", "''")
    dim = classify_dimension(matched_col)
    parent_col = None
    if dim == "ad":
        parent_col = hierarchy.get("campaign")
    elif dim == "campaign":
        parent_col = hierarchy.get("business")

    # Explicit equality phrasing ("campaign is X", "campaign = X") should stay exact.
    # Natural-language phrasing ("X campaigns", "on X campaign") should use LIKE
    # to avoid over-constraining queries that already use wildcard matching.
    use_exact = matched_pattern_idx == 2
    if use_exact:
        pred = lambda col: f"LOWER({col}) = LOWER('{safe_value}')"
    else:
        pred = lambda col: f"LOWER({col}) LIKE LOWER('%{safe_value}%')"

    if parent_col:
        condition = (
            f"({pred(matched_col)} "
            f"OR {pred(parent_col)})"
        )
        return [{
            "column": condition,
            "value": "",
            "raw": True
        }]

    return [{
        "column": pred(matched_col),
        "value": "",
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
    # Always attempt synonym-table based platform matching.
    # Keep explicit phrase presence guard below to avoid fuzzy over-filtering.
    platform, matched_phrase = find_platform_match(msg)
    # Only apply platform filter when the matched phrase is explicitly present in the message.
    # This avoids fuzzy false positives that can over-filter to zero rows.
    if platform and matched_phrase and _is_phrase_present(msg, matched_phrase):
        platform_condition = _build_platform_condition(schema_fields, platform, matched_phrase)
        filters.append({
            "column": platform_condition,
            "value": "",
            "raw": True
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
