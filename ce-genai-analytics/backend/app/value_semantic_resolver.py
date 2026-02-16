import re
from app.platform_mapping import find_platform_match

def extract_platform(message: str):
    msg_lower = message.lower()
    canonical, matched_phrase = find_platform_match(msg_lower)
    if canonical and matched_phrase:
        # remove matched phrase from message
        cleaned_message = re.sub(rf"\b{re.escape(matched_phrase)}\b", "", msg_lower, flags=re.IGNORECASE).strip()
        cleaned_message = re.sub(r"\s+", " ", cleaned_message)
        return canonical, cleaned_message

    return None, message


def _get_string_columns(schema_fields) -> set[str]:
    if not schema_fields:
        return set()

    string_cols = set()
    for field in schema_fields:
        if not isinstance(field, dict):
            continue
        name = field.get("name")
        dtype = str(field.get("type", "")).upper()
        if name and dtype in {"STRING"}:
            string_cols.add(str(name).lower())
    return string_cols


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip("`").lower()


def _derive_dimension_token(column_name: str) -> str | None:
    parts = [p for p in re.split(r"[^a-z0-9]+", column_name.lower()) if p]
    if not parts:
        return None
    joined = " ".join(parts)
    if "business" in joined and "line" in joined:
        return "business"
    if "campaign" in joined:
        return "campaign"
    if "creative" in joined or "ad" in joined:
        return "ad"
    return None


def _pick_hierarchy_columns(string_columns: set[str]) -> dict[str, str | None]:
    def pick_first(candidates):
        for c in sorted(string_columns):
            if candidates(c):
                return c
        return None

    business_col = pick_first(lambda c: "business" in c and "line" in c)
    campaign_col = pick_first(lambda c: "campaign" in c)
    ad_col = pick_first(lambda c: "ad" in c or "creative" in c)

    return {
        "business": business_col,
        "campaign": campaign_col,
        "ad": ad_col,
    }


def normalize_sql_value_semantics(sql: str, schema_fields=None) -> str:
    """
    Normalize string equality predicates to case-insensitive comparisons.
    Also applies hierarchy fallback for dimension phrases like
    '<value> campaigns' or '<value> ads' when parent dimensions exist.
    """
    string_columns = _get_string_columns(schema_fields)
    hierarchy = _pick_hierarchy_columns(string_columns)
    dimension_columns = {"business": [], "campaign": [], "ad": []}
    for c in sorted(string_columns):
        d = _derive_dimension_token(c)
        if d in dimension_columns:
            dimension_columns[d].append(c)

    def get_parent_col(dimension_token: str) -> str | None:
        if dimension_token == "ad":
            return hierarchy.get("campaign")
        if dimension_token == "campaign":
            return hierarchy.get("business")
        return None

    def maybe_add_hierarchy_fallback(
        raw_col: str,
        quote: str,
        raw_value: str,
        base_expr: str,
        op: str = "=",
    ) -> str:
        col_name = _normalize_identifier(raw_col.split(".")[-1])
        dimension_token = _derive_dimension_token(col_name) or ""
        if not dimension_token:
            return base_expr

        exprs = [base_expr]

        # Same-level dynamic fallback: campaign/ad filters should match
        # across all discovered sibling columns of that level.
        for sibling_col in dimension_columns.get(dimension_token, []):
            if sibling_col == col_name:
                continue
            if op.upper() == "LIKE":
                exprs.append(f"LOWER({sibling_col}) LIKE LOWER({quote}{raw_value}{quote})")
            else:
                exprs.append(f"LOWER({sibling_col}) = LOWER({quote}{raw_value}{quote})")

        parent_col = get_parent_col(dimension_token)
        if not parent_col:
            return f"({' OR '.join(exprs)})" if len(exprs) > 1 else base_expr

        value = raw_value.strip().strip("%").strip()
        if not value:
            return f"({' OR '.join(exprs)})" if len(exprs) > 1 else base_expr

        suffix_pattern = rf"\s+{re.escape(dimension_token)}s?$"
        parent_value = re.sub(suffix_pattern, "", value, flags=re.IGNORECASE).strip()
        if not parent_value:
            return f"({' OR '.join(exprs)})" if len(exprs) > 1 else base_expr

        parent_expr = f"LOWER({parent_col}) = LOWER({quote}{parent_value}{quote})"
        exprs.append(parent_expr)
        return f"({' OR '.join(exprs)})"

    eq_pattern = re.compile(
        r"(?P<col>(?:`[^`]+`|[a-zA-Z_][\w.]*))\s*=\s*(?P<q>['\"])(?P<val>[^'\"]+)(?P=q)",
        re.IGNORECASE,
    )

    def eq_repl(match: re.Match) -> str:
        raw_col = match.group("col")
        quote = match.group("q")
        value = match.group("val")
        col_name = _normalize_identifier(raw_col.split(".")[-1])
        if string_columns and col_name not in string_columns:
            return match.group(0)

        ci_expr = f"LOWER({raw_col}) = LOWER({quote}{value}{quote})"
        return maybe_add_hierarchy_fallback(raw_col, quote, value, ci_expr, op="=")

    sql = eq_pattern.sub(eq_repl, sql)

    like_pattern = re.compile(
        r"(?P<lhs>LOWER\(\s*(?P<col1>(?:`[^`]+`|[a-zA-Z_][\w.]*))\s*\)|(?P<col2>(?:`[^`]+`|[a-zA-Z_][\w.]*)))\s+LIKE\s+(?P<rhs>LOWER\(\s*(?P<q1>['\"])(?P<val1>[^'\"]+)(?P=q1)\s*\)|(?P<q2>['\"])(?P<val2>[^'\"]+)(?P=q2))",
        re.IGNORECASE,
    )

    def like_repl(match: re.Match) -> str:
        raw_col = match.group("col1") or match.group("col2")
        quote = match.group("q1") or match.group("q2")
        value = match.group("val1") or match.group("val2")
        col_name = _normalize_identifier(raw_col.split(".")[-1])
        if string_columns and col_name not in string_columns:
            return match.group(0)

        ci_expr = f"LOWER({raw_col}) LIKE LOWER({quote}{value}{quote})"
        return maybe_add_hierarchy_fallback(raw_col, quote, value, ci_expr, op="LIKE")

    return like_pattern.sub(like_repl, sql)
