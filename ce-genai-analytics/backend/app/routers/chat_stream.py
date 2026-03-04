from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
import asyncio
import json
import re
import time
from datetime import date, datetime

from app.schema_introspector import get_json_schema
from app.dynamic_sql_generator import generate_sql
from app.sql_normalizer import normalize_sql
from app.sql_validator import validate_sql
from app.executor import execute_sql
from app.render_builder import build_render_spec
from app.narrator import stream_narrative
from app.audit_service import AuditService
from app.logging_config import app_logger

from app.auth import get_current_user
from fastapi import Depends

import base64
from fastapi import HTTPException
from app.auth_service import AuthService

from app.value_semantic_resolver import extract_platform, normalize_sql_value_semantics
from app.filter_resolver import resolve_filters
from app.session_memory import (
    store_session_filters,
    get_session_filters,
    extract_filters_from_sql,
    clear_session,
    get_last_question,
    store_last_question,
    get_sql_context,
    store_sql_turn,
)

router = APIRouter()


GENERIC_BLOCK_MSG = "I can't perform that action that change system data. I can help with data analysis and retrieval."

def inject_condition(sql: str, condition: str) -> str:
    """
    Safely inject WHERE conditions.
    Handles:
    - No WHERE
    - Existing WHERE
    - Dangling WHERE
    - Trailing AND
    """

    sql = sql.strip().rstrip(";")

    # Remove broken WHERE at end
    sql = re.sub(r"\bWHERE\s*$", "", sql, flags=re.IGNORECASE)

    # Remove trailing AND
    sql = re.sub(r"\bAND\s*$", "", sql, flags=re.IGNORECASE)

    if re.search(r"\bwhere\b", sql, re.IGNORECASE):
        return f"{sql} AND {condition}"
    else:
        return f"{sql} WHERE {condition}"



def check_forbidden_intent(message: str):
    """Check if the user's message contains forbidden operations"""
    msg_lower = message.lower().strip()
    
    delete_patterns = [
        r'\b(delete|remove|purge|wipe|erase|destroy|obliterate)\b',
        r'\bclean\s+(up|out|the)',
        r'\bclear\s+(all|records|data|everything)',
        r'\b(get\s+rid\s+of|throw\s+away|discard)\b',
    ]
    
    update_patterns = [
        r'\b(update|modify|change|alter|edit|revise|replace|swap)\b',
        r'\b(set|assign)\s+',
    ]
    
    insert_patterns = [
        r'\b(insert|add|create|new|append|inject)\b.*\b(record|row|entry|data|into)\b',
        r'\badd\s+(new|record)',
    ]
    
    drop_patterns = [
        r'\b(drop|truncate|dismantle|demolish)\b',
    ]
    
    maintenance_patterns = [
        r'\bclean\s+the\s+(data|database|records)',
        r'\bfix\s+the\s+(database|data|table)',
        r'\borganize\s+(everything|the\s+data|records)',
        r'\brepair\s+the\s+(database|data)',
        r'\boptimize\s+the\s+(database|table)',
        r'\bdefragment',
        r'\barchive\s+(old|records|data)',
        r'\bcompress\s+',
        r'\bcleanup\s+',
    ]
    
    for pattern in delete_patterns:
        if re.search(pattern, msg_lower):
            if not re.search(r'\b(remove|exclude)\s+(filter|constraint|where|condition)\b', msg_lower):
                return GENERIC_BLOCK_MSG
    
    for pattern in update_patterns:
        if re.search(pattern, msg_lower):
            if not re.search(r'\b(update|change)\s+(query|view|chart|report|filter)\b', msg_lower):
                return GENERIC_BLOCK_MSG
    
    for pattern in insert_patterns:
        if re.search(pattern, msg_lower):
            return GENERIC_BLOCK_MSG
    
    for pattern in drop_patterns:
        if re.search(pattern, msg_lower):
            return GENERIC_BLOCK_MSG
    
    for pattern in maintenance_patterns:
        if re.search(pattern, msg_lower):
            return GENERIC_BLOCK_MSG
    
    return None

def event(fn: str, stage: str, etype: str) -> str:
    return f"{fn}:{stage}:{etype}"


def is_follow_up_message(message: str) -> bool:
    msg = message.lower().strip()
    follow_up_patterns = [
        r"^(what about)\b",
        r"^(how about)\b",
        r"^and\b",
        r"^also\b",
        r"^same\b",
        r"^instead\b",
        r"^(that|those|it|them)\b",
        r"^again\b",
        r"\b(this|that|it|them)\s+(number|value|result|metric|spend|cost)\b",
        r"^(break|split|group|bucket|show)\s+(this|that|it|them)\b",
        r"^(break|split|group|bucket)\b.*\b(this|that|it|them)\b.*\bby\b",
    ]
    return any(re.search(p, msg) for p in follow_up_patterns)


def is_temporal_follow_up_message(message: str) -> bool:
    msg = message.lower().strip()
    # Short temporal refinements that usually depend on previous context.
    patterns = [
        r"^(in|for)\s+q[1-4]\b",
        r"^(in|for)\s+\d{4}\b",
        r"^(in|for)\s+(today|yesterday|this|last)\b",
        r"^(today|yesterday|this|last)\b",
        r"\bin\s+q[1-4]\b",
        r"\bin\s+(20\d{2})\b",
    ]
    return any(re.search(p, msg) for p in patterns)


def is_short_metric_lookup_message(message: str) -> bool:
    msg = (message or "").strip().lower()
    if not msg:
        return False

    token_count = len([t for t in re.split(r"\s+", msg) if t])
    if token_count > 10:
        return False

    metric_markers = [
        "spend",
        "cost",
        "ctr",
        "cpc",
        "cpm",
        "clicks",
        "impressions",
        "enrollments",
        "enrollment rate",
        "revenue",
        "roas",
        "conversions",
        "leads",
    ]
    has_metric = any(m in msg for m in metric_markers)
    if not has_metric:
        return False

    explicit_scope_markers = [
        "campaign",
        "platform",
        "channel",
        "source",
        "state",
        "region",
        "market",
        "yesterday",
        "today",
        "last week",
        "this week",
        "last month",
        "this month",
    ]
    has_explicit_scope = any(m in msg for m in explicit_scope_markers) or bool(
        re.search(r"\bq[1-4]\b|\b20\d{2}\b", msg)
    )
    return not has_explicit_scope


def is_format_only_follow_up_message(message: str) -> bool:
    msg = (message or "").lower().strip()
    if not msg:
        return False

    format_markers = [
        "executive summary",
        "paragraph form",
        "single paragraph",
        "in paragraph",
        "rewrite",
        "rephrase",
        "summarize this",
        "make it concise",
        "short summary",
        "bullet points",
        "format this",
        "narrative",
    ]
    scope_markers = [
        "this",
        "that",
        "same",
        "above",
        "previous",
        "follow up",
        "follow-up",
    ]

    has_format_intent = any(m in msg for m in format_markers)
    has_scope_reference = any(m in msg for m in scope_markers)

    # Keep this strict so normal analytical questions are not misclassified.
    return has_format_intent and (has_scope_reference or len(msg.split()) <= 14)


def is_temporal_condition(condition: str) -> bool:
    c = condition.lower()
    temporal_patterns = [
        r"\bdate\b",
        r"\bcurrent_date\(",
        r"\bdate_sub\(",
        r"\bextract\s*\(\s*(year|quarter|month|week|day)",
        r"\bbetween\b",
        r"\bq[1-4]\b",
        r"\b\d{4}\b",
    ]
    return any(re.search(p, c) for p in temporal_patterns)


def dedupe_conditions(conditions: list[str]) -> list[str]:
    seen = set()
    out = []
    for c in conditions:
        key = c.strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def is_total_null_artifact(rows: list[dict]) -> bool:
    """
    Detect result shape like:
      [{'campaign_name': 'TOTAL', metric1: None, metric2: None, ...}]
    which usually indicates an empty inner CTE with a total aggregation wrapper.
    """
    if not rows or len(rows) != 1 or not isinstance(rows[0], dict):
        return False
    row = rows[0]
    keys = list(row.keys())
    if not keys:
        return False

    label_keys = {"campaign_name", "platform", "source", "datasource", "channel", "dimension", "group_name"}
    label_val = None
    for k in keys:
        if str(k).strip().lower() in label_keys:
            label_val = row.get(k)
            break
    if str(label_val or "").strip().upper() != "TOTAL":
        return False

    for k, v in row.items():
        if str(k).strip().lower() in label_keys:
            continue
        if v is not None:
            return False
    return True


def build_sql_follow_up_context_prompt(
    current_message: str,
    history: list[dict],
    last_sql: str | None,
    schema_fields,
    last_result_columns: list[str] | None = None,
) -> str:
    parts = [
        "Current user question:",
        current_message,
    ]

    if last_sql:
        parts.extend(
            [
                "",
                "Previous SQL from this session (use as context, adapt as needed):",
                last_sql,
            ]
        )

    if schema_fields is not None:
        parts.extend(
            [
                "",
                "Session schema context:",
                str(schema_fields),
            ]
        )

    if isinstance(last_result_columns, list) and last_result_columns:
        parts.extend(
            [
                "",
                "Previous result columns from this session:",
                str(last_result_columns),
            ]
        )

    if history:
        lines = []
        for m in history[-12:]:
            role = m.get("role")
            content = (m.get("content") or "").strip()
            if role in {"user", "assistant"} and content:
                lines.append(f"{role}: {content}")
        if lines:
            parts.extend(
                [
                    "",
                    "Recent conversation history (oldest to newest):",
                    "\n".join(lines),
                ]
            )

    parts.extend(
        [
            "",
            "Generate SQL for the current user question while preserving follow-up context from prior turns.",
        ]
    )
    return "\n".join(parts)


def _safe_float_value(val):
    if val is None:
        return None
    try:
        if isinstance(val, str):
            cleaned = val.replace("$", "").replace("%", "").replace(",", "").strip()
            if cleaned == "":
                return None
            return float(cleaned)
        return float(val)
    except Exception:
        return None


def _find_metric_key(row: dict, candidates: list[str]) -> str | None:
    lowered = {str(k).strip().lower(): k for k in row.keys()}
    for c in candidates:
        if c in lowered:
            return lowered[c]
    return None


def _has_metric_intent(message: str) -> bool:
    msg = (message or "").lower()
    metric_markers = [
        "spend",
        "cost",
        "ctr",
        "cpc",
        "cpm",
        "clicks",
        "impressions",
        "enrollments",
        "enrollment rate",
        "revenue",
        "roas",
        "conversions",
        "leads",
    ]
    return any(m in msg for m in metric_markers)


def _extract_temporal_markers(message: str) -> set[str]:
    msg = (message or "").lower()
    markers = set()
    for q in re.findall(r"\bq[1-4]\b", msg):
        markers.add(q)
    for y in re.findall(r"\b20\d{2}\b", msg):
        markers.add(y)
    phrases = [
        "last week",
        "this week",
        "last month",
        "this month",
        "month-to-date",
        "mtd",
        "ytd",
        "today",
        "yesterday",
    ]
    for p in phrases:
        if p in msg:
            markers.add(p)
    return markers


def _scope_compatible_with_prior(message: str, prior_question: str | None) -> bool:
    if not isinstance(prior_question, str) or not prior_question.strip():
        return False
    current_markers = _extract_temporal_markers(message)
    if not current_markers:
        return True
    prior_markers = _extract_temporal_markers(prior_question)
    if not prior_markers:
        return False
    return current_markers.issubset(prior_markers)


def _remembered_question_for_turn(
    message: str,
    prior_question: str | None,
    *,
    metric_intent: bool,
    short_metric_lookup: bool,
    format_only: bool,
) -> str:
    """
    Preserve analytical scope anchor for short metric/format follow-ups so
    subsequent turns (e.g., '... in Q4') keep the original scoped context.
    """
    if prior_question and (short_metric_lookup or format_only or (metric_intent and len(message.split()) <= 12)):
        return prior_question
    return message


def _norm_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (value or "").lower())).strip()


def _find_mentioned_group_labels(message: str, rows: list[dict], label_key: str | None) -> list[str]:
    if not label_key or not rows:
        return []
    msg = _norm_text(message or "")
    if not msg:
        return []
    labels = []
    for r in rows:
        raw = str(r.get(label_key, "")).strip()
        if not raw:
            continue
        if raw.lower() in {"total", "all"}:
            continue
        n = _norm_text(raw)
        if not n:
            continue
        n_core = re.sub(r"\b(campaign|campaigns)\b", "", n).strip()
        # Strict contains first, then tolerant token subset for labels with extra suffix words.
        if n in msg or (n_core and n_core in msg):
            labels.append(raw)
            continue
    # preserve order, dedupe
    out = []
    seen = set()
    for l in labels:
        k = _norm_text(l)
        if k in seen:
            continue
        seen.add(k)
        out.append(l)
    return out


def _filter_rows_by_labels(rows: list[dict], label_key: str | None, labels: list[str]) -> list[dict]:
    if not rows or not label_key or not labels:
        return rows
    wanted = {_norm_text(v) for v in labels}
    out = []
    for r in rows:
        rv = _norm_text(str(r.get(label_key, "")))
        if rv in wanted:
            out.append(r)
    return out


def build_metric_lookup_response(message: str, rows: list[dict]) -> str | None:
    if not rows or not isinstance(rows[0], dict):
        return None
    msg = (message or "").lower()
    sample = rows[0]

    label_key = _find_metric_key(
        sample,
        ["campaign_name", "platform", "source", "datasource", "channel", "dimension", "group_name"],
    )
    total_rows = []
    detail_rows = rows
    if label_key:
        total_rows = [r for r in rows if str(r.get(label_key, "")).strip().lower() == "total"]
        detail_rows = [r for r in rows if str(r.get(label_key, "")).strip().lower() != "total"]
    mentioned_labels = _find_mentioned_group_labels(message, detail_rows if detail_rows else rows, label_key)
    if mentioned_labels and label_key:
        wanted = {_norm_text(v) for v in mentioned_labels}
        scoped_rows = [
            r for r in (detail_rows if detail_rows else rows)
            if _norm_text(str(r.get(label_key, ""))) in wanted
        ]
        if scoped_rows:
            detail_rows = scoped_rows
            total_rows = []

    def sum_col(key: str | None):
        if not key:
            return None
        for tr in total_rows:
            v = _safe_float_value(tr.get(key))
            if v is not None:
                return v
        # If rows were narrowed for a follow-up (e.g., specific campaigns),
        # aggregate over narrowed detail_rows even when TOTAL row is absent.
        source_rows = detail_rows if detail_rows else rows
        total = 0.0
        seen = False
        for r in source_rows:
            v = _safe_float_value(r.get(key))
            if v is None:
                continue
            total += v
            seen = True
        return total if seen else None

    spend_key = _find_metric_key(sample, ["total_spend", "spend", "amount_spent", "ad_spend"])
    clicks_key = _find_metric_key(sample, ["total_clicks", "clicks"])
    impr_key = _find_metric_key(sample, ["total_impressions", "impressions"])
    enroll_key = _find_metric_key(sample, ["total_enrollments", "enrollments", "total_enrollment", "enrollment_count"])

    t_spend = sum_col(spend_key)
    t_clicks = sum_col(clicks_key)
    t_impr = sum_col(impr_key)
    t_enroll = sum_col(enroll_key)

    lines = []
    spend_intent = any(k in msg for k in ["total spend", "spend", "total cost", "cost"])
    max_intent = any(k in msg for k in ["max spend", "maximum spend", "highest spend", "top spend", "most spend"])
    min_intent = any(k in msg for k in ["min spend", "minimum spend", "lowest spend", "least spend"])
    if spend_intent:
        if max_intent:
            source_rows = detail_rows if detail_rows else rows
            best_row = None
            best_val = None
            for r in source_rows:
                v = _safe_float_value(r.get(spend_key)) if spend_key else None
                if v is None:
                    continue
                if best_val is None or v > best_val:
                    best_val = v
                    best_row = r
            if best_val is None:
                lines.append("Maximum Spend: N/A")
            else:
                lines.append(f"Maximum Spend: ${best_val:,.2f}")
                if best_row is not None and label_key:
                    lbl = str(best_row.get(label_key, "")).strip()
                    if lbl:
                        lines.append(f"Campaign/Group: {lbl}")
        elif min_intent:
            source_rows = detail_rows if detail_rows else rows
            best_row = None
            best_val = None
            for r in source_rows:
                v = _safe_float_value(r.get(spend_key)) if spend_key else None
                if v is None:
                    continue
                if best_val is None or v < best_val:
                    best_val = v
                    best_row = r
            if best_val is None:
                lines.append("Minimum Spend: N/A")
            else:
                lines.append(f"Minimum Spend: ${best_val:,.2f}")
                if best_row is not None and label_key:
                    lbl = str(best_row.get(label_key, "")).strip()
                    if lbl:
                        lines.append(f"Campaign/Group: {lbl}")
        elif mentioned_labels:
            labels_text = " and ".join(mentioned_labels)
            lines.append(
                f"Combined Total Spend: ${t_spend:,.2f}"
                if t_spend is not None
                else "Combined Total Spend: N/A"
            )
            if label_key and t_spend is not None:
                for lbl in mentioned_labels:
                    rv = None
                    for r in detail_rows:
                        if _norm_text(str(r.get(label_key, ""))) == _norm_text(lbl):
                            rv = _safe_float_value(r.get(spend_key))
                            break
                    if rv is not None:
                        pretty_label = re.sub(r"_+", " ", lbl).strip()
                        lines.append(f"{pretty_label}: ${rv:,.2f}")
            # Clarify explicit inclusion scope when selected groups are a subset.
            all_labels = []
            for r in (rows or []):
                raw = str(r.get(label_key, "")).strip() if label_key else ""
                if raw and raw.lower() not in {"total", "all"}:
                    all_labels.append(raw)
            all_norm = {_norm_text(x) for x in all_labels}
            sel_norm = {_norm_text(x) for x in mentioned_labels}
            if all_norm and sel_norm and len(sel_norm) < len(all_norm):
                lines.append(
                    f"This includes only {labels_text} and excludes other campaigns in this result set."
                )
        else:
            lines.append(f"Total Spend: ${t_spend:,.2f}" if t_spend is not None else "Total Spend: N/A")
    if any(k in msg for k in ["clicks", "total clicks"]):
        lines.append(f"Total Clicks: {t_clicks:,.0f}" if t_clicks is not None else "Total Clicks: N/A")
    if any(k in msg for k in ["impressions", "total impressions"]):
        lines.append(f"Total Impressions: {t_impr:,.0f}" if t_impr is not None else "Total Impressions: N/A")
    if any(k in msg for k in ["ctr", "click through rate", "click-through rate"]):
        if t_clicks is not None and t_impr not in (None, 0):
            lines.append(f"CTR: {((t_clicks / t_impr) * 100):.2f}%")
        else:
            lines.append("CTR: N/A")
    if any(k in msg for k in ["enrollment rate"]):
        if t_enroll is not None and t_clicks not in (None, 0):
            lines.append(f"Enrollment Rate: {((t_enroll / t_clicks) * 100):.2f}%")
        else:
            lines.append("Enrollment Rate: N/A")

    if not lines:
        return None
    return "\n".join(dict.fromkeys(lines))


def _is_invalid_temporal_condition(condition: str) -> bool:
    c = condition or ""
    patterns = [
        r"\bTIMESTAMP\s*\(\s*['\"]\s*['\"]\s*\)",
        r"\bDATE\s*\(\s*['\"]\s*['\"]\s*\)",
        r"\bDATETIME\s*\(\s*['\"]\s*['\"]\s*\)",
        r"\bPARSE_TIMESTAMP\s*\([^,]+,\s*['\"]\s*['\"]\s*\)",
        r"\bPARSE_DATE\s*\([^,]+,\s*['\"]\s*['\"]\s*\)",
        r"\bPARSE_DATETIME\s*\([^,]+,\s*['\"]\s*['\"]\s*\)",
        r"\b(CAST|SAFE_CAST)\s*\(\s*['\"]\s*['\"]\s+AS\s+(TIMESTAMP|DATE|DATETIME)\s*\)",
        r"\bBETWEEN\s*''\s*AND\s*''",
        r"\bBETWEEN\s*\"\"\s*AND\s*\"\"",
        r"\bBETWEEN\s*''\s*AND\b",
        r"\bBETWEEN\s*\"\"\s*AND\b",
        r"\bBETWEEN\b.*\bAND\s*''\b",
        r"\bBETWEEN\b.*\bAND\s*\"\"\b",
        r"\b(date|time|timestamp|datetime)\b[^=<>!]*\s*(=|!=|<>|>=|<=|>|<)\s*''",
        r"\b(date|time|timestamp|datetime)\b[^=<>!]*\s*(=|!=|<>|>=|<=|>|<)\s*\"\"",
        r"''\s*(=|!=|<>|>=|<=|>|<)\s*[^ \n]*(date|time|timestamp|datetime)",
        r"\"\"\s*(=|!=|<>|>=|<=|>|<)\s*[^ \n]*(date|time|timestamp|datetime)",
        r"\b(date|time|timestamp)\b.*=\s*''",
        r"\b(date|time|timestamp)\b.*=\s*\"\"",
        r"=\s*''.*\b(date|time|timestamp)\b",
        r"=\s*\"\".*\b(date|time|timestamp)\b",
    ]
    return any(re.search(p, c, re.IGNORECASE) for p in patterns)


def strip_invalid_temporal_conditions(sql: str) -> str:
    """
    Remove malformed temporal predicates such as TIMESTAMP('') from WHERE.
    Keeps the rest of the query intact.
    """
    if not sql or not isinstance(sql, str):
        return sql

    raw = sql.strip().rstrip(";")

    # First, neutralize empty temporal literals anywhere in SQL so BigQuery won't fail.
    # Examples: TIMESTAMP(''), DATE(""), DATETIME(''), CAST('' AS TIMESTAMP), SAFE_CAST("" AS DATE)
    raw = re.sub(
        r"\b(TIMESTAMP|DATE|DATETIME)\s*\(\s*(['\"])\s*\2\s*\)",
        "NULL",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bSAFE_CAST\s*\(\s*(['\"])\s*\1\s+AS\s+(TIMESTAMP|DATE|DATETIME)\s*\)",
        "NULL",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bCAST\s*\(\s*(['\"])\s*\1\s+AS\s+(TIMESTAMP|DATE|DATETIME)\s*\)",
        "NULL",
        raw,
        flags=re.IGNORECASE,
    )
    # Normalize empty string temporal literals in direct comparisons to NULL.
    raw = re.sub(
        r"(\b(?:date|time|timestamp|datetime)\b[^=<>!\n]*\s*(?:=|!=|<>|>=|<=|>|<)\s*)''",
        r"\1NULL",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"(\b(?:date|time|timestamp|datetime)\b[^=<>!\n]*\s*(?:=|!=|<>|>=|<=|>|<)\s*)\"\"",
        r"\1NULL",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bBETWEEN\s*''\s*AND",
        "BETWEEN NULL AND",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bBETWEEN\s*\"\"\s*AND",
        "BETWEEN NULL AND",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bAND\s*''\b",
        "AND NULL",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bAND\s*\"\"\b",
        "AND NULL",
        raw,
        flags=re.IGNORECASE,
    )
    # Handle parse functions with empty temporal input, e.g. PARSE_TIMESTAMP('%F', '').
    raw = re.sub(
        r"\bPARSE_TIMESTAMP\s*\(\s*[^,]+,\s*(['\"])\s*\1\s*\)",
        "NULL",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bPARSE_DATE\s*\(\s*[^,]+,\s*(['\"])\s*\1\s*\)",
        "NULL",
        raw,
        flags=re.IGNORECASE,
    )
    raw = re.sub(
        r"\bPARSE_DATETIME\s*\(\s*[^,]+,\s*(['\"])\s*\1\s*\)",
        "NULL",
        raw,
        flags=re.IGNORECASE,
    )
    split_match = re.search(r"\b(GROUP BY|ORDER BY|LIMIT|HAVING)\b", raw, re.IGNORECASE)
    base_sql = raw if not split_match else raw[:split_match.start()]
    tail_sql = "" if not split_match else raw[split_match.start():]

    where_match = re.search(r"\bWHERE\b(?P<body>.*)$", base_sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return raw

    prefix = base_sql[:where_match.start()].rstrip()
    body = where_match.group("body")
    parts = [p.strip() for p in re.split(r"\bAND\b", body, flags=re.IGNORECASE) if p.strip()]
    valid_parts = [p for p in parts if not _is_invalid_temporal_condition(p)]

    if valid_parts:
        rebuilt = f"{prefix} WHERE " + " AND ".join(valid_parts)
    else:
        rebuilt = prefix

    return (rebuilt + ("\n" + tail_sql if tail_sql else "")).strip()


def scrub_sql_for_invalid_timestamp(sql: str) -> str:
    """
    Last-resort sanitization used only when BigQuery returns:
    Invalid timestamp: ''
    """
    if not sql or not isinstance(sql, str):
        return sql

    cleaned = strip_invalid_temporal_conditions(sql)
    cleaned = re.sub(
        r"\b(TIMESTAMP|DATE|DATETIME)\s*\(\s*(['\"])\s*\2\s*\)",
        "NULL",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(PARSE_TIMESTAMP|PARSE_DATE|PARSE_DATETIME)\s*\(\s*[^,]+,\s*(['\"])\s*\2\s*\)",
        "NULL",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(CAST|SAFE_CAST)\s*\(\s*(['\"])\s*\2\s+AS\s+(TIMESTAMP|DATE|DATETIME)\s*\)",
        "NULL",
        cleaned,
        flags=re.IGNORECASE,
    )
    # If DATE(date_col) fails due to malformed string values in the source,
    # rewrite to SAFE_CAST(date_col AS DATE) for tolerant filtering.
    cleaned = re.sub(
        r"\bDATE\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)",
        r"SAFE_CAST(\1 AS DATE)",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned

@router.get("/chat/stream")
async def chat_stream(
    request: Request,
    message: str,
    conversation_id: str | None = None,
    auth: str | None = None
):
    conversation_id = conversation_id or request.query_params.get("conversation_id") or request.headers.get("X-Conversation-ID") or f"conv-unknown"
    
    if not auth:
        raise HTTPException(status_code=401, detail="Missing auth")

    try:
        decoded = base64.b64decode(auth).decode()
        email, password = decoded.split(":", 1)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid auth format")

    user = AuthService.authenticate_user(email, password)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user_id = user.get("user_id")

    
    '''user_id = None
    try:
        uid = request.headers.get("X-User-ID")
        if uid:
            user_id = int(uid)
    except Exception:
        user_id = None'''
    # user_id = user.get("user_id")
    
    endpoint = request.url.path
    method = request.method
    full_response_text = []
    async def event_generator():
        start_time = time.time()
        narrative_session_id = conversation_id
        
        # Audit state - collect data without logging yet
        audit_data = {
            "conversationid": conversation_id,
            "userid": user_id,
            "endpoint": endpoint,
            "httpmethod": method,
            "usermessage": message[:2000],
            "generatedsql": None,
            "rowsreturned": 0,
            "durationms": 0,
            "sqlstatus": None,
            "errortype": None,
            "errormessage": None,
            "eventtype": None,
            "response": None,
            "responsestatus": 200,  # default success
            "responsedurationms": 0
        }
        
        try:
            # Check forbidden intent
            intent_error = check_forbidden_intent(message)
            if intent_error:
                #add function name to log
                #audit_data["event_type"] = "VALIDATION_ERROR"
                audit_data["eventtype"] = event("event_generator", "check_forbidden_intent", "FORBIDDEN_OPERATION")
                audit_data["sqlstatus"] = "BLOCKED"
                audit_data["errortype"] = "FORBIDDEN_OPERATION"
                audit_data["errormessage"] = intent_error
                audit_data["responsestatus"] = 403 
                audit_data["durationms"] = int((time.time() - start_time) * 1000)
                audit_data["responsedurationms"] = audit_data["durationms"]
                audit_data["response"] = json.dumps({"status": "error", "type": "FORBIDDEN_OPERATION", "response_text": intent_error})[:2000]
                #audit_data["response"] = "".join(full_response_text)[:8000] if full_response_text else str(intent_error)[:2000]
    
                # Log once with error
                AuditService.log_audit_event(**audit_data)
                
                err = {"error": True, "message": intent_error, "type": "validation_error"}
                yield f"event: validation_error\ndata: {json.dumps(err)}\n\n"
                return

            # ----------------------------------
            # 1. Generate base SQL
            # ----------------------------------
            json_fields = get_json_schema()
            prior_question = get_last_question(user_id, conversation_id)
            session_ctx = get_sql_context(user_id, conversation_id)
            history = session_ctx.get("history", []) if isinstance(session_ctx, dict) else []
            last_sql = session_ctx.get("last_sql") if isinstance(session_ctx, dict) else None
            stored_schema = session_ctx.get("schema") if isinstance(session_ctx, dict) else None
            last_result_columns = session_ctx.get("last_result_columns", []) if isinstance(session_ctx, dict) else []
            last_rows = session_ctx.get("last_rows", []) if isinstance(session_ctx, dict) else []
            short_metric_lookup = is_short_metric_lookup_message(message)
            metric_intent = _has_metric_intent(message)
            format_only_follow_up = is_format_only_follow_up_message(message)
            scope_compatible_with_prior = _scope_compatible_with_prior(message, prior_question)
            label_ref_in_last_rows = False
            mentioned_labels_in_last_rows = []
            lr_label_key = None
            if isinstance(last_rows, list) and last_rows and isinstance(last_rows[0], dict):
                lr_label_key = _find_metric_key(
                    last_rows[0],
                    ["campaign_name", "platform", "source", "datasource", "channel", "dimension", "group_name"],
                )
                mentioned_labels_in_last_rows = _find_mentioned_group_labels(message, last_rows, lr_label_key)
                label_ref_in_last_rows = bool(mentioned_labels_in_last_rows)
            is_contextual_follow_up = (
                is_follow_up_message(message)
                or is_temporal_follow_up_message(message)
                or format_only_follow_up
                or short_metric_lookup
                or (metric_intent and label_ref_in_last_rows and bool(prior_question))
                or (len(message.split()) < 12 and bool(prior_question))
            )

            # Priority 0: short entity-only follow-up (e.g., "how about GA Atlanta NB?")
            # Reuse prior result rows and narrow to the mentioned label(s).
            if (
                not metric_intent
                and label_ref_in_last_rows
                and isinstance(last_rows, list)
                and last_rows
                and len(message.split()) <= 12
            ):
                scoped_rows = _filter_rows_by_labels(last_rows, lr_label_key, mentioned_labels_in_last_rows)
                if scoped_rows:
                    render_spec = build_render_spec(message, scoped_rows)
                    focus_prompt = (
                        f"{prior_question}\n"
                        f"Follow-up focus: {message}\n"
                        "Use the same analytical scope and report only for the requested campaign/group."
                        if prior_question else message
                    )
                    audit_data["generatedsql"] = "[SESSION_RESULT_REUSE_ENTITY]"
                    audit_data["rowsreturned"] = len(scoped_rows)
                    audit_data["sqlstatus"] = "SUCCESS"
                    full_response_text.append(json.dumps(scoped_rows, default=json_safe))
                    yield f"event: render\ndata: {json.dumps(render_spec, default=json_safe)}\n\n"
                    await asyncio.sleep(0.01)
                    assistant_text_parts = []
                    async for token in stream_narrative(
                        session_id=narrative_session_id,
                        question=focus_prompt,
                        rows=scoped_rows,
                        render_spec=render_spec,
                        conversation_history=history,
                        last_sql=last_sql,
                    ):
                        assistant_text_parts.append(token)
                        full_response_text.append(token)
                        yield f"data: {token}\n\n"
                        await asyncio.sleep(0)
                    yield "event: done\ndata: [DONE]\n\n"
                    total_duration = int((time.time() - start_time) * 1000)
                    audit_data["durationms"] = total_duration
                    audit_data["responsestatus"] = 200
                    audit_data["responsedurationms"] = total_duration
                    audit_data["response"] = json.dumps({
                        "status": "success",
                        "rowsreturned": len(scoped_rows),
                        "sql_execution_time_ms": 0,
                        "total_response_time_ms": total_duration,
                        "response_text": "".join(full_response_text),
                    }, default=json_safe)
                    remember_q = _remembered_question_for_turn(
                        message,
                        prior_question,
                        metric_intent=metric_intent,
                        short_metric_lookup=short_metric_lookup,
                        format_only=format_only_follow_up,
                    )
                    store_last_question(user_id, conversation_id, remember_q)
                    store_sql_turn(
                        user_id=user_id,
                        conversation_id=conversation_id,
                        user_message=message,
                        assistant_message="".join(assistant_text_parts),
                        sql=last_sql or "[SESSION_RESULT_REUSE_ENTITY]",
                        schema=stored_schema or json_fields,
                        rows=last_rows if isinstance(last_rows, list) and last_rows else scoped_rows,
                    )
                    AuditService.log_audit_event(**audit_data)
                    return

            # Priority 1: answer short metric follow-up directly from stored result rows.
            can_answer_metric_from_memory = (
                short_metric_lookup
                or label_ref_in_last_rows
                or (len(message.split()) <= 12 and scope_compatible_with_prior)
            )
            if metric_intent and isinstance(last_rows, list) and last_rows and can_answer_metric_from_memory:
                direct_metric = build_metric_lookup_response(message, last_rows)
                if direct_metric:
                    render_spec = build_render_spec(message, last_rows)
                    audit_data["generatedsql"] = "[SESSION_RESULT_REUSE_METRIC]"
                    audit_data["rowsreturned"] = len(last_rows)
                    audit_data["sqlstatus"] = "SUCCESS"
                    full_response_text.append(json.dumps(last_rows, default=json_safe))
                    yield f"event: render\ndata: {json.dumps(render_spec, default=json_safe)}\n\n"
                    await asyncio.sleep(0.01)
                    full_response_text.append(direct_metric)
                    yield f"data: {direct_metric}\n\n"
                    await asyncio.sleep(0)
                    yield "event: done\ndata: [DONE]\n\n"
                    total_duration = int((time.time() - start_time) * 1000)
                    audit_data["durationms"] = total_duration
                    audit_data["responsestatus"] = 200
                    audit_data["responsedurationms"] = total_duration
                    audit_data["response"] = json.dumps({
                        "status": "success",
                        "rowsreturned": len(last_rows),
                        "sql_execution_time_ms": 0,
                        "total_response_time_ms": total_duration,
                        "response_text": "".join(full_response_text),
                    }, default=json_safe)
                    remember_q = _remembered_question_for_turn(
                        message,
                        prior_question,
                        metric_intent=metric_intent,
                        short_metric_lookup=short_metric_lookup,
                        format_only=format_only_follow_up,
                    )
                    store_last_question(user_id, conversation_id, remember_q)
                    store_sql_turn(
                        user_id=user_id,
                        conversation_id=conversation_id,
                        user_message=message,
                        assistant_message=direct_metric,
                        sql=last_sql or "[SESSION_RESULT_REUSE_METRIC]",
                        schema=stored_schema or json_fields,
                        rows=last_rows if isinstance(last_rows, list) and last_rows else rows,
                    )
                    AuditService.log_audit_event(**audit_data)
                    return

            effective_message = message
            narrative_message = message
            if prior_question and format_only_follow_up:
                effective_message = prior_question
                narrative_message = (
                    f"{prior_question}\n\n"
                    f"Follow-up formatting instruction: {message}\n"
                    "Keep the same analytical scope as the prior question and only change response format/style."
                )

            platform, cleaned_message = extract_platform(effective_message)
            sql_prompt = cleaned_message
            # Priority 2: short metric follow-up should modify previous SQL, preserving filters.
            if short_metric_lookup and last_sql:
                sql_prompt = (
                    f"Previous SQL:\n{last_sql}\n\n"
                    f"Follow-up user question: {cleaned_message}\n\n"
                    "Modify the previous SQL to return only the requested metric. "
                    "Do NOT remove existing WHERE filters."
                )
            if is_contextual_follow_up and (history or last_sql or stored_schema or last_result_columns):
                sql_prompt = build_sql_follow_up_context_prompt(
                    current_message=cleaned_message,
                    history=history,
                    last_sql=last_sql,
                    schema_fields=stored_schema or json_fields,
                    last_result_columns=last_result_columns,
                )
            # Keep the previous-SQL instruction dominant for short metric follow-ups.
            if short_metric_lookup and last_sql:
                sql_prompt = (
                    f"Previous SQL:\n{last_sql}\n\n"
                    f"Follow-up user question: {cleaned_message}\n\n"
                    "Modify the previous SQL to return only the requested metric. "
                    "Do NOT remove existing WHERE filters."
                )

            generated_sql = generate_sql(sql_prompt, json_fields)
            sql = normalize_sql_value_semantics(generated_sql, json_fields)

            # ----------------------------------
            # 2. Collect filters
            # ----------------------------------
            new_filters = resolve_filters(effective_message, json_fields)
            previous_filters = get_session_filters(user_id, conversation_id)
            current_sql_filters = extract_filters_from_sql(sql)
            has_non_temporal_sql_filters = any(
                not is_temporal_condition(c) for c in current_sql_filters
            )

            conditions = []

            # Reuse session filters only for explicit follow-up/contextual turns.
            if (
                not new_filters
                and previous_filters
                and (
                    is_contextual_follow_up
                    or (is_temporal_follow_up_message(message) and not has_non_temporal_sql_filters)
                )
            ):
                conditions.extend(previous_filters)

            # Add new filters
            for f in new_filters:
                column = f["column"]
                value = f["value"]
                raw = f.get("raw", False)

                if raw:
                    if value is None or str(value).strip() == "":
                        conditions.append(f"{column}")
                    else:
                        conditions.append(f"{column} = {value}")
                else:
                    conditions.append(f"{column} = '{value}'")

            # ----------------------------------
            # 3. Inject filters safely
            # ----------------------------------
            base_sql = sql
            had_injected_conditions = False
            if conditions:
                conditions = dedupe_conditions(conditions)
                sql = inject_filters_safely(sql, conditions)
                had_injected_conditions = len(conditions) > 0

            # Remove malformed temporal predicates such as TIMESTAMP('').
            sql = strip_invalid_temporal_conditions(sql)

            # ----------------------------------
            # 4. Store filters for next turn
            # ----------------------------------
            final_conditions = dedupe_conditions(extract_filters_from_sql(sql))
            if final_conditions:
                store_session_filters(user_id, conversation_id, final_conditions)



            audit_data["generatedsql"] = sql[:4000]

            # Validate SQL
            try:
                validate_sql(sql)
            except ValueError as ve:
                #audit_data["event_type"] = "VALIDATION_ERROR"
                audit_data["eventtype"] = event("event_generator", "validate_sql", "VALIDATION_ERROR")
                audit_data["sqlstatus"] = "INVALID"
                audit_data["errortype"] = "SQL_VALIDATION_FAILED"
                audit_data["errormessage"] = str(ve)[:2000]
                audit_data["responsestatus"] = 400
                audit_data["durationms"] = int((time.time() - start_time) * 1000)
                audit_data["responsedurationms"] = audit_data["durationms"]
                audit_data["response"] = json.dumps({"status": "error", "type": "validation_failed", "response_text": str(ve)[:500]})[:2000]
                #audit_data["response"] = "".join(full_response_text)[:8000] if full_response_text else str(ve)[:2000]

                # Log once with error
                AuditService.log_audit_event(**audit_data)
                
                err = {"error": True, "message": str(ve), "type": "validation_error"}
                yield f"event: validation_error\ndata: {json.dumps(err)}\n\n"
                return

            # Execute SQL
            exec_start = time.time()
            try:
                rows = execute_sql(sql, {})
                if (not rows) or is_total_null_artifact(rows):
                    # Fallback order when transformed SQL is over-constrained:
                    # 1) remove injected dynamic/session filters,
                    # 2) use raw generated SQL before semantic normalization.
                    fallback_sqls = []
                    if had_injected_conditions:
                        fallback_sqls.append(strip_invalid_temporal_conditions(base_sql))
                    fallback_sqls.append(strip_invalid_temporal_conditions(generated_sql))

                    for fsql in fallback_sqls:
                        if not fsql or fsql.strip() == sql.strip():
                            continue
                        fallback_rows = execute_sql(fsql, {})
                        if fallback_rows and not is_total_null_artifact(fallback_rows):
                            rows = fallback_rows
                            sql = fsql
                            audit_data["generatedsql"] = sql[:4000]
                            break
            except Exception as ex:
                err_text = str(ex)
                if "Invalid timestamp: ''" in err_text or "Invalid date" in err_text or "Invalid timestamp" in err_text:
                    # Drop stale/bad temporal artifacts and retry once.
                    clear_session(user_id, conversation_id)
                    sql = scrub_sql_for_invalid_timestamp(sql)
                    audit_data["generatedsql"] = sql[:4000]
                    rows = execute_sql(sql, {})
                elif "UNION ALL has incompatible types" in err_text or "incompatible types" in err_text and "UNION ALL" in err_text:
                    # Retry with BigQuery UNION ALL BY NAME to avoid positional column mismatches.
                    sql_by_name = re.sub(r"\bUNION\s+ALL\b", "UNION ALL BY NAME", sql, flags=re.IGNORECASE)
                    if sql_by_name != sql:
                        sql = sql_by_name
                        audit_data["generatedsql"] = sql[:4000]
                        rows = execute_sql(sql, {})
                    else:
                        raise
                else:
                    raise

            exec_duration = int((time.time() - exec_start) * 1000)
            
            audit_data["rowsreturned"] = len(rows)
            audit_data["eventtype"] = event("event_generator", "execute_sql", "SQL_EXECUTED")
            audit_data["sqlstatus"] = "SUCCESS"
            audit_data["eventtype"] = "SQL_EXECUTED"
            audit_data["durationms"] = exec_duration 
            
            # Build render spec
            render_spec = build_render_spec(message, rows)
            full_response_text.append(json.dumps(rows, default=json_safe))
            yield f"event: render\ndata: {json.dumps(render_spec, default=json_safe)}\n\n"
            await asyncio.sleep(0.01)

            # Stream narrative tokens
            assistant_text_parts = []
            async for token in stream_narrative(
                session_id=narrative_session_id,
                question=narrative_message,
                rows=rows,
                render_spec=render_spec,
                conversation_history=history,
                last_sql=last_sql,
            ):
                assistant_text_parts.append(token)
                full_response_text.append(token) 
                yield f"data: {token}\n\n"
                await asyncio.sleep(0)

            yield "event: done\ndata: [DONE]\n\n"
            
            total_duration = int((time.time() - start_time) * 1000)  # Total from request start to finish
            # Finalize audit data
            audit_data["durationms"] = int((time.time() - start_time) * 1000)
            audit_data["responsestatus"] = 200
            audit_data["responsedurationms"] = audit_data["durationms"]
            audit_data["response"] = json.dumps({
                "status": "success",
                "rowsreturned": len(rows),
                "sql_execution_time_ms": exec_duration,
                "total_response_time_ms": total_duration,
                "response_text": "".join(full_response_text)
            }, default=json_safe)
            remember_q = _remembered_question_for_turn(
                effective_message,
                prior_question,
                metric_intent=metric_intent,
                short_metric_lookup=short_metric_lookup,
                format_only=format_only_follow_up,
            )
            store_last_question(user_id, conversation_id, remember_q)
            store_sql_turn(
                user_id=user_id,
                conversation_id=conversation_id,
                user_message=message,
                assistant_message="".join(assistant_text_parts),
                sql=sql,
                schema=json_fields,
                rows=rows,
            )
            
            # Log once at end with success
            AuditService.log_audit_event(**audit_data)
            app_logger.info(f"Chat stream completed - {len(rows)} rows in {audit_data['durationms']}ms")
            
        except ValueError as ve:
            #audit_data["event_type"] = "VALIDATION_ERROR"
            audit_data["eventtype"] = event("event_generator", "execute_sql", "VALIDATION_ERROR")
            audit_data["errortype"] = "VALIDATION_ERROR"
            audit_data["errormessage"] = str(ve)[:2000]
            audit_data["responsestatus"] = 400
            audit_data["durationms"] = int((time.time() - start_time) * 1000)
            audit_data["responsedurationms"] = audit_data["durationms"]
            audit_data["response"] = json.dumps({
                "status": "error", 
                "type": "validation_error",
                "response_text": str(ve)
            })[:2000]
            #audit_data["response"] = "".join(full_response_text)[:8000] if full_response_text else str(ve)[:2000]

            AuditService.log_audit_event(**audit_data)
            
            err = {"error": True, "message": str(ve), "type": "validation_error"}
            yield f"event: validation_error\ndata: {json.dumps(err)}\n\n"
            
        except Exception as ex:
            #audit_data["event_type"] = "EXECUTION_ERROR"
            audit_data["eventtype"] = event("event_generator", "chat_stream", "EXECUTION_ERROR")
            audit_data["sqlstatus"] = "FAILED"
            audit_data["errortype"] = "EXECUTION_ERROR"
            audit_data["errormessage"] = str(ex)[:2000]
            audit_data["responsestatus"] = 500
            audit_data["durationms"] = int((time.time() - start_time) * 1000)
            audit_data["responsedurationms"] = audit_data["durationms"]
            audit_data["response"] = json.dumps({
                "status": "error", 
                "type": "execution_error",
                "response_text": str(ex)[:2000]
            })[:2000]
            #audit_data["response"] = "".join(full_response_text)[:8000] if full_response_text else str(ex)[:2000]

            AuditService.log_audit_event(**audit_data)
            app_logger.exception("CHAT STREAM ERROR")
            
            err = {"error": True, "message": str(ex), "type": "execution_error"}
            yield f"event: execution_error\ndata: {json.dumps(err)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def json_safe(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return str(obj)


import re

def inject_filters_safely(sql: str, conditions):

    if not conditions:
        return sql

    if isinstance(conditions, str):
        conditions = [conditions]

    if not isinstance(sql, str):
        raise ValueError(f"SQL must be string, got {type(sql)}")

    sql = sql.strip().rstrip(";")

    split_match = re.search(
        r"\b(GROUP BY|ORDER BY|LIMIT)\b",
        sql,
        re.IGNORECASE
    )

    base_sql = sql
    tail_sql = ""

    if split_match:
        base_sql = sql[:split_match.start()]
        tail_sql = sql[split_match.start():]

    if re.search(r"\bWHERE\b", base_sql, re.IGNORECASE):
        base_sql += "\nAND " + "\nAND ".join(conditions)
    else:
        base_sql += "\nWHERE " + "\nAND ".join(conditions)

    return (base_sql + "\n" + tail_sql).strip()


