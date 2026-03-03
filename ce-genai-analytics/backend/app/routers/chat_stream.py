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
    ]
    return any(re.search(p, msg) for p in patterns)


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

            effective_message = message
            narrative_message = message
            if prior_question and is_format_only_follow_up_message(message):
                effective_message = prior_question
                narrative_message = (
                    f"{prior_question}\n\n"
                    f"Follow-up formatting instruction: {message}\n"
                    "Keep the same analytical scope as the prior question and only change response format/style."
                )

            platform, cleaned_message = extract_platform(effective_message)

            generated_sql = generate_sql(cleaned_message, json_fields)
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
                    is_follow_up_message(message)
                    or is_format_only_follow_up_message(message)
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
            async for token in stream_narrative(narrative_message, rows, render_spec):
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
            store_last_question(user_id, conversation_id, effective_message)
            
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


