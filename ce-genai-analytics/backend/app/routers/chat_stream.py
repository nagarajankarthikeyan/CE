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

router = APIRouter()

GENERIC_BLOCK_MSG = "I can't perform that action that change system data. I can help with data analysis and retrieval."

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

@router.get("/chat/stream")
async def chat_stream(request: Request, message: str, conversation_id: str | None = None, user: str = Depends(get_current_user)):
    conversation_id = conversation_id or request.query_params.get("conversation_id") or request.headers.get("X-Conversation-ID") or f"conv-unknown"
    '''user_id = None
    try:
        uid = request.headers.get("X-User-ID")
        if uid:
            user_id = int(uid)
    except Exception:
        user_id = None'''
    user_id = user.get("user_id")
    
    endpoint = request.url.path
    method = request.method
    full_response_text = []
    async def event_generator():
        start_time = time.time()
        
        # Audit state - collect data without logging yet
        audit_data = {
            "conversation_id": conversation_id,
            "user_id": user_id,
            "endpoint": endpoint,
            "method": method,
            "user_message": message[:2000],
            "generated_sql": None,
            "rows_returned": 0,
            "duration_ms": 0,
            "sql_status": None,
            "error_type": None,
            "error_message": None,
            "event_type": None,
            "response": None,
            "response_status": 200,  # default success
            "response_duration_ms": 0
        }
        
        try:
            # Check forbidden intent
            intent_error = check_forbidden_intent(message)
            if intent_error:
                #add function name to log
                #audit_data["event_type"] = "VALIDATION_ERROR"
                audit_data["event_type"] = event("event_generator", "check_forbidden_intent", "FORBIDDEN_OPERATION")
                audit_data["sql_status"] = "BLOCKED"
                audit_data["error_type"] = "FORBIDDEN_OPERATION"
                audit_data["error_message"] = intent_error
                audit_data["response_status"] = 403 
                audit_data["duration_ms"] = int((time.time() - start_time) * 1000)
                audit_data["response_duration_ms"] = audit_data["duration_ms"]
                audit_data["response"] = json.dumps({"status": "error", "type": "FORBIDDEN_OPERATION", "response_text": intent_error})[:2000]
                #audit_data["response"] = "".join(full_response_text)[:8000] if full_response_text else str(intent_error)[:2000]
    
                # Log once with error
                AuditService.log_audit_event(**audit_data)
                
                err = {"error": True, "message": intent_error, "type": "validation_error"}
                yield f"event: validation_error\ndata: {json.dumps(err)}\n\n"
                return

            # Generate SQL
            json_fields = get_json_schema()
            raw_sql = generate_sql(message, json_fields)
            sql = normalize_sql(raw_sql)
            audit_data["generated_sql"] = sql[:4000]

            # Validate SQL
            try:
                validate_sql(sql)
            except ValueError as ve:
                #audit_data["event_type"] = "VALIDATION_ERROR"
                audit_data["event_type"] = event("event_generator", "validate_sql", "VALIDATION_ERROR")
                audit_data["sql_status"] = "INVALID"
                audit_data["error_type"] = "SQL_VALIDATION_FAILED"
                audit_data["error_message"] = str(ve)[:2000]
                audit_data["response_status"] = 400
                audit_data["duration_ms"] = int((time.time() - start_time) * 1000)
                audit_data["response_duration_ms"] = audit_data["duration_ms"]
                audit_data["response"] = json.dumps({"status": "error", "type": "validation_failed", "response_text": str(ve)[:500]})[:2000]
                #audit_data["response"] = "".join(full_response_text)[:8000] if full_response_text else str(ve)[:2000]

                # Log once with error
                AuditService.log_audit_event(**audit_data)
                
                err = {"error": True, "message": str(ve), "type": "validation_error"}
                yield f"event: validation_error\ndata: {json.dumps(err)}\n\n"
                return

            # Execute SQL
            exec_start = time.time()
            rows = execute_sql(sql, {})
            exec_duration = int((time.time() - exec_start) * 1000)
            
            audit_data["rows_returned"] = len(rows)
            audit_data["event_type"] = event("event_generator", "execute_sql", "SQL_EXECUTED")
            audit_data["sql_status"] = "SUCCESS"
            #audit_data["event_type"] = "SQL_EXECUTED"
            audit_data["duration_ms"] = exec_duration 
            
            # Build render spec
            render_spec = build_render_spec(message, rows)
            full_response_text.append(json.dumps(rows, default=json_safe))
            yield f"event: render\ndata: {json.dumps(render_spec, default=json_safe)}\n\n"
            await asyncio.sleep(0.01)

            # Stream narrative tokens
            async for token in stream_narrative(message, rows, render_spec):
                full_response_text.append(token) 
                yield f"data: {token}\n\n"
                await asyncio.sleep(0)

            yield "event: done\ndata: [DONE]\n\n"
            
            total_duration = int((time.time() - start_time) * 1000)  # Total from request start to finish
            # Finalize audit data
            audit_data["duration_ms"] = int((time.time() - start_time) * 1000)
            audit_data["response_status"] = 200
            audit_data["response_duration_ms"] = audit_data["duration_ms"]
            audit_data["response"] = json.dumps({
                "status": "success",
                "rows_returned": len(rows),
                "sql_execution_time_ms": exec_duration,
                "total_response_time_ms": total_duration,
                "response_text": "".join(full_response_text)
            })
            
            # Log once at end with success
            AuditService.log_audit_event(**audit_data)
            app_logger.info(f"Chat stream completed - {len(rows)} rows in {audit_data['duration_ms']}ms")
            
        except ValueError as ve:
            #audit_data["event_type"] = "VALIDATION_ERROR"
            audit_data["event_type"] = event("event_generator", "execute_sql", "VALIDATION_ERROR")
            audit_data["error_type"] = "VALIDATION_ERROR"
            audit_data["error_message"] = str(ve)[:2000]
            audit_data["response_status"] = 400
            audit_data["duration_ms"] = int((time.time() - start_time) * 1000)
            audit_data["response_duration_ms"] = audit_data["duration_ms"]
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
            audit_data["event_type"] = event("event_generator", "chat_stream", "EXECUTION_ERROR")
            audit_data["sql_status"] = "FAILED"
            audit_data["error_type"] = "EXECUTION_ERROR"
            audit_data["error_message"] = str(ex)[:2000]
            audit_data["response_status"] = 500
            audit_data["duration_ms"] = int((time.time() - start_time) * 1000)
            audit_data["response_duration_ms"] = audit_data["duration_ms"]
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