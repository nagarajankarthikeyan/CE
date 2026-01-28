from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
import asyncio
import json

from app.schema_introspector import get_json_schema
from app.dynamic_sql_generator import generate_sql
from app.sql_normalizer import normalize_sql
from app.sql_validator import validate_sql
from app.executor import execute_sql
from app.render_builder import build_render_spec   # YOU MUST HAVE THIS
from app.narrator import stream_narrative
import json
from datetime import date, datetime


router = APIRouter()


@router.get("/chat/stream")
async def chat_stream(message: str, conversation_id: str | None = None):
    try:
        # =========================
        # 1. SCHEMA + SQL
        # =========================
        json_fields = get_json_schema()

        raw_sql = generate_sql(message, json_fields)
        sql = normalize_sql(raw_sql)
        validate_sql(sql)

        # =========================
        # 2. EXECUTE SQL
        # =========================
        rows = execute_sql(sql, {})

        # =========================
        # 3. BUILD RENDER SPEC
        # =========================
        render_spec = build_render_spec(message, rows)

        async def event_generator():

            # ---- IMMEDIATE STRUCTURED RENDER ----
            yield f"event: render\ndata: {json.dumps(render_spec, default=json_safe)}\n\n"
            await asyncio.sleep(0.01)   # 🔥 force flush

            # ---- TRUE TOKEN STREAMING ----
            async for token in stream_narrative(message, rows, render_spec):
                yield f"data: {token}\n\n"
                await asyncio.sleep(0)  # 🔥 critical for flushing

            # ---- DONE ----
            yield "event: done\ndata: [DONE]\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    except Exception as ex:
        print("CHAT STREAM ERROR:", ex)
        raise HTTPException(status_code=400, detail=str(ex))

def json_safe(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return str(obj)
