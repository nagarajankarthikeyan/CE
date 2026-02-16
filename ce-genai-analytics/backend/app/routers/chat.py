from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.schema_introspector import get_json_schema
from app.dynamic_sql_generator import generate_sql
from app.sql_normalizer import normalize_sql
from app.sql_validator import validate_sql
from app.executor import execute_sql
from app.narrator import narrate
from app.value_semantic_resolver import normalize_sql_value_semantics

# =========================
# Router MUST be defined BEFORE decorators
# =========================
router = APIRouter()


class ChatRequest(BaseModel):
    message: str


@router.post("/chat")
def chat(req: ChatRequest):
    try:
        # 1. Discover schema dynamically
        json_fields = get_json_schema()

        # 2. Generate SQL dynamically
        raw_sql = generate_sql(req.message, json_fields)

        # 3. Normalize markdown fences
        sql = normalize_sql(raw_sql)
        sql = normalize_sql_value_semantics(sql, json_fields)

        # 4. Validate SQL
        validate_sql(sql)

        # 5. Execute
        rows = execute_sql(sql, {})

        # 6. Generic GPT-driven narrative
        narrative = narrate(req.message, rows)

        return {
            "sql": sql,
            "data": rows,
            "narrative": narrative
        }

    except Exception as ex:
        raise HTTPException(status_code=400, detail=str(ex))
