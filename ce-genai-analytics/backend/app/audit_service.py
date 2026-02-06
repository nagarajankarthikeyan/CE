import logging
import json
from datetime import datetime
from typing import Optional
from sqlalchemy import text
from app.db import engine
from app.logging_config import app_logger


class AuditService:
    """Service for managing audit logs"""
    
    TABLE_NAME = "AuditLogs"
    
    @staticmethod
    def log_audit_event(
        conversation_id: str,
        user_id: Optional[int] = None,
        event_type: Optional[str] = None,
        user_message: Optional[str] = None,
        generated_sql: Optional[str] = None,
        sql_status: Optional[str] = None,
        rows_returned: Optional[int] = None,
        duration_ms: Optional[int] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        response: Optional[str] = None,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
        response_status: Optional[int] = None,
        response_duration_ms: Optional[int] = None
    ) -> None:
        """
        Log a single audit event with all available data.
        """
        try:
            # Use bracket notation for all column names to handle any casing
            sql = text(f"""
            INSERT INTO {AuditService.TABLE_NAME} 
            ([ConversationID], [EventType], [UserID], [UserMessage], [GeneratedSQL], [SQLStatus], [RowsReturned], [DurationMS], 
             [ErrorType], [ErrorMessage], [Endpoint], [HTTPMethod], [ResponseStatus], [ResponseDurationMS], [Response], [CreatedAt])
            VALUES (:conv_id, :event_type, :user_id, :user_msg, :sql_gen, :sql_status, :rows, :duration,
                    :err_type, :err_msg, :endpoint, :method, :resp_status, :resp_duration, :response, GETUTCDATE())
            """)
            
            with engine.begin() as conn:
                conn.execute(sql, {
                    "conv_id": conversation_id,
                    "event_type": event_type,
                    "user_id": user_id,
                    "user_msg": user_message,
                    "sql_gen": generated_sql,
                    "sql_status": sql_status,
                    "rows": rows_returned,
                    "duration": duration_ms,
                    "err_type": error_type,
                    "err_msg": error_message,
                    "endpoint": endpoint,
                    "method": method,
                    "resp_status": response_status,
                    "resp_duration": response_duration_ms,
                    "response": response
                })
            
            app_logger.info(
                f"AUDIT | conv={conversation_id} | event={event_type} | status={sql_status} | rows={rows_returned} | duration={duration_ms}ms"
            )
        except Exception as e:
            app_logger.error(f"Error logging audit event: {e}", exc_info=True)
    
    @staticmethod
    def get_conversation_logs(conversation_id: str) -> list:
        """Retrieve all logs for a conversation"""
        try:
            sql = text(f"""
            SELECT * FROM {AuditService.TABLE_NAME}
            WHERE [ConversationID] = :conv_id
            ORDER BY [CreatedAt] ASC
            """)
            
            with engine.connect() as conn:
                result = conn.execute(sql, {"conv_id": conversation_id})
                return [dict(row._mapping) for row in result]
        except Exception as e:
            app_logger.error(f"Error retrieving conversation logs: {e}", exc_info=True)
            return []
    
    @staticmethod
    def get_logs_by_event_type(event_type: str, limit: int = 100) -> list:
        """Retrieve logs by event type"""
        try:
            sql = text(f"""
            SELECT TOP {limit} * FROM {AuditService.TABLE_NAME}
            WHERE [EventType] = :event_type
            ORDER BY [CreatedAt] DESC
            """)
            
            with engine.connect() as conn:
                result = conn.execute(sql, {"event_type": event_type})
                return [dict(row._mapping) for row in result]
        except Exception as e:
            app_logger.error(f"Error retrieving logs by event type: {e}", exc_info=True)
            return []