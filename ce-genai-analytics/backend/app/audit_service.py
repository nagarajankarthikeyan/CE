from google.cloud import bigquery
from datetime import datetime, date
from app.config import (
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET,
    BIGQUERY_LOCATION,
    GOOGLE_APPLICATION_CREDENTIALS
)


client = bigquery.Client(
    project=BIGQUERY_PROJECT,
    location=BIGQUERY_LOCATION
)

AUDIT_TABLE = "constellation_media_AI_ANALYST.AuditLogs"


# Create client once
client = bigquery.Client.from_service_account_json(
    GOOGLE_APPLICATION_CREDENTIALS
)


def make_json_safe(obj):
    """
    Recursively convert non-JSON-safe objects.
    Handles datetime inside nested dicts/lists.
    """
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    else:
        return obj


class AuditService:

    @staticmethod
    def log_audit_event(**data):

        # Always set created timestamp
        data["CreatedAt"] = datetime.utcnow()

        # VERY IMPORTANT — sanitize before insert
        safe_data = make_json_safe(data)

        errors = client.insert_rows_json(AUDIT_TABLE, [safe_data])

        if errors:
            print("Audit insert failed:", errors)

