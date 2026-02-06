from google.cloud import bigquery
from app.config import (
    BIGQUERY_PROJECT,
    BIGQUERY_DATASET,
    BIGQUERY_VIEW,
    BIGQUERY_LOCATION,
    GOOGLE_APPLICATION_CREDENTIALS
)

import os


# =========================
# BigQuery Client
# =========================

def get_bigquery_client():
    if GOOGLE_APPLICATION_CREDENTIALS:
        return bigquery.Client.from_service_account_json(
            GOOGLE_APPLICATION_CREDENTIALS,
            project=BIGQUERY_PROJECT,
            location=BIGQUERY_LOCATION
        )

    return bigquery.Client(
        project=BIGQUERY_PROJECT,
        location=BIGQUERY_LOCATION
    )


# =========================
# Execute SQL
# =========================

def execute_sql(sql: str, params: dict | None = None):
    """
    Executes SQL in BigQuery and ALWAYS returns:
        List[Dict]
    Never Row objects.
    Never strings.
    """

    client = get_bigquery_client()

    job = client.query(sql)
    result = job.result()

    rows = []

    for row in result:
        # THIS LINE IS CRITICAL
        rows.append(dict(row))

    return rows
