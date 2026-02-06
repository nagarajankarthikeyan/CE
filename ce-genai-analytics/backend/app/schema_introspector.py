from app.executor import get_bigquery_client
from app.config import BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_VIEW, BIGQUERY_LOCATION

def get_json_schema():
    client = get_bigquery_client()

    query = f"""
    SELECT column_name, data_type
    FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{BIGQUERY_VIEW}'
    """

    job = client.query(query, location=BIGQUERY_LOCATION)
    rows = job.result()

    return [
        {
            "name": row.column_name,
            "type": row.data_type
        }
        for row in rows
    ]
