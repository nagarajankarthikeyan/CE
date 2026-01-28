from sqlalchemy import create_engine, text
from app.config import settings


def get_connection_string():
    return settings.get_odbc_connection_string()


engine = create_engine(
    "mssql+pyodbc:///?odbc_connect="
    + get_connection_string().replace(";", "%3B"),
    fast_executemany=True
)


def run_query(sql: str, params: dict = None):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        return [dict(row._mapping) for row in result]
