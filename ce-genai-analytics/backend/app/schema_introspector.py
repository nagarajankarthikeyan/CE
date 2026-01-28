from app.db import run_query
import json
from collections import Counter


def get_json_schema(sample_size: int = 2000):
    sql = f"""
    SELECT TOP {sample_size}
           RawJson
    FROM DataLakeRaw
    """

    rows = run_query(sql)

    field_counts = Counter()

    for r in rows:
        try:
            obj = json.loads(r["RawJson"])
            for k in obj.keys():
                field_counts[k] += 1
        except Exception:
            continue

    # Return fields sorted by frequency (most common first)
    fields = [k for k, _ in field_counts.most_common()]
    return fields
