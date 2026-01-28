import re

def normalize_sql(sql: str) -> str:
    """
    Removes markdown code fences and trims whitespace.
    """

    s = sql.strip()

    # Remove ```sql or ``` fences
    if s.startswith("```"):
        # Remove starting ```sql or ```
        s = re.sub(r"^```[a-zA-Z]*", "", s).strip()
        # Remove ending ```
        s = re.sub(r"```$", "", s).strip()

    return s
