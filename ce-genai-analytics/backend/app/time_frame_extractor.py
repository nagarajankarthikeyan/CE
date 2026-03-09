import re
from datetime import date, datetime, timedelta


def extract_time_frame_from_sql(sql: str) -> dict | None:
    """
    Extract time frame information from SQL WHERE clauses.
    Returns dict with 'description' and 'condition' keys, or None.
    """
    if not sql or not isinstance(sql, str):
        return None

    sql_lower = sql.lower()
    
    # Pattern 1: BETWEEN dates
    between_match = re.search(
        r"date\s*\([^)]+\)\s*between\s+['\"]([^'\"]+)['\"]\s+and\s+['\"]([^'\"]+)['\"]",
        sql_lower,
        re.IGNORECASE
    )
    if between_match:
        start_date = between_match.group(1)
        end_date = between_match.group(2)
        return {
            "description": f"between {start_date} and {end_date}",
            "condition": f"DATE(date) BETWEEN '{start_date}' AND '{end_date}'"
        }
    
    # Pattern 2: >= and <= date range
    gte_match = re.search(r"date\s*\([^)]+\)\s*>=\s*['\"]([^'\"]+)['\"]", sql_lower)
    lte_match = re.search(r"date\s*\([^)]+\)\s*<=\s*['\"]([^'\"]+)['\"]", sql_lower)
    
    if gte_match and lte_match:
        start_date = gte_match.group(1)
        end_date = lte_match.group(1)
        return {
            "description": f"from {start_date} to {end_date}",
            "condition": f"DATE(date) >= '{start_date}' AND DATE(date) <= '{end_date}'"
        }
    
    # Pattern 3: DATE_TRUNC for last week
    if "date_trunc" in sql_lower and "week" in sql_lower:
        # Try to extract the actual dates if present
        week_match = re.search(
            r"date_trunc\([^,]+,\s*week\(monday\)\)\s*=\s*date_trunc\(date\(['\"]([^'\"]+)['\"]\)",
            sql_lower
        )
        if week_match:
            ref_date = week_match.group(1)
            return {
                "description": f"for the week of {ref_date}",
                "condition": f"DATE_TRUNC(DATE(date), WEEK(MONDAY)) = DATE_TRUNC(DATE('{ref_date}'), WEEK(MONDAY))"
            }
        return {
            "description": "for last week",
            "condition": "DATE_TRUNC(DATE(date), WEEK(MONDAY)) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY))"
        }
    
    # Pattern 4: DATE_TRUNC for month
    if "date_trunc" in sql_lower and "month" in sql_lower:
        month_match = re.search(
            r"date_trunc\([^,]+,\s*month\)\s*=\s*date_trunc\(current_date\(\),\s*month\)",
            sql_lower
        )
        if month_match:
            return {
                "description": "for this month",
                "condition": "DATE_TRUNC(DATE(date), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)"
            }
    
    # Pattern 5: EXTRACT YEAR/QUARTER
    year_match = re.search(r"extract\(year\s+from\s+[^)]+\)\s*=\s*(\d{4})", sql_lower)
    quarter_match = re.search(r"extract\(quarter\s+from\s+[^)]+\)\s*=\s*(\d)", sql_lower)
    
    if year_match and quarter_match:
        year = year_match.group(1)
        quarter = quarter_match.group(1)
        return {
            "description": f"for Q{quarter} {year}",
            "condition": f"EXTRACT(YEAR FROM DATE(date)) = {year} AND EXTRACT(QUARTER FROM DATE(date)) = {quarter}"
        }
    
    if year_match:
        year = year_match.group(1)
        return {
            "description": f"for year {year}",
            "condition": f"EXTRACT(YEAR FROM DATE(date)) = {year}"
        }
    
    # Pattern 6: Single date comparison
    single_date_match = re.search(r"date\s*\([^)]+\)\s*=\s*['\"]([^'\"]+)['\"]", sql_lower)
    if single_date_match:
        target_date = single_date_match.group(1)
        return {
            "description": f"for {target_date}",
            "condition": f"DATE(date) = '{target_date}'"
        }
    
    # Pattern 7: Last N days
    last_days_match = re.search(
        r"date\s*\([^)]+\)\s*>=\s*date_sub\(current_date\(\),\s*interval\s+(\d+)\s+day\)",
        sql_lower
    )
    if last_days_match:
        days = last_days_match.group(1)
        return {
            "description": f"for the last {days} days",
            "condition": f"DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"
        }
    
    return None


def extract_time_frame_from_question(question: str) -> dict | None:
    """
    Extract time frame from natural language question.
    Returns dict with 'description' and 'period_type' keys, or None.
    """
    if not question or not isinstance(question, str):
        return None
    
    q_lower = question.lower().strip()
    
    # Specific date patterns
    date_pattern = r"\b(\d{4}-\d{2}-\d{2})\b"
    dates = re.findall(date_pattern, q_lower)
    if len(dates) >= 2:
        return {
            "description": f"between {dates[0]} and {dates[1]}",
            "period_type": "date_range"
        }
    
    # Quarter patterns
    quarter_match = re.search(r"\bq([1-4])\s+(\d{4})\b", q_lower)
    if quarter_match:
        quarter = quarter_match.group(1)
        year = quarter_match.group(2)
        return {
            "description": f"Q{quarter} {year}",
            "period_type": "quarter"
        }
    
    # Relative time patterns
    relative_patterns = {
        r"\blast\s+week\b": {"description": "last week", "period_type": "last_week"},
        r"\bthis\s+week\b": {"description": "this week", "period_type": "this_week"},
        r"\blast\s+month\b": {"description": "last month", "period_type": "last_month"},
        r"\bthis\s+month\b": {"description": "this month", "period_type": "this_month"},
        r"\bmonth[- ]to[- ]date\b": {"description": "month-to-date", "period_type": "mtd"},
        r"\bmtd\b": {"description": "month-to-date", "period_type": "mtd"},
        r"\byear[- ]to[- ]date\b": {"description": "year-to-date", "period_type": "ytd"},
        r"\bytd\b": {"description": "year-to-date", "period_type": "ytd"},
        r"\byesterday\b": {"description": "yesterday", "period_type": "yesterday"},
        r"\btoday\b": {"description": "today", "period_type": "today"},
    }
    
    for pattern, result in relative_patterns.items():
        if re.search(pattern, q_lower):
            return result
    
    # Year pattern
    year_match = re.search(r"\b(20\d{2})\b", q_lower)
    if year_match:
        year = year_match.group(1)
        return {
            "description": f"for {year}",
            "period_type": "year"
        }
    
    return None


def build_time_frame_context(last_sql: str | None, last_question: str | None) -> str:
    """
    Build a context string describing the time frame from previous query.
    Returns empty string if no time frame detected.
    """
    time_frame = None
    
    # Try SQL first (most reliable)
    if last_sql:
        time_frame = extract_time_frame_from_sql(last_sql)
    
    # Fallback to question
    if not time_frame and last_question:
        tf_from_q = extract_time_frame_from_question(last_question)
        if tf_from_q:
            time_frame = {"description": tf_from_q["description"]}
    
    if time_frame and time_frame.get("description"):
        return f"Previous query time frame: {time_frame['description']}"
    
    return ""


def get_time_frame_condition(last_sql: str | None) -> str | None:
    """
    Extract the SQL time frame condition to reuse in follow-up queries.
    Returns the WHERE condition string or None.
    """
    if not last_sql:
        return None
    
    time_frame = extract_time_frame_from_sql(last_sql)
    if time_frame and time_frame.get("condition"):
        return time_frame["condition"]
    
    return None
