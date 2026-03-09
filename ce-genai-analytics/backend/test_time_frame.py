#!/usr/bin/env python3
"""
Test script for time frame extraction functionality
"""

import sys
sys.path.insert(0, '/Users/aritra/dev/genAI/CE/ce-genai-analytics/backend')

from app.time_frame_extractor import (
    extract_time_frame_from_sql,
    extract_time_frame_from_question,
    build_time_frame_context,
    get_time_frame_condition
)

def test_sql_extraction():
    print("=" * 60)
    print("Testing SQL Time Frame Extraction")
    print("=" * 60)
    
    test_cases = [
        # BETWEEN pattern
        """
        SELECT SUM(spend) as total_spend
        FROM table
        WHERE DATE(date) BETWEEN '2024-01-01' AND '2024-01-31'
        """,
        
        # Last week pattern
        """
        SELECT platform, SUM(spend) as spend
        FROM table
        WHERE DATE_TRUNC(DATE(date), WEEK(MONDAY)) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY))
        GROUP BY platform
        """,
        
        # Quarter pattern
        """
        SELECT SUM(spend)
        FROM table
        WHERE EXTRACT(YEAR FROM DATE(date)) = 2024
        AND EXTRACT(QUARTER FROM DATE(date)) = 1
        """,
        
        # Month pattern
        """
        SELECT *
        FROM table
        WHERE DATE_TRUNC(DATE(date), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
        """
    ]
    
    for i, sql in enumerate(test_cases, 1):
        print(f"\nTest Case {i}:")
        result = extract_time_frame_from_sql(sql)
        if result:
            print(f"  Description: {result['description']}")
            print(f"  Condition: {result['condition']}")
        else:
            print("  No time frame detected")

def test_question_extraction():
    print("\n" + "=" * 60)
    print("Testing Question Time Frame Extraction")
    print("=" * 60)
    
    test_questions = [
        "What was the total spend last week?",
        "Show me performance for Q1 2024",
        "What is the total spend?",  # No time frame
        "How much did we spend this month?",
        "What was the CTR yesterday?",
        "Show me spend for 2024"
    ]
    
    for question in test_questions:
        print(f"\nQuestion: {question}")
        result = extract_time_frame_from_question(question)
        if result:
            print(f"  Description: {result['description']}")
            print(f"  Period Type: {result['period_type']}")
        else:
            print("  No time frame detected")

def test_context_building():
    print("\n" + "=" * 60)
    print("Testing Context Building")
    print("=" * 60)
    
    last_sql = """
    SELECT SUM(spend) as total_spend
    FROM table
    WHERE DATE(date) BETWEEN '2024-01-01' AND '2024-01-31'
    """
    
    last_question = "What was the total spend last week?"
    
    print("\nScenario 1: SQL with time frame")
    context = build_time_frame_context(last_sql, None)
    print(f"Context: {context}")
    
    print("\nScenario 2: Question with time frame (no SQL)")
    context = build_time_frame_context(None, last_question)
    print(f"Context: {context}")
    
    print("\nScenario 3: Both SQL and question")
    context = build_time_frame_context(last_sql, last_question)
    print(f"Context: {context}")
    
    print("\nScenario 4: Get reusable condition")
    condition = get_time_frame_condition(last_sql)
    print(f"Condition: {condition}")

def test_follow_up_scenario():
    print("\n" + "=" * 60)
    print("Testing Follow-up Question Scenario")
    print("=" * 60)
    
    # Simulate conversation
    print("\nUser: What was the total spend last week?")
    first_question = "What was the total spend last week?"
    
    # Simulated SQL generated
    first_sql = """
    SELECT SUM(spend) as total_spend
    FROM table
    WHERE DATE_TRUNC(DATE(date), WEEK(MONDAY)) = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK), WEEK(MONDAY))
    """
    
    print(f"Generated SQL extracts time frame...")
    tf = extract_time_frame_from_sql(first_sql)
    if tf:
        print(f"  Detected: {tf['description']}")
    
    # Follow-up question
    print("\nUser: What is the total spend?")
    follow_up = "What is the total spend?"
    
    has_time_frame = extract_time_frame_from_question(follow_up)
    print(f"Follow-up has explicit time frame: {has_time_frame is not None}")
    
    if not has_time_frame:
        context = build_time_frame_context(first_sql, first_question)
        print(f"Adding context from previous query: {context}")
        print(f"\nEnhanced prompt would be:")
        print(f"  '{follow_up}\\n\\nContext: {context}'")

if __name__ == "__main__":
    test_sql_extraction()
    test_question_extraction()
    test_context_building()
    test_follow_up_scenario()
    
    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)
