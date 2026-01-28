# ======================================
# METRICS (Aggregations + Aliases)
# ======================================

METRICS = {
    # -------------------------
    # Record Counts
    # -------------------------
    "record_count": "COUNT(1)",
    "count": "COUNT(1)",
    "total_records": "COUNT(1)",
    "rows": "COUNT(1)",

    # -------------------------
    # Spend / Cost
    # -------------------------
    "total_spend": "SUM(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "spend": "SUM(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "cost": "SUM(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "total_cost": "SUM(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",

    "avg_spend": "AVG(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "average_spend": "AVG(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "mean_spend": "AVG(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",

    "max_spend": "MAX(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "maximum_spend": "MAX(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "highest_spend": "MAX(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",

    "min_spend": "MIN(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "minimum_spend": "MIN(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "lowest_spend": "MIN(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",

    # -------------------------
    # Impressions
    # -------------------------
    "total_impressions": "SUM(CAST(JSON_VALUE(RawJson, '$.impressions') AS FLOAT))",
    "impressions": "SUM(CAST(JSON_VALUE(RawJson, '$.impressions') AS FLOAT))",

    # -------------------------
    # Clicks
    # -------------------------
    "total_clicks": "SUM(CAST(JSON_VALUE(RawJson, '$.link_clicks') AS FLOAT))",
    "clicks": "SUM(CAST(JSON_VALUE(RawJson, '$.link_clicks') AS FLOAT))",

    # -------------------------
    # Enrollments / Conversions
    # -------------------------
    "total_enrollments": "SUM(CAST(JSON_VALUE(RawJson, '$.actions_enrollments_v2_') AS FLOAT))",
    "enrollments": "SUM(CAST(JSON_VALUE(RawJson, '$.actions_enrollments_v2_') AS FLOAT))",
    "conversions": "SUM(CAST(JSON_VALUE(RawJson, '$.actions_enrollments_v2_') AS FLOAT))",

    # -------------------------
    # Derived KPIs (Proxy)
    # -------------------------
    # CTR = Clicks / Impressions
    "ctr": """
        CASE 
            WHEN SUM(CAST(JSON_VALUE(RawJson, '$.impressions') AS FLOAT)) = 0 THEN 0
            ELSE 
              SUM(CAST(JSON_VALUE(RawJson, '$.link_clicks') AS FLOAT)) 
              / SUM(CAST(JSON_VALUE(RawJson, '$.impressions') AS FLOAT))
        END
    """,

    # CPA = Spend / Enrollments
    "cpa": """
        CASE 
            WHEN SUM(CAST(JSON_VALUE(RawJson, '$.actions_enrollments_v2_') AS FLOAT)) = 0 THEN NULL
            ELSE 
              SUM(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))
              / SUM(CAST(JSON_VALUE(RawJson, '$.actions_enrollments_v2_') AS FLOAT))
        END
    """,

    # ROI proxy (if revenue not available, GPT can narrate)
    "roi": None,   # handled at narrative layer (proxy explanation)
}

# ======================================
# DIMENSIONS (Group By + Aliases)
# ======================================

DIMENSIONS = {
    # -------------------------
    # Time
    # -------------------------
    "date": "JSON_VALUE(RawJson, '$.date')",
    "day": "JSON_VALUE(RawJson, '$.date')",

    # -------------------------
    # Channel / Source
    # -------------------------
    "source": "JSON_VALUE(RawJson, '$.source')",
    "channel": "JSON_VALUE(RawJson, '$.source')",
    "platform": "JSON_VALUE(RawJson, '$.source')",

    # -------------------------
    # Campaign
    # -------------------------
    "campaign": "JSON_VALUE(RawJson, '$.campaign')",
    "campaign_name": "JSON_VALUE(RawJson, '$.campaign')",

    # -------------------------
    # Account
    # -------------------------
    "account_id": "JSON_VALUE(RawJson, '$.account_id')",
    "account_name": "JSON_VALUE(RawJson, '$.account_name')",
    "account": "JSON_VALUE(RawJson, '$.account_name')",

    # -------------------------
    # Ad / Adset (optional)
    # -------------------------
    "ad_id": "JSON_VALUE(RawJson, '$.ad_id')",
    "ad_name": "JSON_VALUE(RawJson, '$.ad_name')",
    "adset_id": "JSON_VALUE(RawJson, '$.adset_id')",
    "adset_name": "JSON_VALUE(RawJson, '$.adset_name')",

    # -------------------------
    # Objective
    # -------------------------
    "objective": "JSON_VALUE(RawJson, '$.objective')",
}

# ======================================
# FILTER ALIASES (for business language)
# ======================================

FILTER_ALIASES = {
    "home services": ("campaign", "Home Services"),
    "winback": ("campaign", "Winback"),
    "facebook": ("source", "facebook"),
    "google": ("source", "google"),
}

# =========================
# Business Metric Aliases
# =========================

METRICS.update({
    # Business language
    "total cost": "SUM(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",
    "cost": "SUM(CAST(JSON_VALUE(RawJson, '$.spend') AS FLOAT))",

    # Performance is NOT a single metric
    # It is handled as a bundle in logic layer
    "performance": None,
})
