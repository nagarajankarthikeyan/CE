import os

# ===============================
# BIGQUERY CONFIGURATION
# ===============================

# GCP Project ID (NOT dataset)
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "bounteous-bi")

# Dataset name (NOT project.dataset)
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "constellation_media_AI_ANALYST")

# View or Table name
BIGQUERY_VIEW = os.getenv("BIGQUERY_VIEW", "complete_constellation")

# Region (must match dataset region exactly)
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")

# ===============================
# SERVICE ACCOUNT (Optional)
# ===============================
# If you want to explicitly set JSON credentials file path
# Otherwise BigQuery will use GOOGLE_APPLICATION_CREDENTIALS env variable

GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    ""
)
