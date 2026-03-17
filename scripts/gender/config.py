"""Configuration for gender analysis pipeline."""
import os

# Gender API settings (set in .env or environment)
GENDER_API_KEY = os.getenv("GENDER_API_KEY", "")

# Thresholds
GENDER_CONFIDENCE_THRESHOLD = 0.60  # Minimum probability to assign gender
MIN_FORENAME_LENGTH = 3             # Skip initials-only names

# Scope
START_YEAR = 2005
END_YEAR = 2026

# Paths
GENDER_CACHE_DB = "data/gender_cache.db"
IMPACT_DB = "data/impact.db"
PUBMED_BULK_DB = "data/pubmed_bulk.db"
