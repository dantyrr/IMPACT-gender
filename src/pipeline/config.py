"""
IMPACT Configuration
API keys, rate limits, journal definitions, and paths.
"""

import os

# Optional: load from .env file if python-dotenv is installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- API Keys ---
PUBMED_API_KEY = os.getenv("PUBMED_API_KEY", "")
PUBMED_EMAIL = os.getenv("PUBMED_EMAIL", "dantyrr@gmail.com")

# --- Rate Limits (requests per second) ---
PUBMED_RATE_LIMIT = 10 if PUBMED_API_KEY else 3
ICITE_RATE_LIMIT = 5  # Conservative; no official limit documented

# --- Target Journals ---
# ISSN → metadata for the 5 starter journals
JOURNALS = {
    "1474-9718": {
        "name": "Aging Cell",
        "abbreviation": "Aging Cell",
        "slug": "aging-cell",
    },
    "0021-9738": {
        "name": "Journal of Clinical Investigation",
        "abbreviation": "J Clin Invest",
        "slug": "jci",
    },
    "2662-8465": {
        "name": "Nature Aging",
        "abbreviation": "Nat Aging",
        "slug": "nature-aging",
    },
    "2050-084X": {
        "name": "eLife",
        "abbreviation": "eLife",
        "slug": "elife",
    },
    "2047-9980": {
        "name": "Journal of the American Heart Association",
        "abbreviation": "J Am Heart Assoc",
        "slug": "jaha",
    },
}

# --- Paths ---
# Resolve relative to repo root
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(_REPO_ROOT, "data", "impact.db")
WEBSITE_DATA_DIR = os.path.join(_REPO_ROOT, "website", "data")
SNAPSHOTS_DIR = os.path.join(WEBSITE_DATA_DIR, "journals")

# --- Computation Parameters ---
ROLLING_WINDOW_MONTHS = 24

# --- PubMed E-utilities base URL ---
PUBMED_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# --- iCite API base URL ---
ICITE_BASE_URL = "https://icite.od.nih.gov/api/pubs"
