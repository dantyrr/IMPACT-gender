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

# --- Cloudflare R2 ---
R2_ACCOUNT_ID      = os.getenv("R2_ACCOUNT_ID", "")
R2_BUCKET_NAME     = os.getenv("R2_BUCKET_NAME", "impact-data")
R2_ACCESS_KEY_ID   = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_PUBLIC_URL      = os.getenv("R2_PUBLIC_URL", "")

# --- Rate Limits (requests per second) ---
PUBMED_RATE_LIMIT = 10 if PUBMED_API_KEY else 3
ICITE_RATE_LIMIT = 5  # Conservative; no official limit documented

# --- Target Journals ---
# ISSN → metadata
JOURNALS = {
    # Original 5
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
    # High-impact general/clinical journals
    "0028-4793": {
        "name": "New England Journal of Medicine",
        "abbreviation": "NEJM",
        "slug": "nejm",
    },
    "0140-6736": {
        "name": "The Lancet",
        "abbreviation": "Lancet",
        "slug": "lancet",
    },
    "0098-7484": {
        "name": "JAMA",
        "abbreviation": "JAMA",
        "slug": "jama",
    },
    "0003-4819": {
        "name": "Annals of Internal Medicine",
        "abbreviation": "Ann Intern Med",
        "slug": "annals-internal-medicine",
    },
    # High-impact basic/translational journals
    "1078-8956": {
        "name": "Nature Medicine",
        "abbreviation": "Nat Med",
        "slug": "nature-medicine",
    },
    "0092-8674": {
        "name": "Cell",
        "abbreviation": "Cell",
        "slug": "cell",
    },
    "1946-6234": {
        "name": "Science Translational Medicine",
        "abbreviation": "Sci Transl Med",
        "slug": "science-translational-medicine",
    },
    # Cardiovascular
    "0009-7322": {
        "name": "Circulation",
        "abbreviation": "Circulation",
        "slug": "circulation",
    },
    # Hematology/Oncology
    "0006-4971": {
        "name": "Blood",
        "abbreviation": "Blood",
        "slug": "blood",
    },
    # Immunology
    "1074-7613": {
        "name": "Immunity",
        "abbreviation": "Immunity",
        "slug": "immunity",
    },
    "1529-2908": {
        "name": "Nature Immunology",
        "abbreviation": "Nat Immunol",
        "slug": "nature-immunology",
    },
    # Gastroenterology
    "0016-5085": {
        "name": "Gastroenterology",
        "abbreviation": "Gastroenterology",
        "slug": "gastroenterology",
    },
    "0017-5749": {
        "name": "Gut",
        "abbreviation": "Gut",
        "slug": "gut",
    },
    # Autophagy / Redox
    "1554-8627": {
        "name": "Autophagy",
        "abbreviation": "Autophagy",
        "slug": "autophagy",
    },
    "2213-2317": {
        "name": "Redox Biology",
        "abbreviation": "Redox Biol",
        "slug": "redox-biology",
    },
    # Immunology
    "2470-9468": {
        "name": "Science Immunology",
        "abbreviation": "Sci Immunol",
        "slug": "science-immunology",
    },
    # Broad-scope / multidisciplinary
    "0036-8075": {
        "name": "Science",
        "abbreviation": "Science",
        "slug": "science",
    },
    "0028-0836": {
        "name": "Nature",
        "abbreviation": "Nature",
        "slug": "nature",
    },
    "2041-1723": {
        "name": "Nature Communications",
        "abbreviation": "Nat Commun",
        "slug": "nature-communications",
    },
    # Nature specialty journals
    "1061-4036": {
        "name": "Nature Genetics",
        "abbreviation": "Nat Genet",
        "slug": "nature-genetics",
    },
    "1548-7091": {
        "name": "Nature Methods",
        "abbreviation": "Nat Methods",
        "slug": "nature-methods",
    },
    "1087-0156": {
        "name": "Nature Biotechnology",
        "abbreviation": "Nat Biotechnol",
        "slug": "nature-biotechnology",
    },
    "1465-7392": {
        "name": "Nature Cell Biology",
        "abbreviation": "Nat Cell Biol",
        "slug": "nature-cell-biology",
    },
    "1545-9993": {
        "name": "Nature Structural and Molecular Biology",
        "abbreviation": "Nat Struct Mol Biol",
        "slug": "nature-structural-molecular-biology",
    },
    # Clinical
    "0959-8138": {
        "name": "The BMJ",
        "abbreviation": "BMJ",
        "slug": "bmj",
    },
    "0923-7534": {
        "name": "Annals of Oncology",
        "abbreviation": "Ann Oncol",
        "slug": "annals-of-oncology",
    },
    "2374-2437": {
        "name": "JAMA Oncology",
        "abbreviation": "JAMA Oncol",
        "slug": "jama-oncology",
    },
    "2168-6106": {
        "name": "JAMA Internal Medicine",
        "abbreviation": "JAMA Intern Med",
        "slug": "jama-internal-medicine",
    },
    # Cell biology / molecular
    "1097-2765": {
        "name": "Molecular Cell",
        "abbreviation": "Mol Cell",
        "slug": "molecular-cell",
    },
    "1534-5807": {
        "name": "Developmental Cell",
        "abbreviation": "Dev Cell",
        "slug": "developmental-cell",
    },
    "0261-4189": {
        "name": "EMBO Journal",
        "abbreviation": "EMBO J",
        "slug": "embo-journal",
    },
    # Experimental medicine / open access
    "0022-1007": {
        "name": "Journal of Experimental Medicine",
        "abbreviation": "J Exp Med",
        "slug": "journal-of-experimental-medicine",
    },
    "1544-9173": {
        "name": "PLOS Biology",
        "abbreviation": "PLoS Biol",
        "slug": "plos-biology",
    },
}

# --- Paths ---
# Resolve relative to repo root
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(_REPO_ROOT, "data", "impact.db")
PMID_DATE_CACHE_PATH = os.path.join(_REPO_ROOT, "data", "pmid_dates.db")
PUBMED_BULK_DB_PATH  = os.path.join(_REPO_ROOT, "data", "pubmed_bulk.db")
WEBSITE_DATA_DIR = os.path.join(_REPO_ROOT, "docs", "data")
SNAPSHOTS_DIR = os.path.join(WEBSITE_DATA_DIR, "journals")

# --- Computation Parameters ---
ROLLING_WINDOW_MONTHS = 24

# --- PubMed E-utilities base URL ---
PUBMED_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# --- iCite API base URL ---
ICITE_BASE_URL = "https://icite.od.nih.gov/api/pubs"
