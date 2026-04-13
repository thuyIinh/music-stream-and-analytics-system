from pathlib import Path

# ─── Database ────────────────────────────────────────────────
DB_URL = "postgresql://postgres.mjmeexzmkzfgrdzvpadd:quanghuy238@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

# ─── Paths ───────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent / "data_lake"

BRONZE_DIR = BASE_DIR / "bronze"
SILVER_DIR = BASE_DIR / "silver"
GOLD_DIR   = BASE_DIR / "gold"

LAST_RUN_FILE = BASE_DIR / "_last_run.json"

# ─── Các bảng incremental ────────────────────────────────────
EXTRACT_TABLES = {
    "play_history":   "played_at",
    "payments":       "paid_at",
    "likes":          "liked_at",
    "follows":        "followed_at",
    "ad_impressions": "shown_at",
}

# ─── Các bảng reference (full load) ─────────────────────────
REFERENCE_TABLES = [
    "users", "user_profiles", "songs", "artists",
    "albums", "genres", "subscriptions", "ads"
]

# ─── Transform settings ───────────────────────────────────────
COMPLETION_THRESHOLD = 0.8
SKIP_THRESHOLD       = 0.3
SESSION_GAP_MINUTES  = 30