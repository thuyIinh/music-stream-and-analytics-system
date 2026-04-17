"""
p04_load_dw.py
==============
PIPELINE 4: LOAD DATA WAREHOUSE
Mục đích: Đẩy dữ liệu từ Silver/Gold vào PostgreSQL DW.

Luồng:
  A. Load Dimensions (dim_genre → dim_artist → dim_ad → dim_song → dim_user)
  B. Load Facts      (fact_plays, fact_payments, fact_likes, fact_ad_impressions)
     └─ DimMaps được load 1 lần lên RAM rồi dùng chung cho tất cả fact

Chạy: python pipelines/p04_load_dw.py
"""

import json
import logging
import sys
from pathlib import Path

import psycopg2
import numpy as np
from psycopg2.extensions import register_adapter, AsIs

# Dạy psycopg2 cách đọc các kiểu dữ liệu số của NumPy (Pandas)
register_adapter(np.int64, AsIs)
register_adapter(np.int32, AsIs)
register_adapter(np.float64, AsIs)
register_adapter(np.float32, AsIs)

# ─── sys.path ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent   # etl/
sys.path.insert(0, str(BASE_DIR))

from loaders.dim_loader  import run_all_dims
from loaders.fact_loaders import run_all_facts

# ─── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("p04_load_dw")

# ─── paths ──────────────────────────────────────────────────────────────────
DATA_LAKE      = BASE_DIR / "Data_lake"
LAST_RUN       = DATA_LAKE / "_last_run.json"
SILVER_CATALOG = DATA_LAKE / "silver" / "catalog"
SILVER_EVENTS  = DATA_LAKE / "silver" / "events"

# ─── DB config ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "music_app_db",
    "user":     "postgres",
    "password": "nam2005S",
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def read_last_run() -> str:
    if LAST_RUN.exists():
        with open(LAST_RUN) as f:
            return json.load(f).get("next_extract_from", "2020-01-01T00:00:00")
    return "2020-01-01T00:00:00"


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def run(run_id: int = None) -> dict:
    """
    Chạy toàn bộ Pipeline 4.
    run_id: etl_runs.run_id để ghi log (None nếu chạy standalone).
    Trả về dict tổng kết rows loaded.
    """
    log.info("=" * 60)
    log.info("PIPELINE 4: LOAD DATA WAREHOUSE")
    log.info("=" * 60)

    since = read_last_run()
    log.info(f"  Incremental từ: {since}")

    conn = get_conn()
    conn.autocommit = False

    all_stats = {}

    try:
        # ── A. DIMENSIONS ─────────────────────────────────────
        log.info("\n[A] Load Dimensions ...")
        dim_stats = run_all_dims(conn, SILVER_CATALOG)
        all_stats.update(dim_stats)

        # ── B. FACTS ──────────────────────────────────────────
        log.info("\n[B] Load Facts ...")
        fact_stats = run_all_facts(conn, SILVER_EVENTS, since)
        all_stats.update(fact_stats)

    except Exception as e:
        conn.rollback()
        log.error(f"  Pipeline 4 FAILED: {e}")
        conn.close()
        raise

    conn.close()

    # ── SUMMARY ───────────────────────────────────────────────
    total = sum(all_stats.values())
    log.info("\n" + "=" * 60)
    log.info("PIPELINE 4 HOÀN THÀNH")
    log.info("  Dimensions:")
    for k in ["dim_genre", "dim_artist", "dim_ad", "dim_song", "dim_user"]:
        log.info(f"    {k:<25} {all_stats.get(k, 0):>8,} rows")
    log.info("  Facts:")
    for k in ["fact_plays", "fact_payments", "fact_likes", "fact_ad_impressions"]:
        log.info(f"    {k:<25} {all_stats.get(k, 0):>8,} rows")
    log.info(f"  {'TỔNG':<25} {total:>8,} rows")
    log.info("=" * 60)

    return all_stats


if __name__ == "__main__":
    run()
