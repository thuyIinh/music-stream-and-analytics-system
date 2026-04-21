"""
p04_load_dw.py
==============
PIPELINE 4: LOAD DATA WAREHOUSE
Ghi lịch sử chạy vào etl_runs + etl_run_details (giống pattern p03).
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import psycopg2
import numpy as np
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64,   AsIs)
register_adapter(np.int32,   AsIs)
register_adapter(np.float64, AsIs)
register_adapter(np.float32, AsIs)

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from loaders.dim_loader   import run_all_dims
from loaders.fact_loaders import run_all_facts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("p04_load_dw")

DATA_LAKE      = BASE_DIR / "Data_lake"
LAST_RUN       = DATA_LAKE / "_last_run.json"
SILVER_CATALOG = DATA_LAKE / "silver" / "catalog"
SILVER_EVENTS  = DATA_LAKE / "silver" / "events"

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "music_app_db",
    "user":     "postgres",
    "password": "nam2005S",
}

PIPELINE_NAME = "p04_load_dw"

# Thứ tự các table — dùng để lặp summary và tracking
DIM_TABLES  = ["dim_genre", "dim_artist", "dim_ad", "dim_song", "dim_user"]
FACT_TABLES = ["fact_plays", "fact_payments", "fact_likes", "fact_ad_impressions"]


# ══════════════════════════════════════════════
# DB HELPERS — giống hệt pattern p03
# ══════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def db_start_run(conn, triggered_by: str = "manual") -> tuple[int, datetime]:
    started_at = datetime.now()
    sql = """
        INSERT INTO etl_runs (pipeline_name, started_at, status, triggered_by)
        VALUES (%s, %s, 'running', %s)
        RETURNING run_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PIPELINE_NAME, started_at, triggered_by))
        run_id = cur.fetchone()[0]
    conn.commit()
    log.info(f"[DB] etl_runs: Tạo run_id={run_id}")
    return run_id, started_at


def db_finish_run(
    conn,
    run_id:          int,
    status:          str,
    started_at:      datetime,
    rows_loaded:     int = 0,
    rows_failed:     int = 0,
    error_message:   str = None,
) -> None:
    finished_at = datetime.now()
    duration    = int((finished_at - started_at).total_seconds())
    sql = """
        UPDATE etl_runs
        SET finished_at       = %s,
            duration_seconds  = %s,
            status            = %s,
            rows_extracted    = %s,
            rows_transformed  = %s,
            rows_loaded       = %s,
            rows_failed       = %s,
            error_message     = %s
        WHERE run_id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            finished_at, duration, status,
            rows_loaded,  # p04 không extract/transform riêng
            0,
            rows_loaded,
            rows_failed,
            error_message,
            run_id,
        ))
    conn.commit()
    log.info(f"[DB] etl_runs: run_id={run_id} -> status={status}, duration={duration}s")


def db_start_detail(
    conn, run_id: int, stage: str, table_name: str
) -> tuple[int, datetime]:
    started_at = datetime.now()
    sql = """
        INSERT INTO etl_run_details (run_id, stage, table_name, started_at, status)
        VALUES (%s, %s, %s, %s, 'running')
        RETURNING detail_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, stage, table_name, started_at))
        detail_id = cur.fetchone()[0]
    conn.commit()
    return detail_id, started_at


def db_finish_detail(
    conn,
    detail_id:  int,
    started_at: datetime,
    status:     str,
    rows_out:   int = 0,
    message:    str = None,
) -> None:
    sql = """
        UPDATE etl_run_details
        SET finished_at = %s,
            rows_in     = %s,
            rows_out    = %s,
            rows_error  = %s,
            status      = %s,
            message     = %s
        WHERE detail_id = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            datetime.now(),
            rows_out,   # load DW: rows_in = rows_out (không transform)
            rows_out,
            0,
            status,
            message,
            detail_id,
        ))
    conn.commit()


# ══════════════════════════════════════════════
# HELPER: ghi etl_run_details cho từng table
# từ dict stats trả về bởi run_all_dims / run_all_facts
# ══════════════════════════════════════════════

def _write_details_from_stats(
    conn,
    run_id:     int,
    stage:      str,
    stats:      dict,           # {table_name: rows_loaded}
    err_tables: set = None,     # tên table bị lỗi (nếu có)
) -> None:
    """
    Với mỗi table trong stats, ghi 1 record vào etl_run_details.
    Vì run_all_dims/facts đã chạy xong trước khi gọi hàm này,
    started_at được ước tính = now() (chấp nhận được cho mục đích audit).
    """
    err_tables = err_tables or set()
    for table_name, rows in stats.items():
        detail_id, det_started = db_start_detail(conn, run_id, stage, table_name)
        status = "failed" if table_name in err_tables else "success"
        db_finish_detail(
            conn, detail_id,
            started_at=det_started,
            status=status,
            rows_out=rows,
        )
        log.info(f"  [DB] etl_run_details: {table_name} -> {status}, {rows:,} rows")


# ══════════════════════════════════════════════
# HELPERS CŨ
# ══════════════════════════════════════════════

def read_last_run() -> str:
    if LAST_RUN.exists():
        with open(LAST_RUN) as f:
            return json.load(f).get("next_extract_from", "2020-01-01T00:00:00")
    return "2020-01-01T00:00:00"


# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════

def run(triggered_by: str = "manual") -> dict:
    log.info("=" * 60)
    log.info("  PIPELINE 4: LOAD DATA WAREHOUSE")
    log.info("=" * 60)

    since = read_last_run()
    log.info(f"  Incremental từ: {since}\n")

    conn = get_conn()
    conn.autocommit = False

    # ── Tạo etl_runs record ───────────────────────────────────
    run_id, run_started = db_start_run(conn, triggered_by=triggered_by)

    all_stats       = {}
    total_loaded    = 0
    total_failed    = 0
    pipeline_status = "success"
    pipeline_error  = None

    try:
        # ── A. DIMENSIONS ──────────────────────────────────────
        log.info("[A] Load Dimensions ...")
        dim_stats   = {}
        dim_failed  = set()

        try:
            dim_stats = run_all_dims(conn, SILVER_CATALOG)
        except Exception as e:
            pipeline_status = "failed"
            pipeline_error  = str(e)
            # Điền 0 cho tất cả dim để vẫn ghi được detail
            dim_stats  = {t: 0 for t in DIM_TABLES}
            dim_failed = set(DIM_TABLES)
            log.error(f"  [Dimensions] FAILED: {e}", exc_info=True)

        # Ghi etl_run_details: 1 record / dim table
        _write_details_from_stats(
            conn, run_id,
            stage="p04_load_dw",
            stats=dim_stats,
            err_tables=dim_failed,
        )
        all_stats.update(dim_stats)
        total_loaded += sum(dim_stats.values())

        # ── B. FACTS ──────────────────────────────────────────
        log.info("\n[B] Load Facts ...")
        fact_stats  = {}
        fact_failed = set()

        try:
            fact_stats = run_all_facts(conn, SILVER_EVENTS, since)
        except Exception as e:
            if pipeline_status != "failed":
                pipeline_status = "failed"
                pipeline_error  = str(e)
            fact_stats  = {t: 0 for t in FACT_TABLES}
            fact_failed = set(FACT_TABLES)
            log.error(f"  [Facts] FAILED: {e}", exc_info=True)

        # Ghi etl_run_details: 1 record / fact table
        _write_details_from_stats(
            conn, run_id,
            stage="p04_load_dw",
            stats=fact_stats,
            err_tables=fact_failed,
        )
        all_stats.update(fact_stats)
        total_loaded += sum(fact_stats.values())

    except Exception as e:
        pipeline_status = "failed"
        pipeline_error  = str(e)
        conn.rollback()
        log.error(f"  Pipeline 4 lỗi nghiêm trọng: {e}", exc_info=True)

    finally:
        # ── Cập nhật etl_runs ─────────────────────────────────
        db_finish_run(
            conn,
            run_id=run_id,
            status=pipeline_status,
            started_at=run_started,
            rows_loaded=total_loaded,
            rows_failed=total_failed,
            error_message=pipeline_error,
        )
        conn.close()
        log.info("[DB] Đã đóng kết nối.")

    # ── SUMMARY ───────────────────────────────────────────────
    total = sum(all_stats.values())
    log.info("\n" + "=" * 60)
    log.info(f"  PIPELINE 4 HOÀN THÀNH — status={pipeline_status}")
    log.info("  Dimensions:")
    for k in DIM_TABLES:
        log.info(f"    {k:<25} {all_stats.get(k, 0):>8,} rows")
    log.info("  Facts:")
    for k in FACT_TABLES:
        log.info(f"    {k:<25} {all_stats.get(k, 0):>8,} rows")
    log.info(f"  {'TỔNG':<25} {total:>8,} rows")
    log.info("=" * 60)

    return all_stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline 4: Load DW")
    parser.add_argument("--triggered_by", type=str, default="manual")
    args = parser.parse_args()
    run(triggered_by=args.triggered_by)