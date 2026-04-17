"""
p05_refresh_mart.py
===================
PIPELINE 5: REFRESH MATERIALIZED VIEWS
Mục đích: Làm mới tất cả Materialized Views trong schema mart theo đúng
          thứ tự phụ thuộc, ghi log, rồi cập nhật _last_run.json.

Điểm quan trọng nhất:
  next_extract_from = start_time_extract (lấy từ đầu Pipeline 1)
  KHÔNG phải thời điểm kết thúc ETL — tránh mất data sinh ra trong lúc ETL chạy.

Chạy: python pipelines/p05_refresh_mart.py
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2

# ─── sys.path ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent   # etl/
sys.path.insert(0, str(BASE_DIR))

# ─── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("p05_refresh_mart")

# ─── paths ──────────────────────────────────────────────────────────────────
DATA_LAKE = BASE_DIR / "Data_lake"
LAST_RUN  = DATA_LAKE / "_last_run.json"

# ─── DB config ──────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "music_app_db",
    "user":     "postgres",
    "password": "nam2005S",
}


def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True   # REFRESH MATERIALIZED VIEW yêu cầu autocommit=True
    return conn


# ════════════════════════════════════════════════════════════════════════════
# THỨ TỰ REFRESH — từ ít phụ thuộc → nhiều phụ thuộc
# Mỗi nhóm là 1 list, các view trong cùng nhóm độc lập nhau (có thể song song
# về mặt logic, nhưng ở đây chạy tuần tự cho đơn giản và an toàn).
# ════════════════════════════════════════════════════════════════════════════

REFRESH_GROUPS = [
    # ── Nhóm 1: Top Songs (daily → weekly → monthly theo dependency) ──────
    {
        "group": "top_songs",
        "views": [
            "mart.top_songs_daily",
            "mart.top_songs_weekly",
            "mart.top_songs_monthly",
        ],
    },
    # ── Nhóm 2: Genre Stats ───────────────────────────────────────────────
    {
        "group": "genre_stats",
        "views": [
            "mart.genre_stats_daily",
            "mart.genre_stats_monthly",
        ],
    },
    # ── Nhóm 3: Các view độc lập ─────────────────────────────────────────
    {
        "group": "standalone_stats",
        "views": [
            "mart.hourly_heatmap",
            "mart.artist_stats_monthly",
            "mart.yoy_comparison",
            "mart.completion_stats",
        ],
    },
    # ── Nhóm 4: User Activity ─────────────────────────────────────────────
    {
        "group": "user_activity",
        "views": [
            "mart.dau_wau_mau",
            "mart.user_plan_distribution",
            "mart.user_plan_monthly",
        ],
    },
    # ── Nhóm 5: User Segmentation (segment_summary phụ thuộc user_segmentation)
    {
        "group": "user_segmentation",
        "views": [
            "mart.user_segmentation",
            "mart.segment_summary",       # phụ thuộc user_segmentation
        ],
    },
    # ── Nhóm 6: User Behaviour ────────────────────────────────────────────
    {
        "group": "user_behaviour",
        "views": [
            "mart.avg_session_length",
            "mart.dau_trend",
            "mart.user_personal_stats",
            "mart.user_preferences",
        ],
    },
    # ── Nhóm 7: Revenue ───────────────────────────────────────────────────
    {
        "group": "revenue",
        "views": [
            "mart.revenue_daily",
            "mart.revenue_monthly",
            "mart.revenue_by_plan",
        ],
    },
    # ── Nhóm 8: Recommendations (chạy cuối vì có thể phụ thuộc nhiều view)
    {
        "group": "recommendations",
        "views": [
            "mart.song_recommendations",
        ],
    },
]


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def refresh_view(cur, view_name: str) -> dict:
    """
    Chạy REFRESH MATERIALIZED VIEW CONCURRENTLY cho 1 view.
    Trả về dict chứa view_name, row_count, elapsed_seconds, status.
    Lưu ý: CONCURRENTLY yêu cầu view phải có UNIQUE INDEX.
    """
    t0 = time.time()
    result = {
        "view_name":       view_name,
        "row_count":       0,
        "elapsed_seconds": 0,
        "status":          "success",
        "error":           None,
    }

    try:
        cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")

        # Đếm rows sau khi refresh
        cur.execute(f"SELECT COUNT(*) FROM {view_name}")
        result["row_count"] = cur.fetchone()[0]

    except psycopg2.errors.ObjectNotInPrerequisiteState:
        # View chưa có UNIQUE INDEX → fallback sang REFRESH không CONCURRENTLY
        log.warning(
            f"  [{view_name}] Chưa có UNIQUE INDEX, fallback → REFRESH không CONCURRENTLY"
        )
        try:
            cur.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
            cur.execute(f"SELECT COUNT(*) FROM {view_name}")
            result["row_count"] = cur.fetchone()[0]
            result["status"]    = "success_no_concurrent"
        except Exception as e2:
            result["status"] = "failed"
            result["error"]  = str(e2)
            log.error(f"  [{view_name}] FAILED: {e2}")

    except Exception as e:
        result["status"] = "failed"
        result["error"]  = str(e)
        log.error(f"  [{view_name}] FAILED: {e}")

    result["elapsed_seconds"] = round(time.time() - t0, 2)
    return result


def log_view_detail(log_cur, run_id: int,
                    view_name: str, result: dict, t0: datetime):
    """Ghi kết quả 1 view vào etl_run_details."""
    if run_id is None:
        return
    try:
        log_cur.execute(
            """
            INSERT INTO etl_run_details
                (run_id, stage, table_name, started_at, finished_at,
                 rows_in, rows_out, rows_error, status, message)
            VALUES (%s, 'refresh_mart', %s, %s, %s, 0, %s, 0, %s, %s)
            """,
            (
                run_id,
                view_name,
                t0,
                datetime.now(),
                result["row_count"],
                result["status"],
                result.get("error"),
            ),
        )
    except Exception as e:
        log.warning(f"  Không ghi được etl_run_details cho {view_name}: {e}")


def finish_etl_run(log_cur, run_id: int, total_rows: int, status: str = "success"):
    """Cập nhật etl_runs.status = success sau khi toàn bộ pipeline xong."""
    if run_id is None:
        return
    try:
        log_cur.execute(
            """
            UPDATE etl_runs SET
                finished_at      = %s,
                duration_seconds = EXTRACT(EPOCH FROM (%s - started_at))::INTEGER,
                status           = %s,
                rows_loaded      = %s
            WHERE run_id = %s
            """,
            (datetime.now(), datetime.now(), status, total_rows, run_id),
        )
    except Exception as e:
        log.warning(f"  Không update được etl_runs run_id={run_id}: {e}")


def update_last_run(start_time_extract: datetime):
    """
    Cập nhật _last_run.json:
    1. Đọc nội dung cũ để không làm mất 'last_success_run_id' của P1.
    2. Ghi đè next_extract_from = start_time_extract.
    3. Dùng tiếng Anh không dấu để tránh lỗi UnicodeDecodeError trên Windows.
    """
    DATA_LAKE.mkdir(parents=True, exist_ok=True)

    payload = {}

    if LAST_RUN.exists():
        try:
            with open(LAST_RUN, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            log.warning(f"  Không đọc được _last_run.json cũ ({e}), sẽ tạo mới toàn bộ.")


    payload["next_extract_from"] = start_time_extract.isoformat()
    payload["last_updated_at"] = datetime.now().isoformat()

    payload["note"] = "Updated by P05 - Next run will start from here"

    with open(LAST_RUN, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    log.info(f"  _last_run.json cập nhật → next_extract_from = {start_time_extract.isoformat()}")


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def run(run_id: int = None, start_time_extract: datetime = None) -> dict:
    """
    Chạy toàn bộ Pipeline 5.

    Args:
        run_id             : etl_runs.run_id từ Pipeline 1 (None nếu chạy standalone).
        start_time_extract : thời điểm BẮT ĐẦU Pipeline 1 — dùng để ghi _last_run.json.
                             Nếu None và chạy standalone → dùng datetime.now().
    Returns:
        dict tổng kết: view_name → row_count
    """
    log.info("=" * 60)
    log.info("PIPELINE 5: REFRESH MATERIALIZED VIEWS")
    log.info("=" * 60)

    if start_time_extract is None:
        # Standalone mode: đọc từ _last_run.json hoặc dùng now
        if LAST_RUN.exists():
            with open(LAST_RUN) as f:
                ts = json.load(f).get("next_extract_from")
            start_time_extract = datetime.fromisoformat(ts) if ts else datetime.now()
        else:
            start_time_extract = datetime.now()
        log.warning(
            f"  start_time_extract không được truyền vào, dùng: {start_time_extract}"
        )

    conn     = get_conn()   # autocommit=True
    cur      = conn.cursor()

    # Connection riêng để ghi log (cần commit độc lập)
    log_conn = get_conn()
    log_conn.autocommit = True
    log_cur  = log_conn.cursor()

    all_results  = {}
    total_rows   = 0
    failed_views = []

    pipeline_t0 = datetime.now()

    for group_cfg in REFRESH_GROUPS:
        group_name = group_cfg["group"]
        views      = group_cfg["views"]

        log.info(f"\n── Nhóm: {group_name} ({'  →  '.join(views)})")

        for view_name in views:
            view_t0 = datetime.now()
            log.info(f"  Refreshing {view_name} ...")

            result = refresh_view(cur, view_name)

            # Ghi log từng view
            log_view_detail(log_cur, run_id, view_name, result, view_t0)

            all_results[view_name] = result["row_count"]
            total_rows += result["row_count"]

            status_icon = "✓" if result["status"].startswith("success") else "✗"
            log.info(
                f"  {status_icon} {view_name:<40} "
                f"{result['row_count']:>8,} rows  "
                f"({result['elapsed_seconds']}s)"
            )

            if result["status"] == "failed":
                failed_views.append(view_name)

    # ── Kết thúc etl_runs ─────────────────────────────────────
    final_status = "success" if not failed_views else "partial"
    finish_etl_run(log_cur, run_id, total_rows, status=final_status)

    # ── Cập nhật _last_run.json ───────────────────────────────
    # Dùng start_time_extract (đầu Pipeline 1), KHÔNG phải now()
    update_last_run(start_time_extract)

    cur.close()
    conn.close()
    log_cur.close()
    log_conn.close()

    # ── Summary ───────────────────────────────────────────────
    elapsed = round((datetime.now() - pipeline_t0).total_seconds(), 1)
    log.info("\n" + "=" * 60)
    log.info("PIPELINE 5 HOÀN THÀNH")
    log.info(f"  Tổng views refreshed : {len(all_results)}")
    log.info(f"  Tổng rows            : {total_rows:,}")
    log.info(f"  Thời gian            : {elapsed}s")
    log.info(f"  Status               : {final_status}")
    if failed_views:
        log.error(f"  Views FAILED ({len(failed_views)}): {failed_views}")
    log.info(f"  next_extract_from    : {start_time_extract.isoformat()}")
    log.info("=" * 60)

    return all_results


if __name__ == "__main__":
    run()
