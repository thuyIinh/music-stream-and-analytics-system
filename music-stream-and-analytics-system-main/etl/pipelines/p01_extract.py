"""
extract.py — Bronze pipeline
Kết nối Supabase → extract incremental + full reference tables
"""

import json
from datetime import datetime, timezone, timedelta
import argparse

import psycopg2
import psycopg2.extras

from config.settings import (
    DB_URL, BRONZE_DIR, LAST_RUN_FILE,
    EXTRACT_TABLES, REFERENCE_TABLES
)

VN_TZ = timezone(timedelta(hours=7))

# ─── Helpers ─────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(DB_URL)


def load_last_run(reset: bool = False) -> dict:
    if reset or not LAST_RUN_FILE.exists():
        return {
            "last_success_run_id": 0,
            "last_success_at": None,
            "next_extract_from": "2000-01-01T00:00:00+07:00"
        }
    with open(LAST_RUN_FILE, "r", encoding="utf-8") as f:
        return json.load(f)
def write_log(conn, run_id: int, run_type: str, status: str,
              extract_from: str, extract_to: str,
              total_rows: int = 0, details: dict = None, error_msg: str = None):
    # 1. Ghi vào bảng etl_runs
    run_sql = """
        INSERT INTO etl_runs (
            run_id, pipeline_name, started_at, finished_at, 
            status, rows_extracted, error_message
        )
        VALUES (
            %(run_id)s, %(pipeline_name)s, %(started_at)s, NOW(), 
            %(status)s, %(total_rows)s, %(error_msg)s
        )
        ON CONFLICT (run_id) DO UPDATE SET
            finished_at = EXCLUDED.finished_at,
            status = EXCLUDED.status,
            rows_extracted = EXCLUDED.rows_extracted,
            error_message = EXCLUDED.error_message;
    """

    # 2. Ghi vào bảng etl_run_details
    detail_sql = """
        INSERT INTO etl_run_details (
            run_id, stage, table_name, finished_at, rows_out, status
        )
        VALUES (
            %(run_id)s, 'extract', %(table_name)s, NOW(), %(rows_out)s, %(status)s
        )
    """

    with conn.cursor() as cur:
        # Thực thi ghi log tổng
        cur.execute(run_sql, {
            "run_id":         run_id,
            "pipeline_name":  f"p01_extract_{run_type}", # VD: p01_extract_incremental
            "started_at":     extract_from, # Lấy thời gian bắt đầu quét
            "status":         status,
            "total_rows":     total_rows,
            "error_msg":      error_msg
        })

        # Thực thi ghi log chi tiết cho từng bảng (nếu có dictionary details truyền vào)
        if details:
            for table_name, rows_count in details.items():
                cur.execute(detail_sql, {
                    "run_id":     run_id,
                    "table_name": table_name,
                    "rows_out":   rows_count,
                    "status":     "success"
                })

    conn.commit()


def save_last_run(run_id: int, success_at: str):
    LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LAST_RUN_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "last_success_run_id": run_id,
            "last_success_at": success_at,
            "next_extract_from": success_at
        }, f, indent=2)


def write_bronze(table: str, rows: list, run_id: int, extract_from: str, extract_to: str):
    if not rows:
        print(f"  [bronze] {table}: 0 rows (không có dữ liệu mới)")
        return 0

    if table in EXTRACT_TABLES:
        folder = BRONZE_DIR / "events" / table
    else:
        folder = BRONZE_DIR / "catalog" / table

    folder.mkdir(parents=True, exist_ok=True)

    ts_str = datetime.now(VN_TZ).strftime("%Y_%m_%d_%H_%M_%S")
    data_file = folder / f"{ts_str}.json"
    meta_file = folder / f"{ts_str}_metadata.json"  # metadata file riêng

    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, default=str, indent=2)

    metadata = {
        "created_at": datetime.now(VN_TZ).isoformat(),
        "source_table": table,
        "row_count": len(rows),
        "etl_run_id": run_id,
        "extract_from": extract_from,
        "extract_to": extract_to,
        "file": data_file.name
    }
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"  [bronze] {table}: {len(rows):,} rows → {data_file.name}")
    return len(rows)


# ─── Extract logic ────────────────────────────────────────────

def extract_table(conn, table: str, ts_col: str, extract_from: str, extract_to: str) -> list:
    sql = f"""
        SELECT *
        FROM {table}
        WHERE {ts_col} > %(from_ts)s
          AND {ts_col} <= %(to_ts)s
        ORDER BY {ts_col} ASC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, {"from_ts": extract_from, "to_ts": extract_to})
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def extract_reference_table(conn, table: str) -> list:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
    return [dict(r) for r in rows]


# ─── Main ─────────────────────────────────────────────────────

def run(start_time: datetime = None,reset: bool = False):
    if start_time is None:
        start_time = datetime.now()
    last_run     = load_last_run(reset=reset)
    extract_from = last_run["next_extract_from"]
    extract_to   = start_time.isoformat()
    run_id       = last_run.get("last_success_run_id", 0) + 1
    run_type     = "reset" if reset else "incremental"

    print(f"\n{'='*60}")
    print(f"BRONZE EXTRACT  |  Run ID: {run_id}")
    if reset:
        print("⚠️  MODE: RESET FULL EXTRACT (từ 2000-01-01)")
    print(f"  From : {extract_from}")
    print(f"  To   : {extract_to}")
    print(f"{'='*60}\n")

    results    = {}
    total_rows = 0
    conn       = None
    current_table = "N/A"

    try:
        conn = get_connection()
        write_log(conn, run_id, run_type, "running", extract_from, extract_to, 0, {})

        for table, ts_col in EXTRACT_TABLES.items():
            current_table = table
            print(f"→ Extracting incremental [{table}] ...")
            rows  = extract_table(conn, table, ts_col, extract_from, extract_to)
            count = write_bronze(table, rows, run_id, extract_from, extract_to)
            results[table] = count
            total_rows += count

        for table in REFERENCE_TABLES:
            current_table = table
            print(f"→ Extracting reference [{table}] (full) ...")
            rows  = extract_reference_table(conn, table)
            count = write_bronze(table, rows, run_id, extract_from, extract_to)
            results[table] = count
            total_rows += count

        # Ghi log TRƯỚC khi close
        write_log(conn, run_id, run_type, "success",
                  extract_from, extract_to, total_rows, results)

        save_last_run(run_id, extract_to)
        print(f"\n✅ Extract thành công!")
        print(f"   Tổng rows: {total_rows:,}")
        for t, c in results.items():
            if c > 0:
                print(f"     • {t}: {c:,}")

    except Exception as e:
        if conn:
            try:
                error_detail = f"Lỗi tại bảng '{current_table}': {str(e)}"
                write_log(conn, run_id, run_type, "error",
                          extract_from, extract_to, total_rows, results, error_detail)
            except Exception:
                pass
        print(f"\n❌ Extract lỗi: {e}")
        raise

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true",
                       help="Reset extract từ đầu (lấy toàn bộ dữ liệu)")
    args = parser.parse_args()

    run(reset=args.reset)