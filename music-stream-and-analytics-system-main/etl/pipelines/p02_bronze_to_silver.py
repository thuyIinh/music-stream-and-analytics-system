"""
pipelines/p02_bronze_to_silver.py
PIPELINE 2: BRONZE → SILVER  —  Làm sạch, validate, denormalize.
Có ghi log vào PostgreSQL (etl_runs + etl_run_details).
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# PATHS
# ──────────────────────────────────────────────
ROOT         = Path(__file__).parent.parent          # etl/
DATA_LAKE    = ROOT / "Data_lake"
BRONZE       = DATA_LAKE / "bronze"
SILVER       = DATA_LAKE / "silver"
REJECTED     = DATA_LAKE / "rejected"
TRANSFORMS   = ROOT / "transforms"

sys.path.insert(0, str(TRANSFORMS))

PROCESS_DATE = datetime.today().strftime("%Y-%m-%d")

# ──────────────────────────────────────────────
# DATABASE CONFIG
# Đọc từ biến môi trường, fallback về giá trị mặc định local
# ──────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "music_app_db",
    "user":     "postgres",
    "password": "nam2005S",
}

PIPELINE_NAME = "p02_bronze_to_silver"

# ──────────────────────────────────────────────
# CONFIG: CATALOG & EVENT TABLES
# ──────────────────────────────────────────────

# 1. Nhóm Catalog (11 thư mục)
CATALOG_TABLES = {
    "ads":             "ad_transformer",
    "albums":          "album_transformer",
    "artists":         "artist_transformer",
    "genres":          "genre_transformer",
    "playlist_songs":  "playlist_song_transformer",
    "playlists":       "playlist_transformer",
    "song_genres":     "song_genre_transformer",
    "songs":           "song_transformer",
    "subscriptions":   "subscription_transformer",
    "user_profiles":   "user_profile_transformer",
    "users":           "user_transformer",
}

# 2. Nhóm Events (5 thư mục)
EVENT_TABLES = {
    "ad_impressions": "ad_impression_transformer",
    "follows":        "follow_transformer",
    "likes":          "like_transformer",
    "payments":       "payment_transformer",
    "play_history":   "play_history_transformer",
}


# ══════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════

def get_db_connection():
    """Tạo kết nối PostgreSQL. Raise nếu không kết nối được."""
    return psycopg2.connect(**DB_CONFIG)


def db_start_run(conn, triggered_by: str = "manual") -> int:
    """
    Tạo 1 record mới trong etl_runs với status='running'.
    Trả về run_id vừa tạo.
    """
    sql = """
        INSERT INTO etl_runs (pipeline_name, started_at, status, triggered_by)
        VALUES (%s, %s, 'running', %s)
        RETURNING run_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (PIPELINE_NAME, datetime.now(), triggered_by))
        run_id = cur.fetchone()[0]
    conn.commit()
    log.info(f"[DB] etl_runs: Tạo run_id={run_id}")
    return run_id


def db_finish_run(
    conn,
    run_id: int,
    status: str,
    rows_extracted: int = 0,
    rows_transformed: int = 0,
    rows_loaded: int = 0,
    rows_failed: int = 0,
    error_message: str = None,
    started_at: datetime = None,
) -> None:
    """Cập nhật etl_runs khi pipeline kết thúc (thành công hoặc thất bại)."""
    finished_at = datetime.now()
    duration = int((finished_at - started_at).total_seconds()) if started_at else None

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
            rows_extracted, rows_transformed, rows_loaded, rows_failed,
            error_message, run_id,
        ))
    conn.commit()
    log.info(f"[DB] etl_runs: Cập nhật run_id={run_id} → status={status}, duration={duration}s")


def db_start_detail(conn, run_id: int, stage: str, table_name: str) -> int:
    """
    Tạo 1 record trong etl_run_details khi bắt đầu xử lý 1 table.
    Trả về detail_id.
    """
    sql = """
        INSERT INTO etl_run_details (run_id, stage, table_name, started_at, status)
        VALUES (%s, %s, %s, %s, 'running')
        RETURNING detail_id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, stage, table_name, datetime.now()))
        detail_id = cur.fetchone()[0]
    conn.commit()
    return detail_id


def db_finish_detail(
    conn,
    detail_id: int,
    status: str,
    rows_in: int = 0,
    rows_out: int = 0,
    rows_error: int = 0,
    message: str = None,
    started_at: datetime = None,
) -> None:
    """Cập nhật etl_run_details khi xử lý 1 table xong."""
    finished_at = datetime.now()

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
            finished_at, rows_in, rows_out, rows_error,
            status, message, detail_id,
        ))
    conn.commit()


# ══════════════════════════════════════════════
# FILE HELPERS
# ══════════════════════════════════════════════

def read_bronze_json(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy: {path}")

    text = path.read_text(encoding="utf-8").strip()

    try:
        data = json.loads(text)
        if isinstance(data, dict):
            data = [data]
    except json.JSONDecodeError:
        data = [json.loads(line) for line in text.splitlines() if line.strip()]

    return pd.DataFrame(data)


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False, engine="pyarrow")
    log.info(f"    ✔  Lưu {len(df):,} dòng  →  {path.relative_to(ROOT)}")


def save_json(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_json(path, orient="records", force_ascii=False, indent=2, date_format="iso")
    log.info(f"    ✔  Lưu JSON {len(df):,} dòng  →  {path.relative_to(ROOT)}")


def save_rejected(df: pd.DataFrame, path: Path) -> None:
    if df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_json(path, orient="records", force_ascii=False, indent=2, date_format="iso")
    log.warning(f"    ⚠  Đã đẩy {len(df):,} dòng lỗi → {path.relative_to(ROOT)}")


def load_transformer(module_name: str):
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        log.warning(f"    ⚠  Không tìm thấy transformer '{module_name}', bỏ qua.")
        return None


def bronze_event_path(table: str, date: str) -> Path:
    date_formatted = date.replace("-", "_")
    folder = BRONZE / "events" / table

    if folder.is_dir():
        files = sorted([
            f for f in folder.glob(f"{date_formatted}*.json")
            if not f.name.endswith("_metadata.json")
        ])
        if files:
            return files[-1]

    exact = folder / f"{date}.json"
    if exact.exists():
        return exact

    raise FileNotFoundError(f"Không có file bronze")


def silver_event_path(table: str, date: str) -> Path:
    return SILVER / "events" / table / f"{date}.parquet"


def bronze_catalog_path(table: str) -> Path:
    folder = BRONZE / "catalog" / table
    if folder.is_dir():
        files = sorted([
            f for f in folder.glob("*.json")
            if not f.name.endswith("_metadata.json")
        ])
        if not files:
            raise FileNotFoundError(f"Không có file data JSON hợp lệ trong {folder}")
        return files[-1]

    single = BRONZE / "catalog" / f"{table}.json"
    if single.exists():
        return single

    raise FileNotFoundError(f"Không tìm thấy bronze catalog cho '{table}'")


def silver_catalog_path(table: str) -> Path:
    return SILVER / "catalog"/ table / f"{table}.parquet"


def rejected_path(table: str, date: str) -> Path:
    return REJECTED / "events" / table / f"{date}_errors.json"


# ══════════════════════════════════════════════
# CORE LOGIC — Xử lý 1 table, có ghi DB log
# ══════════════════════════════════════════════

def process_table(
    table: str,
    category: str,
    conn,
    run_id: int,
    date: str = None,
) -> dict:
    """
    Xử lý Bronze → Silver cho 1 table.
    Ghi log vào etl_run_details.
    Trả về dict thống kê: {rows_in, rows_out, rows_error, status}
    """
    log.info(f"──  [{category}/{table}]  Bắt đầu  ──")

    # Mở detail record trong DB
    detail_started = datetime.now()
    detail_id = db_start_detail(conn, run_id, stage="bronze_to_silver", table_name=table)

    rows_in = rows_out = rows_error = 0
    status  = "success"
    message = None

    try:
        # 1. Đường dẫn & transformer
        if category == "events":
            bronze_path = bronze_event_path(table, date)
            silver_path = silver_event_path(table, date)
            transformer_name = EVENT_TABLES.get(table)
            rej_path = rejected_path(table, date)
        else:
            bronze_path = bronze_catalog_path(table)
            silver_path = silver_catalog_path(table)
            transformer_name = CATALOG_TABLES.get(table)
            rej_path = REJECTED / "catalog" / table / "errors.json"

        # 2. Đọc Bronze
        df = read_bronze_json(bronze_path)
        rows_in = len(df)
        log.info(f"    Đọc {rows_in:,} dòng thô")

        df_clean    = df
        df_rejected = None

        # 3. Transform
        if transformer_name:
            mod = load_transformer(transformer_name)
            if mod and hasattr(mod, "transform"):
                if table == "play_history":
                    songs_df = read_bronze_json(bronze_catalog_path("songs"))
                    df_clean, df_rejected = mod.transform(df, songs_df=songs_df)
                else:
                    df_clean, df_rejected = mod.transform(df)
            else:
                message = f"Transformer '{transformer_name}' không hợp lệ, giữ nguyên data thô."
                log.warning(f"    ⚠ {message}")
        else:
            message = f"Không có transformer cho '{table}', giữ nguyên data thô."
            log.warning(f"    ⚠ {message}")

        # 4. Lưu Silver
        if df_clean is not None and not df_clean.empty:
            rows_out = len(df_clean)
            save_parquet(df_clean, silver_path)
            save_json(df_clean, silver_path.with_suffix(".json"))
        else:
            log.warning(f"    ⚠  Dữ liệu sạch rỗng, KHÔNG lưu file.")

        # 5. Lưu Rejected
        if df_rejected is not None and not df_rejected.empty:
            rows_error = len(df_rejected)
            save_rejected(df_rejected, rej_path)

    except FileNotFoundError as e:
        status  = "skipped"
        message = str(e)
        log.warning(f"    ⚠  Bỏ qua [{category}/{table}]: {e}")

    except Exception as e:
        status  = "failed"
        message = str(e)
        log.error(f"    ✖  [{category}/{table}] FAILED: {e}", exc_info=True)

    finally:
        # Ghi kết quả vào DB dù thành công hay lỗi
        db_finish_detail(
            conn, detail_id,
            status=status,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_error=rows_error,
            message=message,
            started_at=detail_started,
        )

    log.info(f"──  [{category}/{table}]  {status.upper()}  (in={rows_in}, out={rows_out}, err={rows_error})  ──\n")

    return {
        "rows_in":    rows_in,
        "rows_out":   rows_out,
        "rows_error": rows_error,
        "status":     status,
    }


# ══════════════════════════════════════════════
# ENTRYPOINT
# ══════════════════════════════════════════════

def run(date: str = PROCESS_DATE, triggered_by: str = "manual") -> None:
    log.info("=" * 60)
    log.info(f"  PIPELINE 2: BRONZE → SILVER   date={date}")
    log.info("=" * 60 + "\n")

    # Kết nối DB một lần dùng suốt pipeline
    try:
        conn = get_db_connection()
    except Exception as e:
        log.error(f"[DB] Không thể kết nối PostgreSQL: {e}")
        raise

    # Tạo run record
    pipeline_started = datetime.now()
    run_id = db_start_run(conn, triggered_by=triggered_by)

    # Tổng hợp số liệu toàn pipeline
    total_extracted   = 0
    total_transformed = 0
    total_loaded      = 0
    total_failed      = 0
    pipeline_status   = "success"
    pipeline_error    = None

    try:
        all_tables = (
            [(tbl, "catalog") for tbl in CATALOG_TABLES]
            + [(tbl, "events") for tbl in EVENT_TABLES]
        )

        for tbl, category in all_tables:
            result = process_table(
                table=tbl,
                category=category,
                conn=conn,
                run_id=run_id,
                date=date,
            )

            # Cộng dồn vào tổng — chỉ tính những table không bị skip/failed
            total_extracted   += result["rows_in"]
            total_transformed += result["rows_out"]   # Bronze đọc vào = extracted
            total_loaded      += result["rows_out"]
            total_failed      += result["rows_error"]

            # Pipeline bị đánh dấu failed nếu có bất kỳ table nào failed
            if result["status"] == "failed":
                pipeline_status = "failed"

    except Exception as e:
        pipeline_status = "failed"
        pipeline_error  = str(e)
        log.error(f"Pipeline gặp lỗi nghiêm trọng: {e}", exc_info=True)

    finally:
        # Cập nhật etl_runs dù pipeline thành công hay lỗi
        db_finish_run(
            conn,
            run_id=run_id,
            status=pipeline_status,
            rows_extracted=total_extracted,
            rows_transformed=total_transformed,
            rows_loaded=total_loaded,
            rows_failed=total_failed,
            error_message=pipeline_error,
            started_at=pipeline_started,
        )
        conn.close()
        log.info("[DB] Đã đóng kết nối.")

    log.info("=" * 60)
    log.info(f"  Pipeline 2 hoàn thành — status={pipeline_status}")
    log.info(f"  Tổng: extracted={total_extracted:,}  loaded={total_loaded:,}  failed={total_failed:,}")
    log.info("=" * 60)


if __name__ == "__main__":
    date_arg        = sys.argv[1] if len(sys.argv) > 1 else PROCESS_DATE
    triggered_by    = sys.argv[2] if len(sys.argv) > 2 else "manual"
    run(date_arg, triggered_by)