"""
p03_silver_to_gold.py
=====================
PIPELINE 3: SILVER → GOLD
Mục đích: Đọc Parquet từ Silver, tính aggregate đa chiều,
          lưu ra Gold layer chuẩn bị cho Pipeline 4 (Load DW).

Quy tắc ngày tháng:
  - Ngày xử lý (processing_date) = TÊN FILE parquet ở Silver.
  - Các hàm aggregate KHÔNG tạo thêm cột date từ data, KHÔNG GROUP BY ngày.
  - File Gold output được đặt tên theo processing_date (tên file silver gốc).
  - Chạy với --date YYYY-MM-DD: chỉ load file silver tên khớp đúng ngày đó.
  - Nếu không truyền --date: tự dùng ngày hôm nay (datetime.now()).
"""

import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2

# ─── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("p03_silver_to_gold")

# ─── constants ──────────────────────────────────────────────────────────────
MIN_LISTEN_SECONDS = 20

# ─── paths ──────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent.parent
DATA_LAKE  = BASE_DIR / "data_lake"
SILVER_DIR = DATA_LAKE / "silver"
GOLD_DIR   = DATA_LAKE / "gold"

SILVER_PLAY   = SILVER_DIR / "events" / "play_history"
SILVER_AD_IMP = SILVER_DIR / "events" / "ad_impressions"

GOLD_PLAY_STATS  = GOLD_DIR / "daily_play_stats"
GOLD_USER_STATS  = GOLD_DIR / "daily_user_stats"
GOLD_HOURLY      = GOLD_DIR / "hourly_stats"
GOLD_GENRE_STATS = GOLD_DIR / "daily_genre_stats"
GOLD_AD_STATS    = GOLD_DIR / "daily_ad_stats"

# ─── database config ────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "music_app_db",
    "user":     "postgres",
    "password": "nam2005S",
}

PIPELINE_NAME = "p03_silver_to_gold"


# ══════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def db_start_run(conn, triggered_by: str = "manual") -> int:
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
    log.info(
        f"[DB] etl_runs: Cập nhật run_id={run_id} "
        f"-> status={status}, duration={duration}s"
    )


def db_start_detail(conn, run_id: int, stage: str, table_name: str) -> int:
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

def ensure_dirs():
    for d in [GOLD_PLAY_STATS, GOLD_USER_STATS,
              GOLD_HOURLY, GOLD_GENRE_STATS, GOLD_AD_STATS]:
        d.mkdir(parents=True, exist_ok=True)


def load_silver_parquets(
    silver_path: Path,
    target_date: str,
) -> list[tuple[str, pd.DataFrame]]:
    """
    Trả về list[(date_str, DataFrame)].
    date_str = tên file (stem), VD: "2026-04-21".

    Chỉ load file có tên == target_date (YYYY-MM-DD.parquet).
    Dữ liệu bên trong KHÔNG bao giờ bị filter theo cột ngày trong data.
    """
    files = sorted(silver_path.glob("*.parquet"))

    if not files:
        log.warning(f"  Không có file Parquet trong {silver_path}")
        return []

    target_date_obj = pd.Timestamp(target_date).date()
    result = []

    for f in files:
        try:
            file_date = pd.Timestamp(f.stem).date()
        except Exception:
            log.warning(f"    Bỏ qua file tên không phải ngày: {f.name}")
            continue
        if file_date == target_date_obj:
            log.info(f"    Đọc file: {f.name}")
            result.append((f.stem, pd.read_parquet(f)))

    if not result:
        log.info(f"  Không tìm thấy file {target_date}.parquet trong {silver_path}")

    return result
def save_gold(df: pd.DataFrame, gold_path: Path, tag: str, date_str: str) -> int:
    """Lưu DataFrame ra Gold. Tên file = date_str (tên file silver gốc)."""
    if df.empty:
        log.warning(f"  [{tag}] DataFrame rỗng, bỏ qua.")
        return 0

    gold_path.mkdir(parents=True, exist_ok=True)
    out_parquet = gold_path / f"{date_str}.parquet"
    out_json    = gold_path / f"{date_str}.json"

    df.to_parquet(out_parquet, index=False, engine="pyarrow")
    df.to_json(out_json, orient="records", force_ascii=False, indent=2, date_format="iso")
    log.info(f"  [{tag}] {len(df):,} rows -> {out_parquet.relative_to(BASE_DIR)}")
    return len(df)


# ══════════════════════════════════════════════
# AGGREGATIONS — play_history
#
# Nguyên tắc: KHÔNG tạo cột date từ data, KHÔNG GROUP BY ngày.
# Ngày đã được xác định từ tên file bởi caller.
# Mỗi file silver = 1 ngày → aggregate toàn bộ rồi lưu ra 1 file gold cùng tên.
# ══════════════════════════════════════════════

def agg_daily_play_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """GROUP BY song_id, artist_id, genre_id."""
    agg = (
        ph.groupby(["song_id", "artist_id", "genre_id"])
        .agg(
            play_count         =("play_id",         "count"),
            unique_listeners   =("user_id",          "nunique"),
            total_duration     =("duration_played",  "sum"),
            avg_completion_rate=("completion_rate",  "mean"),
            skip_count         =("is_skipped",       "sum"),
            completed_count    =("is_completed",     "sum"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    return agg


def agg_daily_user_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """GROUP BY user_id."""
    agg = (
        ph.groupby(["user_id"])
        .agg(
            total_duration_played=("duration_played", "sum"),
            total_songs          =("play_id",          "count"),
            unique_songs         =("song_id",          "nunique"),
            avg_completion_rate  =("completion_rate",  "mean"),
            completed_count      =("is_completed",     "sum"),
            skip_count           =("is_skipped",       "sum"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    return agg


def agg_hourly_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """
    GROUP BY hour_of_day, day_of_week.
    Hai cột này đã được Pipeline 2 ghi sẵn vào Silver;
    nếu thiếu thì tính lại từ played_at.
    """
    ph = ph.copy()
    if "hour_of_day" not in ph.columns or "day_of_week" not in ph.columns:
        dt = pd.to_datetime(ph["played_at"])
        if "hour_of_day" not in ph.columns:
            ph["hour_of_day"] = dt.dt.hour
        if "day_of_week" not in ph.columns:
            ph["day_of_week"] = dt.dt.day_of_week

    agg = (
        ph.groupby(["hour_of_day", "day_of_week"])
        .agg(
            play_count         =("play_id",        "count"),
            unique_listeners   =("user_id",         "nunique"),
            avg_completion_rate=("completion_rate", "mean"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    return agg


def agg_daily_genre_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """GROUP BY genre_id."""
    ph = ph[ph["genre_id"].notna()].copy()
    agg = (
        ph.groupby(["genre_id"])
        .agg(
            play_count         =("play_id",         "count"),
            unique_listeners   =("user_id",          "nunique"),
            total_duration     =("duration_played",  "sum"),
            avg_completion_rate=("completion_rate",  "mean"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    agg["genre_id"] = agg["genre_id"].astype(int)
    return agg


# ══════════════════════════════════════════════
# AGGREGATIONS — ad_impressions
# ══════════════════════════════════════════════

def agg_daily_ad_stats(ad: pd.DataFrame) -> pd.DataFrame:
    """GROUP BY ad_id."""
    agg = (
        ad.groupby(["ad_id"])
        .agg(
            total_impressions=("impression_id", "count"),
            total_clicks     =("is_clicked",    "sum"),
        )
        .reset_index()
    )
    agg["ctr"] = (agg["total_clicks"] / agg["total_impressions"] * 100).round(2)
    return agg


# ══════════════════════════════════════════════
# CORE LOGIC
# ══════════════════════════════════════════════

def process_aggregate(
    name: str,
    df: pd.DataFrame,
    agg_fn,
    gold_path: Path,
    date_str: str,
    conn,
    run_id: int,
) -> dict:
    """Chạy một aggregate và lưu Gold. date_str dùng để đặt tên file output."""
    log.info(f"--  [{name}]  Bắt đầu  --")

    detail_started = datetime.now()
    detail_id = db_start_detail(conn, run_id, stage="silver_to_gold", table_name=name)

    rows_in    = len(df)
    rows_out   = 0
    rows_error = 0
    status     = "success"
    message    = None

    try:
        if df.empty:
            status  = "skipped"
            message = "DataFrame đầu vào rỗng."
            log.warning(f"  [{name}] Bỏ qua — dữ liệu đầu vào rỗng.")
        else:
            df_agg   = agg_fn(df)
            rows_out = save_gold(df_agg, gold_path, name, date_str=date_str)

    except Exception as e:
        status  = "failed"
        message = str(e)
        log.error(f"  [{name}] FAILED: {e}", exc_info=True)

    finally:
        db_finish_detail(
            conn, detail_id,
            status=status,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_error=rows_error,
            message=message,
            started_at=detail_started,
        )

    log.info(
        f"--  [{name}]  {status.upper()}  "
        f"(in={rows_in:,}, out={rows_out:,}, err={rows_error})  --\n"
    )
    return {"rows_in": rows_in, "rows_out": rows_out,
            "rows_error": rows_error, "status": status}


# ══════════════════════════════════════════════
# ENTRYPOINT
# ══════════════════════════════════════════════

def run(triggered_by: str = "manual", target_date: str = None) -> dict:
    log.info("=" * 60)
    log.info("  PIPELINE 3: SILVER -> GOLD")
    log.info("=" * 60 + "\n")

    ensure_dirs()

    try:
        conn = get_db_connection()
    except Exception as e:
        log.error(f"[DB] Không thể kết nối PostgreSQL: {e}")
        raise

    pipeline_started = datetime.now()
    run_id           = db_start_run(conn, triggered_by=triggered_by)

    total_extracted   = 0
    total_transformed = 0
    total_loaded      = 0
    total_failed      = 0
    pipeline_status   = "success"
    pipeline_error    = None
    stats             = {}

    try:
        if not target_date:
            target_date = datetime.now().strftime("%Y-%m-%d")
            log.info(f"  Không truyền --date, dùng ngày hôm nay: {target_date}\n")

        # ── 1. PLAY HISTORY ────────────────────────────────────────
        log.info("[1/2] Đọc silver play_history ...")
        ph_files = load_silver_parquets(SILVER_PLAY, target_date=target_date)
        log.info(f"  Tìm thấy {len(ph_files)} file play_history.\n")

        for date_str, ph_raw in ph_files:
            log.info(f"  ── Xử lý play_history [{date_str}] ──")
            rows_ph_raw      = len(ph_raw)
            total_extracted += rows_ph_raw

            # Cast kiểu dữ liệu
            ph_raw["played_at"]       = pd.to_datetime(ph_raw["played_at"])
            ph_raw["duration_played"] = pd.to_numeric(ph_raw["duration_played"], errors="coerce").fillna(0)
            ph_raw["completion_rate"] = pd.to_numeric(ph_raw["completion_rate"], errors="coerce").fillna(0)
            ph_raw["is_skipped"]      = ph_raw["is_skipped"].astype(bool)
            ph_raw["is_completed"]    = ph_raw["is_completed"].astype(bool)

            # Lọc lượt nghe hợp lệ
            ph      = ph_raw[ph_raw["duration_played"] > MIN_LISTEN_SECONDS].copy()
            dropped = rows_ph_raw - len(ph)
            total_failed += dropped
            log.info(
                f"  Lọc > {MIN_LISTEN_SECONDS}s: "
                f"giữ {len(ph):,} / {rows_ph_raw:,} rows  (loại {dropped:,})"
            )

            # 4 aggregation, file output đặt tên theo date_str
            for name, agg_fn, gold_path in [
                ("daily_play_stats",  agg_daily_play_stats,  GOLD_PLAY_STATS),
                ("daily_user_stats",  agg_daily_user_stats,  GOLD_USER_STATS),
                ("hourly_stats",      agg_hourly_stats,      GOLD_HOURLY),
                ("daily_genre_stats", agg_daily_genre_stats, GOLD_GENRE_STATS),
            ]:
                result = process_aggregate(
                    name=name, df=ph, agg_fn=agg_fn,
                    gold_path=gold_path, date_str=date_str,
                    conn=conn, run_id=run_id,
                )
                stats[f"{name}|{date_str}"] = result["rows_out"]
                total_transformed          += result["rows_out"]
                total_loaded               += result["rows_out"]
                if result["status"] == "failed":
                    pipeline_status = "failed"

        # ── 2. AD IMPRESSIONS ──────────────────────────────────────
        log.info("[2/2] Đọc silver ad_impressions ...")
        ad_files = load_silver_parquets(SILVER_AD_IMP, target_date=target_date)
        log.info(f"  Tìm thấy {len(ad_files)} file ad_impressions.\n")

        for date_str, ad in ad_files:
            log.info(f"  ── Xử lý ad_impressions [{date_str}] ──")
            total_extracted += len(ad)

            ad["shown_at"]   = pd.to_datetime(ad["shown_at"])
            ad["is_clicked"] = ad["is_clicked"].astype(bool)

            result = process_aggregate(
                name="daily_ad_stats", df=ad, agg_fn=agg_daily_ad_stats,
                gold_path=GOLD_AD_STATS, date_str=date_str,
                conn=conn, run_id=run_id,
            )
            stats[f"daily_ad_stats|{date_str}"] = result["rows_out"]
            total_transformed                   += result["rows_out"]
            total_loaded                        += result["rows_out"]
            if result["status"] == "failed":
                pipeline_status = "failed"

    except Exception as e:
        pipeline_status = "failed"
        pipeline_error  = str(e)
        log.error(f"Pipeline gặp lỗi nghiêm trọng: {e}", exc_info=True)

    finally:
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

    total_rows = sum(stats.values())
    log.info("=" * 60)
    log.info(f"  Pipeline 3 hoàn thành — status={pipeline_status}")
    for k, v in stats.items():
        log.info(f"    {k:<45} {v:>8,} rows")
    log.info(f"    {'TỔNG':<45} {total_rows:>8,} rows")
    log.info(f"  extracted={total_extracted:,}  loaded={total_loaded:,}  failed={total_failed:,}")
    log.info("=" * 60)

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline 3: Silver to Gold")
    parser.add_argument("--triggered_by", type=str, default="manual",
                        help="Người/hệ thống trigger script")
    parser.add_argument("--date", type=str, default=None,
                        help="Target date để chạy ETL (YYYY-MM-DD)")

    args, unknown = parser.parse_known_args()

    trigger_source = args.triggered_by
    if unknown and args.triggered_by == "manual":
        trigger_source = unknown[0]

    run(triggered_by=trigger_source, target_date=args.date)
