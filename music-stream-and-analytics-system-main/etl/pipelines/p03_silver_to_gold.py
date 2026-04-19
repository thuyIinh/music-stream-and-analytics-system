"""
p03_silver_to_gold.py
=====================
PIPELINE 3: SILVER → GOLD
Mục đích: Đọc Parquet từ Silver, tính aggregate đa chiều,
          lưu ra Gold layer chuẩn bị cho Pipeline 4 (Load DW).

Thay đổi so với phiên bản gốc:
  [1] Chỉ tính lượt nghe hợp lệ: duration_played > 20 giây
  [2] Gold Parquet chia từng ngày dữ liệu (play_date / show_date),
      không gộp nhiều ngày vào 1 file theo ngày chạy ETL

Outputs:
  gold/daily_play_stats/{date}.parquet
  gold/daily_user_stats/{date}.parquet
  gold/hourly_stats/{date}.parquet
  gold/daily_genre_stats/{date}.parquet
  gold/daily_ad_stats/{date}.parquet

Chạy: python pipelines/p03_silver_to_gold.py
"""

import json
import logging
from datetime import date
from pathlib import Path

import pandas as pd

# ─── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("p03_silver_to_gold")

# ─── constants ──────────────────────────────────────────────────────────────
MIN_LISTEN_SECONDS = 20   # [1] ngưỡng tính 1 lượt nghe hợp lệ

# ─── paths ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent  # etl/
DATA_LAKE = BASE_DIR / "Data_lake"
SILVER_DIR = DATA_LAKE / "silver"
GOLD_DIR   = DATA_LAKE / "gold"
LAST_RUN   = DATA_LAKE / "_last_run.json"

SILVER_PLAY   = SILVER_DIR / "events" / "play_history"
SILVER_AD_IMP = SILVER_DIR / "events" / "ad_impressions"

GOLD_PLAY_STATS  = GOLD_DIR / "daily_play_stats"
GOLD_USER_STATS  = GOLD_DIR / "daily_user_stats"
GOLD_HOURLY      = GOLD_DIR / "hourly_stats"
GOLD_GENRE_STATS = GOLD_DIR / "daily_genre_stats"
GOLD_AD_STATS    = GOLD_DIR / "daily_ad_stats"


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def ensure_dirs():
    for d in [GOLD_PLAY_STATS, GOLD_USER_STATS,
              GOLD_HOURLY, GOLD_GENRE_STATS, GOLD_AD_STATS]:
        d.mkdir(parents=True, exist_ok=True)


def read_last_run() -> str:
    """Lấy next_extract_from từ _last_run.json. Mặc định 2020-01-01."""
    if LAST_RUN.exists():
        with open(LAST_RUN) as f:
            data = json.load(f)
        return data.get("next_extract_from", "2020-01-01T00:00:00")
    return "2020-01-01T00:00:00"


def load_silver_parquets(silver_path: Path, since: str) -> pd.DataFrame:
    """
    Đọc các file {YYYY-MM-DD}.parquet trong silver_path có date >= since.
    Silver được pipeline 2 lưu theo ngày nên tên file luôn là YYYY-MM-DD.
    Trả về DataFrame gộp, hoặc DataFrame rỗng nếu không có file phù hợp.
    """
    since_date = pd.Timestamp(since)
    files = sorted(silver_path.glob("*.parquet"))

    if not files:
        log.warning(f"  Không có file Parquet trong {silver_path}")
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            file_date = pd.Timestamp(f.stem)   # stem = YYYY-MM-DD
            if file_date >= since_date:
                dfs.append(pd.read_parquet(f))
                log.info(f"    Đọc: {f.name}")
        except Exception:
            # Tên file không parse được thành ngày → đọc luôn (an toàn)
            log.warning(f"    Không parse được ngày từ '{f.name}', đọc luôn.")
            dfs.append(pd.read_parquet(f))

    if not dfs:
        log.info(f"  Không có file mới hơn {since} trong {silver_path}")
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def filter_valid_plays(ph: pd.DataFrame) -> pd.DataFrame:
    """
    [1] Chỉ giữ lượt nghe có duration_played > MIN_LISTEN_SECONDS.
    Ghi log số bản ghi bị loại để dễ audit.
    """
    before  = len(ph)
    valid   = ph[ph["duration_played"] > MIN_LISTEN_SECONDS].copy()
    dropped = before - len(valid)
    log.info(
        f"  Lọc lượt nghe hợp lệ (> {MIN_LISTEN_SECONDS}s): "
        f"giữ {len(valid):,} / {before:,} rows  (loại {dropped:,} rows)"
    )
    return valid


def save_gold(df: pd.DataFrame, gold_path: Path, tag: str,
              date_col: str) -> int:
    """
    [2] Lưu DataFrame ra Gold, chia 1 file Parquet + JSON cho mỗi ngày.

    date_col : cột ngày dùng để partition ('play_date' hoặc 'show_date').
               Mỗi giá trị unique → 1 file YYYY-MM-DD.parquet + .json.

    Trả về tổng số rows đã lưu.
    """
    if df.empty:
        log.warning(f"  [{tag}] DataFrame rỗng, bỏ qua.")
        return 0

    gold_path.mkdir(parents=True, exist_ok=True)
    total = 0

    for d in sorted(df[date_col].unique()):
        day_df  = df[df[date_col] == d].reset_index(drop=True)
        day_str = str(d)[:10]   # đảm bảo format YYYY-MM-DD

        out_parquet = gold_path / f"{day_str}.parquet"
        out_json    = gold_path / f"{day_str}.json"

        day_df.to_parquet(out_parquet, index=False, engine="pyarrow")
        day_df.to_json(out_json, orient="records",
                       force_ascii=False, indent=2, date_format="iso")

        log.info(
            f"  [{tag}] ✓ {len(day_df):,} rows "
            f"→ {out_parquet.relative_to(BASE_DIR)}"
        )
        total += len(day_df)

    return total


# ════════════════════════════════════════════════════════════════════════════
# AGGREGATIONS — play_history  (toàn bộ dùng ph đã lọc > 20s)
# ════════════════════════════════════════════════════════════════════════════

def agg_daily_play_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """
    GROUP BY song_id, artist_id, genre_id, play_date
    Tính: play_count, unique_listeners, total_duration,
          avg_completion_rate, skip_count, completed_count
    """
    ph = ph.copy()
    ph["play_date"] = pd.to_datetime(ph["played_at"]).dt.date

    agg = (
        ph.groupby(["song_id", "artist_id", "genre_id", "play_date"])
        .agg(
            play_count         =("play_id",          "count"),
            unique_listeners   =("user_id",           "nunique"),
            total_duration     =("duration_played",   "sum"),
            avg_completion_rate=("completion_rate",   "mean"),
            skip_count         =("is_skipped",        "sum"),
            completed_count    =("is_completed",      "sum"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    return agg


def agg_daily_user_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """
    GROUP BY user_id, play_date
    Tính: total_duration_played, total_songs, unique_songs,
          avg_completion_rate, completed_count, skip_count
    """
    ph = ph.copy()
    ph["play_date"] = pd.to_datetime(ph["played_at"]).dt.date

    agg = (
        ph.groupby(["user_id", "play_date"])
        .agg(
            total_duration_played=("duration_played", "sum"),
            total_songs          =("play_id",          "count"),
            unique_songs         =("song_id",           "nunique"),
            avg_completion_rate  =("completion_rate",   "mean"),
            completed_count      =("is_completed",      "sum"),
            skip_count           =("is_skipped",        "sum"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    return agg


def agg_hourly_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """
    GROUP BY play_date, hour_of_day, day_of_week
    Tính: play_count, unique_listeners, avg_completion_rate

    play_date được thêm vào group key để save_gold partition theo ngày.
    hour_of_day / day_of_week ưu tiên dùng cột sẵn có (pipeline 2 đã tính),
    nếu không có thì tự tính từ played_at.
    """
    ph = ph.copy()
    dt = pd.to_datetime(ph["played_at"])

    ph["play_date"]   = dt.dt.date
    ph["hour_of_day"] = ph["hour_of_day"] if "hour_of_day" in ph.columns else dt.dt.hour
    ph["day_of_week"] = ph["day_of_week"] if "day_of_week" in ph.columns else dt.dt.dayofweek

    agg = (
        ph.groupby(["play_date", "hour_of_day", "day_of_week"])
        .agg(
            play_count         =("play_id",         "count"),
            unique_listeners   =("user_id",          "nunique"),
            avg_completion_rate=("completion_rate",  "mean"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    return agg


def agg_daily_genre_stats(ph: pd.DataFrame) -> pd.DataFrame:
    """
    GROUP BY genre_id, play_date
    Tính: play_count, unique_listeners, total_duration, avg_completion_rate
    """
    ph = ph.copy()
    ph["play_date"] = pd.to_datetime(ph["played_at"]).dt.date
    ph = ph[ph["genre_id"].notna()]

    agg = (
        ph.groupby(["genre_id", "play_date"])
        .agg(
            play_count         =("play_id",          "count"),
            unique_listeners   =("user_id",           "nunique"),
            total_duration     =("duration_played",   "sum"),
            avg_completion_rate=("completion_rate",   "mean"),
        )
        .reset_index()
    )
    agg["avg_completion_rate"] = agg["avg_completion_rate"].round(4)
    agg["genre_id"] = agg["genre_id"].astype(int)
    return agg


# ════════════════════════════════════════════════════════════════════════════
# AGGREGATIONS — ad_impressions
# ════════════════════════════════════════════════════════════════════════════

def agg_daily_ad_stats(ad: pd.DataFrame) -> pd.DataFrame:
    """
    GROUP BY ad_id, show_date
    Tính: total_impressions, total_clicks, ctr (%)
    """
    ad = ad.copy()
    ad["show_date"] = pd.to_datetime(ad["shown_at"]).dt.date

    agg = (
        ad.groupby(["ad_id", "show_date"])
        .agg(
            total_impressions=("impression_id", "count"),
            total_clicks     =("is_clicked",    "sum"),
        )
        .reset_index()
    )
    agg["ctr"] = (agg["total_clicks"] / agg["total_impressions"] * 100).round(2)
    return agg


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def run(run_id: int = None) -> dict:
    """
    Chạy toàn bộ Pipeline 3.
    run_id : etl_runs.run_id để ghi log (None nếu chạy standalone).
    Trả về dict tổng kết số rows mỗi output.
    """
    log.info("=" * 60)
    log.info("PIPELINE 3: SILVER → GOLD")
    log.info("=" * 60)

    ensure_dirs()
    since = read_last_run()
    log.info(f"  Đọc Silver từ: {since}")

    stats = {}

    # ── 1. PLAY HISTORY ────────────────────────────────────────
    log.info("\n[1/2] Đọc silver play_history ...")
    ph = load_silver_parquets(SILVER_PLAY, since)

    if ph.empty:
        log.warning("  play_history Silver rỗng — bỏ qua các agg liên quan.")
        for k in ["daily_play_stats", "daily_user_stats",
                  "hourly_stats", "daily_genre_stats"]:
            stats[k] = 0
    else:
        log.info(f"  Tổng rows play_history (raw): {len(ph):,}")

        # Ép kiểu
        ph["played_at"]       = pd.to_datetime(ph["played_at"])
        ph["duration_played"] = pd.to_numeric(ph["duration_played"], errors="coerce").fillna(0)
        ph["completion_rate"] = pd.to_numeric(ph["completion_rate"], errors="coerce").fillna(0)
        ph["is_skipped"]      = ph["is_skipped"].astype(bool)
        ph["is_completed"]    = ph["is_completed"].astype(bool)

        # [1] Lọc lượt nghe hợp lệ > 20s — 1 lần, dùng chung cho tất cả aggregate
        ph = filter_valid_plays(ph)

        if ph.empty:
            log.warning("  Không còn row hợp lệ sau khi lọc 20s.")
            for k in ["daily_play_stats", "daily_user_stats",
                      "hourly_stats", "daily_genre_stats"]:
                stats[k] = 0
        else:
            # 1a. Daily Play Stats — [2] partition theo play_date
            log.info("  → daily_play_stats ...")
            stats["daily_play_stats"] = save_gold(
                agg_daily_play_stats(ph),
                GOLD_PLAY_STATS, "daily_play_stats", date_col="play_date",
            )

            # 1b. Daily User Stats — [2] partition theo play_date
            log.info("  → daily_user_stats ...")
            stats["daily_user_stats"] = save_gold(
                agg_daily_user_stats(ph),
                GOLD_USER_STATS, "daily_user_stats", date_col="play_date",
            )

            # 1c. Hourly Stats — [2] partition theo play_date
            log.info("  → hourly_stats ...")
            stats["hourly_stats"] = save_gold(
                agg_hourly_stats(ph),
                GOLD_HOURLY, "hourly_stats", date_col="play_date",
            )

            # 1d. Daily Genre Stats — [2] partition theo play_date
            log.info("  → daily_genre_stats ...")
            stats["daily_genre_stats"] = save_gold(
                agg_daily_genre_stats(ph),
                GOLD_GENRE_STATS, "daily_genre_stats", date_col="play_date",
            )

    # ── 2. AD IMPRESSIONS ──────────────────────────────────────
    log.info("\n[2/2] Đọc silver ad_impressions ...")
    ad = load_silver_parquets(SILVER_AD_IMP, since)

    if ad.empty:
        log.warning("  ad_impressions Silver rỗng — bỏ qua.")
        stats["daily_ad_stats"] = 0
    else:
        log.info(f"  Tổng rows ad_impressions: {len(ad):,}")
        ad["shown_at"]   = pd.to_datetime(ad["shown_at"])
        ad["is_clicked"] = ad["is_clicked"].astype(bool)

        # [2] partition theo show_date
        log.info("  → daily_ad_stats ...")
        stats["daily_ad_stats"] = save_gold(
            agg_daily_ad_stats(ad),
            GOLD_AD_STATS, "daily_ad_stats", date_col="show_date",
        )

    # ── SUMMARY ────────────────────────────────────────────────
    total_rows = sum(stats.values())
    log.info("\n" + "=" * 60)
    log.info("PIPELINE 3 HOÀN THÀNH")
    for k, v in stats.items():
        log.info(f"  {k:<25} {v:>10,} rows")
    log.info(f"  {'TỔNG':<25} {total_rows:>10,} rows")
    log.info("=" * 60)

    return stats


if __name__ == "__main__":
    run()