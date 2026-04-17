"""
loaders/fact_loader.py
======================
Load tất cả Fact tables vào dw schema.

Chiến lược:
  1. Load toàn bộ dim key mapping lên RAM (1 lần duy nhất).
  2. Với mỗi fact table:
     - Đọc Silver Parquet.
     - Merge với dim DataFrame trên RAM → ánh xạ surrogate key.
     - Loại bỏ dòng không map được key bắt buộc.
     - Bulk INSERT vào DW với ON CONFLICT (id_gốc) DO NOTHING.
"""

import logging
from pathlib import Path

import pandas as pd
import psycopg2.extras

log = logging.getLogger("fact_loader")

BATCH_SIZE = 10_000


# ════════════════════════════════════════════════════════════════════════════
# STEP 1 — TẢI KEY MAPPING LÊN RAM
# ════════════════════════════════════════════════════════════════════════════

class DimMaps:
    """
    Giữ toàn bộ surrogate key mapping trong RAM.
    Tra cứu bằng pandas merge → O(n) thay vì O(n×m) loop.
    """

    def __init__(self, conn):
        log.info("  [DimMaps] Tải surrogate key mapping lên RAM ...")
        cur = conn.cursor()

        # dim_user: chỉ lấy is_current=true
        cur.execute(
            "SELECT user_id, user_key FROM dw.dim_user WHERE is_current = true"
        )
        self.user = pd.DataFrame(cur.fetchall(), columns=["user_id", "user_key"])

        cur.execute("SELECT song_id, song_key FROM dw.dim_song")
        self.song = pd.DataFrame(cur.fetchall(), columns=["song_id", "song_key"])

        cur.execute("SELECT artist_id, artist_key FROM dw.dim_artist")
        self.artist = pd.DataFrame(cur.fetchall(), columns=["artist_id", "artist_key"])

        cur.execute("SELECT genre_id, genre_key FROM dw.dim_genre")
        self.genre = pd.DataFrame(cur.fetchall(), columns=["genre_id", "genre_key"])

        cur.execute("SELECT ad_id, ad_key FROM dw.dim_ad")
        self.ad = pd.DataFrame(cur.fetchall(), columns=["ad_id", "ad_key"])

        # dim_date: date_key theo format YYYYMMDD
        cur.execute("SELECT date_key, full_date FROM dw.dim_date")
        rows = cur.fetchall()
        self.date = pd.DataFrame(rows, columns=["date_key", "full_date"])
        self.date["full_date"] = pd.to_datetime(self.date["full_date"]).dt.date

        cur.close()
        log.info(
            f"  [DimMaps] users={len(self.user):,}  songs={len(self.song):,}"
            f"  artists={len(self.artist):,}  genres={len(self.genre):,}"
            f"  ads={len(self.ad):,}  dates={len(self.date):,}"
        )


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _read_silver_parquets(silver_path: Path, since: str) -> pd.DataFrame:
    """Đọc tất cả Parquet trong silver_path có date >= since."""
    since_ts = pd.Timestamp(since)
    files = sorted(silver_path.glob("*.parquet"))
    dfs = []
    for f in files:
        try:
            if pd.Timestamp(f.stem) >= since_ts:
                dfs.append(pd.read_parquet(f))
        except Exception:
            dfs.append(pd.read_parquet(f))

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _bulk_insert(cur, sql: str, df: pd.DataFrame,
                 cols: list, tag: str) -> int:
    """Chia DataFrame thành batch rồi execute_values."""
    if df.empty:
        return 0

    total = 0
    rows_all = [tuple(r) for r in df[cols].itertuples(index=False)]

    for i in range(0, len(rows_all), BATCH_SIZE):
        chunk = rows_all[i: i + BATCH_SIZE]
        psycopg2.extras.execute_values(cur, sql, chunk, page_size=BATCH_SIZE)
        total += len(chunk)

    log.info(f"  [{tag}] Inserted {total:,} rows")
    return total


def _add_date_key(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Thêm cột play_date (date) để merge với dim_date."""
    df = df.copy()
    df["_play_date"] = pd.to_datetime(df[ts_col]).dt.date
    return df


# ════════════════════════════════════════════════════════════════════════════
# STEP 2 — FACT_PLAYS
# ════════════════════════════════════════════════════════════════════════════

def load_fact_plays(conn, silver_events: Path, maps: DimMaps, since: str) -> int:
    """
    Source: silver/events/play_history/*.parquet
    Lookup: user_key, song_key, artist_key, genre_key, date_key
    Target: dw.fact_plays
    Idempotency: ON CONFLICT (play_id) DO NOTHING
    """
    log.info("  [fact_plays] Đọc Silver play_history ...")
    df = _read_silver_parquets(silver_events / "play_history", since)

    if df.empty:
        log.warning("  [fact_plays] Không có dữ liệu mới.")
        return 0

    log.info(f"  [fact_plays] {len(df):,} rows trước lookup")

    # ── Lookup surrogate keys (pandas merge trên RAM) ──────────
    df = _add_date_key(df, "played_at")

    df = df.merge(maps.user,   on="user_id",   how="left")
    df = df.merge(maps.song,   on="song_id",   how="left")
    df = df.merge(maps.artist, on="artist_id", how="left")
    df = df.merge(
        maps.genre,
        on="genre_id", how="left"
    )
    df = df.merge(
        maps.date.rename(columns={"full_date": "_play_date"}),
        on="_play_date", how="left"
    )

    # ── Loại bỏ dòng thiếu key bắt buộc ───────────────────────
    before = len(df)
    df = df.dropna(subset=["user_key", "song_key", "artist_key", "date_key"])
    dropped = before - len(df)
    if dropped:
        log.warning(f"  [fact_plays] Loại {dropped:,} dòng thiếu surrogate key")

    # Chuyển kiểu
    for col in ["user_key", "song_key", "artist_key", "date_key"]:
        df[col] = df[col].astype(int)
    df["genre_key"] = df["genre_key"].where(df["genre_key"].notna(), other=None)

    # ── Bulk INSERT ────────────────────────────────────────────
    cur = conn.cursor()

    INSERT_SQL = """
        INSERT INTO dw.fact_plays
            (
             user_key, song_key, artist_key, genre_key, date_key,
             hour_of_day, day_of_week, duration_played, song_duration,
             completion_rate, is_completed, is_skipped, source, played_at)
        VALUES %s
    """

    cols = [
        "user_key", "song_key", "artist_key", "genre_key", "date_key",
        "hour_of_day", "day_of_week", "duration_played", "duration",
        "completion_rate", "is_completed", "is_skipped", "source", "played_at",
    ]

    n = _bulk_insert(cur, INSERT_SQL, df, cols, "fact_plays")
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# STEP 3 — FACT_PAYMENTS
# ════════════════════════════════════════════════════════════════════════════

def load_fact_payments(conn, silver_events: Path, maps: DimMaps, since: str) -> int:
    """
    Source: silver/events/payments/*.parquet
    Lookup: user_key, date_key
    Target: dw.fact_payments
    Idempotency: ON CONFLICT (payment_id) DO NOTHING
    """
    log.info("  [fact_payments] Đọc Silver payments ...")
    df = _read_silver_parquets(silver_events / "payments", since)

    if df.empty:
        log.warning("  [fact_payments] Không có dữ liệu mới.")
        return 0

    log.info(f"  [fact_payments] {len(df):,} rows trước lookup")

    df = _add_date_key(df, "paid_at")
    df = df.merge(maps.user, on="user_id", how="left")
    df = df.merge(
        maps.date.rename(columns={"full_date": "_play_date"}),
        on="_play_date", how="left"
    )

    before = len(df)
    df = df.dropna(subset=["user_key", "date_key"])
    if before - len(df):
        log.warning(f"  [fact_payments] Loại {before - len(df):,} dòng thiếu key")

    for col in ["user_key", "date_key"]:
        df[col] = df[col].astype(int)

    cur = conn.cursor()

    INSERT_SQL = """
        INSERT INTO dw.fact_payments
            ( user_key, date_key, amount, currency,
             plan, duration_days, status, paid_at)
        VALUES %s
    """

    cols = [ "user_key", "date_key", "amount", "currency",
            "plan", "duration_days", "status", "paid_at"]

    n = _bulk_insert(cur, INSERT_SQL, df, cols, "fact_payments")
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# STEP 4 — FACT_LIKES
# ════════════════════════════════════════════════════════════════════════════

def load_fact_likes(conn, silver_events: Path, maps: DimMaps, since: str) -> int:
    """
    Source: silver/events/likes/*.parquet
    Lookup: user_key, song_key, date_key
    Target: dw.fact_likes
    Idempotency: ON CONFLICT (like_id) DO NOTHING
    """
    log.info("  [fact_likes] Đọc Silver likes ...")
    df = _read_silver_parquets(silver_events / "likes", since)

    if df.empty:
        log.warning("  [fact_likes] Không có dữ liệu mới.")
        return 0

    log.info(f"  [fact_likes] {len(df):,} rows trước lookup")

    df = _add_date_key(df, "liked_at")
    df = df.merge(maps.user, on="user_id", how="left")
    df = df.merge(maps.song, on="song_id", how="left")
    df = df.merge(
        maps.date.rename(columns={"full_date": "_play_date"}),
        on="_play_date", how="left"
    )

    before = len(df)
    df = df.dropna(subset=["user_key", "song_key", "date_key"])
    if before - len(df):
        log.warning(f"  [fact_likes] Loại {before - len(df):,} dòng thiếu key")

    for col in ["user_key", "song_key", "date_key"]:
        df[col] = df[col].astype(int)

    cur = conn.cursor()

    INSERT_SQL = """
        INSERT INTO dw.fact_likes
            ( user_key, song_key, date_key, liked_at)
        VALUES %s
    """

    cols = ["user_key", "song_key", "date_key", "liked_at"]

    n = _bulk_insert(cur, INSERT_SQL, df, cols, "fact_likes")
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# STEP 5 — FACT_AD_IMPRESSIONS
# ════════════════════════════════════════════════════════════════════════════

def load_fact_ad_impressions(conn, silver_events: Path,
                              maps: DimMaps, since: str) -> int:
    """
    Source: silver/events/ad_impressions/*.parquet
    Lookup: ad_key, user_key, date_key
    Target: dw.fact_ad_impressions
    Idempotency: ON CONFLICT (impression_id) DO NOTHING
    """
    log.info("  [fact_ad_impressions] Đọc Silver ad_impressions ...")
    df = _read_silver_parquets(silver_events / "ad_impressions", since)

    if df.empty:
        log.warning("  [fact_ad_impressions] Không có dữ liệu mới.")
        return 0

    log.info(f"  [fact_ad_impressions] {len(df):,} rows trước lookup")

    df = _add_date_key(df, "shown_at")
    df = df.merge(maps.user, on="user_id", how="left")
    df = df.merge(maps.ad,   on="ad_id",   how="left")
    df = df.merge(
        maps.date.rename(columns={"full_date": "_play_date"}),
        on="_play_date", how="left"
    )

    before = len(df)
    df = df.dropna(subset=["ad_key", "user_key", "date_key"])
    if before - len(df):
        log.warning(f"  [fact_ad_impressions] Loại {before - len(df):,} dòng thiếu key")

    for col in ["ad_key", "user_key", "date_key"]:
        df[col] = df[col].astype(int)

    if "hour_of_day" not in df.columns:
        df["hour_of_day"] = pd.to_datetime(df["shown_at"]).dt.hour

    cur = conn.cursor()

    INSERT_SQL = """
        INSERT INTO dw.fact_ad_impressions
            (ad_key, user_key, date_key,
             hour_of_day, is_clicked, shown_at)
        VALUES %s
    """

    cols = ["ad_key", "user_key", "date_key",
            "hour_of_day", "is_clicked", "shown_at"]

    n = _bulk_insert(cur, INSERT_SQL, df, cols, "fact_ad_impressions")
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def run_all_facts(conn, silver_events: Path, since: str) -> dict:
    """
    Tải dim maps 1 lần rồi load 4 fact tables.
    Trả về dict rows loaded.
    """
    log.info("── Load Facts ───────────────────────────────────────")
    maps  = DimMaps(conn)   # load key mapping lên RAM 1 lần duy nhất
    stats = {}

    stats["fact_plays"]          = load_fact_plays(conn, silver_events, maps, since)
    stats["fact_payments"]       = load_fact_payments(conn, silver_events, maps, since)
    stats["fact_likes"]          = load_fact_likes(conn, silver_events, maps, since)
    stats["fact_ad_impressions"] = load_fact_ad_impressions(conn, silver_events, maps, since)

    log.info(f"── Facts done: {stats}")
    return stats
