"""
loaders/dim_loader.py
=====================
Load tất cả Dimension tables từ Silver catalog vào dw schema.

Chiến lược:
  - dim_genre, dim_artist, dim_ad : INSERT ID chưa tồn tại (INSERT ... ON CONFLICT DO NOTHING)
  - dim_song                      : INSERT bài mới, JOIN pandas để denormalize
  - dim_user                      : SCD Type 2 — theo dõi thay đổi plan/role/country/gender
  - dim_date                      : KHÔNG load — đã seed sẵn bằng SQL
"""

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

log = logging.getLogger("dim_loader")

SCD2_FIELDS = ("plan", "role", "country", "gender")


# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def _read_parquet(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        log.warning(f"  [{name}] File không tồn tại: {path}")
        return pd.DataFrame()
    df = pd.read_parquet(path)
    log.info(f"  [{name}] Đọc {len(df):,} rows từ {path.name}")
    return df


def _bulk_insert(cur, sql: str, rows: list, tag: str) -> int:
    if not rows:
        return 0
    psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    n = len(rows)
    log.info(f"  [{tag}] Inserted {n:,} rows")
    return n


# ════════════════════════════════════════════════════════════════════════════
# 1. DIM_GENRE
# ════════════════════════════════════════════════════════════════════════════

def load_dim_genre(conn, silver_catalog: Path) -> int:
    """
    Insert genre chưa tồn tại trong DW.
    Source: silver/catalog/genres.parquet
    Natural key: genre_id
    """
    df = _read_parquet(silver_catalog / "genres" /"genres.parquet", "dim_genre")
    if df.empty:
        return 0

    cur = conn.cursor()

    # Lấy genre_id đã có trong DW
    cur.execute("SELECT genre_id FROM dw.dim_genre")
    existing = {r[0] for r in cur.fetchall()}

    rows = [
        (int(r.genre_id), str(r.name))
        for r in df.itertuples()
        if int(r.genre_id) not in existing
    ]

    n = _bulk_insert(
        cur,
        "INSERT INTO dw.dim_genre (genre_id, name) VALUES %s ",
        rows, "dim_genre"
    )
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# 2. DIM_ARTIST
# ════════════════════════════════════════════════════════════════════════════

def load_dim_artist(conn, silver_catalog: Path) -> int:
    """
    Insert artist chưa tồn tại trong DW.
    Source: silver/catalog/artists.parquet
    Natural key: artist_id
    """
    df = _read_parquet(silver_catalog / "artists" /"artists.parquet", "dim_artist")
    if df.empty:
        return 0

    cur = conn.cursor()

    cur.execute("SELECT artist_id FROM dw.dim_artist")
    existing = {r[0] for r in cur.fetchall()}

    rows = [
        (int(r.artist_id),
         str(r.name),
         str(r.country) if pd.notna(r.country) else None,
         bool(r.is_active) if pd.notna(r.is_active) else True)
        for r in df.itertuples()
        if int(r.artist_id) not in existing
    ]

    n = _bulk_insert(
        cur,
        """
        INSERT INTO dw.dim_artist (artist_id, name, country, is_active)
        VALUES %s 
        """,
        rows, "dim_artist"
    )
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# 3. DIM_AD
# ════════════════════════════════════════════════════════════════════════════

def load_dim_ad(conn, silver_catalog: Path) -> int:
    """
    Insert ad chưa tồn tại trong DW.
    Source: silver/catalog/ads.parquet
    Natural key: ad_id
    """
    df = _read_parquet(silver_catalog / "ads" /"ads.parquet", "dim_ad")
    if df.empty:
        return 0

    cur = conn.cursor()

    cur.execute("SELECT ad_id FROM dw.dim_ad")
    existing = {r[0] for r in cur.fetchall()}

    rows = [
        (int(r.ad_id),
         str(r.title),
         str(r.sponsor) if pd.notna(r.sponsor) else None)
        for r in df.itertuples()
        if int(r.ad_id) not in existing
    ]

    n = _bulk_insert(
        cur,
        """
        INSERT INTO dw.dim_ad (ad_id, title, sponsor)
        VALUES %s 
        """,
        rows, "dim_ad"
    )
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# 4. DIM_SONG  (denormalize bằng pandas JOIN trên RAM)
# ════════════════════════════════════════════════════════════════════════════

def load_dim_song(conn, silver_catalog: Path) -> int:
    """
    Insert bài hát mới vào DW.
    Source: silver/catalog/songs.parquet
    JOIN với artists + genres trên RAM để denormalize artist_name, genre_name.
    Natural key: song_id
    """
    df_songs   = _read_parquet(silver_catalog /"songs" / "songs.parquet",   "dim_song/songs")
    df_artists = _read_parquet(silver_catalog / "artists" /"artists.parquet", "dim_song/artists")
    df_albums  = _read_parquet(silver_catalog / "albums" /"albums.parquet",  "dim_song/albums")
    df_genres  = _read_parquet(silver_catalog / "genres" /"genres.parquet",  "dim_song/genres")

    if df_songs.empty:
        return 0

    # JOIN trên RAM — denormalize
    df = df_songs.copy()

    if not df_artists.empty:
        df = df.merge(
            df_artists[["artist_id", "name"]].rename(columns={"name": "artist_name"}),
            on="artist_id", how="left"
        )
    else:
        df["artist_name"] = None

    if not df_albums.empty:
        df = df.merge(
            df_albums[["album_id", "title"]].rename(columns={"title": "album_title"}),
            on="album_id", how="left"
        )
    else:
        df["album_title"] = None

    if not df_genres.empty:
        df = df.merge(
            df_genres[["genre_id", "name"]].rename(columns={"name": "genre_name"}),
            on="genre_id", how="left"
        )
    else:
        df["genre_name"] = None

    cur = conn.cursor()

    # Lấy song_id đã có trong DW
    cur.execute("SELECT song_id FROM dw.dim_song")
    existing = {r[0] for r in cur.fetchall()}

    new_songs = df[~df["song_id"].isin(existing)]
    log.info(f"  [dim_song] Bài mới cần insert: {len(new_songs):,}")

    rows = []
    for r in new_songs.itertuples():
        rows.append((
            int(r.song_id),
            str(r.title),
            str(r.artist_name)  if pd.notna(r.artist_name)  else None,
            str(r.album_title)  if pd.notna(r.album_title)  else None,
            str(r.genre_name)   if pd.notna(r.genre_name)   else None,
            int(r.duration)     if pd.notna(r.duration)     else None,
            str(r.mood)         if pd.notna(r.mood)         else None,
            str(r.language)     if pd.notna(r.language)     else None,
            r.release_date      if pd.notna(r.release_date) else None,
            bool(r.is_active)   if pd.notna(r.is_active)    else True,
        ))

    n = _bulk_insert(
        cur,
        """
        INSERT INTO dw.dim_song
            (song_id, title, artist_name, album_title, genre_name,
             duration, mood, language, release_date, is_active)
        VALUES %s 
        """,
        rows, "dim_song"
    )
    conn.commit()
    cur.close()
    return n


# ════════════════════════════════════════════════════════════════════════════
# 5. DIM_USER  (SCD Type 2)
# ════════════════════════════════════════════════════════════════════════════

def load_dim_user(conn, silver_catalog: Path) -> int:
    """
    SCD Type 2:
      · User mới → INSERT is_current=true
      · User thay đổi SCD2_FIELDS → expire bản cũ + INSERT bản mới
    Source: JOIN users.parquet và user_profiles.parquet trên RAM
    Natural key: user_id
    """
    # 1. Đọc cả 2 file từ tầng Silver
    df_users = _read_parquet(silver_catalog / "users" / "users.parquet", "dim_user/users")
    df_profiles = _read_parquet(silver_catalog / "user_profiles" / "user_profiles.parquet", "dim_user/profiles")

    if df_users.empty:
        return 0

    # 2. JOIN trên RAM bằng Pandas (để đắp country, gender vào users)
    if not df_profiles.empty:
        df = df_users.merge(df_profiles, on="user_id", how="left")
    else:
        df = df_users.copy()
        df["country"] = None
        df["gender"] = None

        # ─── BẮT ĐẦU ĐOẠN SỬA LỖI ───
        # Xử lý trường hợp đụng độ tên cột sinh ra _x, _y
    if "created_at_x" in df.columns:
        df = df.rename(columns={"created_at_x": "registration_date"})
    elif "created_at" in df.columns:
        df = df.rename(columns={"created_at": "registration_date"})

        # Phòng thủ lớp 2: Nếu tìm mãi vẫn không thấy cột nào, thì tạo một cột rỗng (None)
        # để vòng lặp itertuples() bên dưới không bị văng lỗi.
    if "registration_date" not in df.columns:
        df["registration_date"] = None
        # ─── KẾT THÚC ĐOẠN SỬA LỖI ───

    today = date.today()
    yesterday = today - timedelta(days=1)
    cur = conn.cursor()

    # Lấy bản ghi is_current=true hiện có trong DW
    cur.execute(
        """
        SELECT user_id, plan, role, country, gender, user_key
        FROM   dw.dim_user
        WHERE  is_current = true
        """
    )
    existing = {
        r[0]: {"plan": r[1], "role": r[2], "country": r[3],
               "gender": r[4], "user_key": r[5]}
        for r in cur.fetchall()
    }
    log.info(f"  [dim_user] DW hiện có {len(existing):,} bản ghi is_current=true")

    new_rows     = []
    scd2_updates = []

    for r in df.itertuples():
        uid = int(r.user_id)

        src = {
            "plan":    str(r.plan)    if pd.notna(r.plan)    else None,
            "role":    str(r.role)    if pd.notna(r.role)    else None,
            "country": str(r.country) if pd.notna(r.country) else None,
            "gender":  str(r.gender)  if pd.notna(r.gender)  else None,
        }

        reg_date = r.registration_date if pd.notna(r.registration_date) else None

        if uid not in existing:
            # User hoàn toàn mới
            new_rows.append((
                uid,
                str(r.email)        if pd.notna(r.email)        else None,
                str(r.display_name) if pd.notna(r.display_name) else None,
                src["role"], src["plan"], src["country"], src["gender"],
                reg_date,
                today,   # effective_date
                None,    # expiry_date
                True,    # is_current
            ))
        else:
            ex = existing[uid]
            changed = any(src.get(f) != ex.get(f) for f in SCD2_FIELDS)
            if changed:
                scd2_updates.append((uid, ex["user_key"], r, src, reg_date))

    # INSERT user mới
    if new_rows:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.dim_user
                (user_id, email, display_name, role, plan, country, gender,
                 registration_date, effective_date, expiry_date, is_current)
            VALUES %s
            """,  # Đã xóa bỏ ON CONFLICT DO NOTHING vì code logic trên RAM đã chặn trùng lặp
            new_rows, page_size=500
        )
        log.info(f"  [dim_user] Đã insert {len(new_rows):,} user mới")

    # SCD2: expire bản cũ → INSERT bản mới
    # for uid, old_key, r, src, reg_date in scd2_updates:
    #     # Expire bản cũ
    #     cur.execute(
    #         """
    #         UPDATE dw.dim_user
    #         SET    is_current  = false,
    #                expiry_date = %s
    #         WHERE  user_key    = %s
    #         """,
    #         (yesterday, old_key)
    #     )
    #     # Insert bản mới
    #     cur.execute(
    #         """
    #         INSERT INTO dw.dim_user
    #             (user_id, email, display_name, role, plan, country, gender,
    #              registration_date, effective_date, expiry_date, is_current)
    #         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,true)
    #         """,
    #         (uid,
    #          str(r.email)        if pd.notna(r.email)        else None,
    #          str(r.display_name) if pd.notna(r.display_name) else None,
    #          src["role"], src["plan"], src["country"], src["gender"],
    #          reg_date, today)
    #     )

    if scd2_updates:
        # Tách dữ liệu ra thành danh sách các khóa cũ và các dòng dữ liệu mới
        keys_to_expire = tuple([old_key for _, old_key, _, _, _ in scd2_updates])
        new_scd2_rows = []

        for uid, _, r, src, reg_date in scd2_updates:
            new_scd2_rows.append((
                uid,
                str(r.email) if pd.notna(r.email) else None,
                str(r.display_name) if pd.notna(r.display_name) else None,
                src["role"], src["plan"], src["country"], src["gender"],
                reg_date, today, None, True
            ))

        # Bước A: Bulk Update - Đóng băng TẤT CẢ các gói cước cũ cùng 1 lúc (Chỉ gọi DB 1 lần)
        cur.execute(
            """
            UPDATE dw.dim_user
            SET    is_current  = false,
                   expiry_date = %s
            WHERE  user_key IN %s
            """,
            (yesterday, keys_to_expire)
        )

        # Bước B: Bulk Insert - Thêm TẤT CẢ các dòng gói cước mới cùng 1 lúc
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO dw.dim_user
                (user_id, email, display_name, role, plan, country, gender,
                 registration_date, effective_date, expiry_date, is_current)
            VALUES %s
            """,
            new_scd2_rows, page_size=500
        )

        log.info(f"  [dim_user] SCD2: Đã cập nhật & sinh mới {len(scd2_updates):,} users")

    conn.commit()
    cur.close()
    return len(new_rows) + len(scd2_updates)


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def run_all_dims(conn, silver_catalog: Path) -> dict:
    """Chạy toàn bộ dim theo đúng thứ tự. Trả về dict rows loaded."""
    log.info("── Load Dimensions ──────────────────────────────────")
    stats = {}
    # Thứ tự quan trọng: genre/artist/ad trước, song sau
    stats["dim_genre"]  = load_dim_genre(conn, silver_catalog)
    stats["dim_artist"] = load_dim_artist(conn, silver_catalog)
    stats["dim_ad"]     = load_dim_ad(conn, silver_catalog)
    stats["dim_song"]   = load_dim_song(conn, silver_catalog)   # cần genre+artist đã load
    stats["dim_user"]   = load_dim_user(conn, silver_catalog)
    log.info(f"── Dims done: {stats}")
    return stats
