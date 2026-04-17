"""
transforms/song_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: songs.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng songs.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [songs] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên DB Schema
    target_columns = [
        "song_id", "title", "artist_id", "album_id", "genre_id", "duration",
        "file_url", "cover_url", "lyrics", "play_count", "like_count",
        "mood", "language", "is_active", "release_date", "created_at", "updated_at"
    ]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ cho các trường quan trọng để Validate
    df["song_id"] = pd.to_numeric(df["song_id"], errors="coerce")
    df["artist_id"] = pd.to_numeric(df["artist_id"], errors="coerce")
    df["duration"] = pd.to_numeric(df["duration"], errors="coerce")

    # 3. Ràng buộc NOT NULL theo Schema
    # Các trường bắt buộc: song_id, title, artist_id, duration, file_url
    mask_valid = (
            df["song_id"].notna() & (df["song_id"] > 0) &
            df["artist_id"].notna() & (df["artist_id"] > 0) &
            df["title"].notna() & (df["title"].astype(str).str.strip() != "") &
            df["file_url"].notna() & (df["file_url"].astype(str).str.strip() != "") &
            df["duration"].notna() & (df["duration"] >= 0)
    )

    # TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu song_id, artist_id, title, file_url hoặc duration < 0)"
        log.info(f"    [songs] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # 4. Ép kiểu Int64 cho toàn bộ các trường ID (Hỗ trợ Null cho album_id, genre_id)
    df_clean["song_id"] = df_clean["song_id"].astype("Int64")
    df_clean["artist_id"] = df_clean["artist_id"].astype("Int64")
    df_clean["album_id"] = pd.to_numeric(df_clean["album_id"], errors="coerce").astype("Int64")
    df_clean["genre_id"] = pd.to_numeric(df_clean["genre_id"], errors="coerce").astype("Int64")

    # 5. Xử lý kiểu chuỗi (String) thông thường
    df_clean["title"] = df_clean["title"].astype(str).str.strip()
    df_clean["file_url"] = df_clean["file_url"].astype(str).str.strip()
    df_clean["cover_url"] = df_clean["cover_url"].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    df_clean["lyrics"] = df_clean["lyrics"].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # 6. Chuẩn hóa chuỗi đặc biệt (Mood, Language)
    df_clean["mood"] = df_clean["mood"].apply(
        lambda x: str(x).strip().title() if pd.notna(x) and str(x).strip() else None
    )
    # language DEFAULT 'en', đồng thời viết thường (vd: 'EN', ' En ' -> 'en')
    df_clean["language"] = df_clean["language"].apply(
        lambda x: str(x).strip().lower() if pd.notna(x) and str(x).strip() else "en"
    )

    # 7. Xử lý Ngày tháng (Date/Timestamp)
    df_clean["release_date"] = pd.to_datetime(df_clean["release_date"], errors="coerce").dt.date
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")
    df_clean["updated_at"] = pd.to_datetime(df_clean["updated_at"], errors="coerce")

    # 8. Xử lý Giá trị mặc định (Default values) theo Schema
    # is_active: DEFAULT true
    df_clean["is_active"] = df_clean["is_active"].fillna(True).astype(bool)

    # play_count & like_count: DEFAULT 0
    df_clean["play_count"] = pd.to_numeric(df_clean["play_count"], errors="coerce").fillna(0).astype(int)
    df_clean["like_count"] = pd.to_numeric(df_clean["like_count"], errors="coerce").fillna(0).astype(int)

    # duration: Ép lại về int sau khi đã validate
    df_clean["duration"] = df_clean["duration"].astype(int)

    # 9. Loại bỏ bản ghi trùng lặp (Deduplicate)
    if "updated_at" in df_clean.columns:
        df_clean = df_clean.sort_values(by=["song_id", "updated_at"], ascending=[True, False])

    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["song_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [songs] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp song_id.")

    # 10. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected