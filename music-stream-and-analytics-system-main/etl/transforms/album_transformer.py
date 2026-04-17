"""
transforms/album_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: albums.
Áp dụng chuẩn output mới: return df_clean, df_rejected
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng albums.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [albums] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên DB Schema
    target_columns = [
        "album_id", "title", "artist_id", "cover_url", "release_date",
        "total_tracks", "description", "is_active", "created_at", "updated_at"
    ]

    # Bổ sung các cột bị thiếu bằng None để tránh lỗi KeyError
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý các cột ID (Primary Key, Foreign Key) sơ bộ để Validate
    df["album_id"] = pd.to_numeric(df["album_id"], errors="coerce")
    df["artist_id"] = pd.to_numeric(df["artist_id"], errors="coerce")

    # Lọc bỏ các dòng có ID không hợp lệ hoặc bị Null (NOT NULL constraints)
    mask_valid = (
        df["album_id"].notna() & (df["album_id"] > 0) &
        df["artist_id"].notna() & (df["artist_id"] > 0) &
        df["title"].notna() & (df["title"].astype(str).str.strip() != "")
    )

    # 3. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu album_id, artist_id hoặc title)"
        log.info(f"    [albums] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 4. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép về kiểu Int64 (Hỗ trợ Null trong quá trình xử lý của Pandas, sau này ghi ra Parquet sẽ mượt)
    df_clean["album_id"] = df_clean["album_id"].astype("Int64")
    df_clean["artist_id"] = df_clean["artist_id"].astype("Int64")

    # 5. Xử lý kiểu chuỗi (String)
    df_clean["title"] = df_clean["title"].astype(str).str.strip()
    df_clean["cover_url"] = df_clean["cover_url"].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    df_clean["description"] = df_clean["description"].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # 6. Xử lý Ngày tháng (Date/Timestamp)
    df_clean["release_date"] = pd.to_datetime(df_clean["release_date"], errors="coerce").dt.date
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")
    df_clean["updated_at"] = pd.to_datetime(df_clean["updated_at"], errors="coerce")

    # 7. Xử lý Giá trị mặc định (Default values) cho Numeric và Boolean
    # total_tracks: DEFAULT 0
    df_clean["total_tracks"] = pd.to_numeric(df_clean["total_tracks"], errors="coerce").fillna(0).astype(int)

    # is_active: DEFAULT true
    df_clean["is_active"] = df_clean["is_active"].fillna(True).astype(bool)

    # 8. Loại bỏ bản ghi trùng lặp (Deduplicate)
    # Ưu tiên giữ lại bản ghi có 'updated_at' mới nhất nếu bị trùng album_id
    if "updated_at" in df_clean.columns:
        df_clean = df_clean.sort_values(by=["album_id", "updated_at"], ascending=[True, False])

    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["album_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [albums] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp album_id.")

    # 9. Giữ lại đúng danh sách cột mục tiêu để đưa vào Silver
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected