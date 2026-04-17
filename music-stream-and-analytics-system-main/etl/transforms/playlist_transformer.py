"""
transforms/playlist_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: playlists.
Áp dụng chuẩn output mới: return df_clean, df_rejected
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng playlists.
    Trả về Tuple gồm 2 DataFrame: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [playlists] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên DB Schema
    target_columns = [
        "playlist_id", "user_id", "name", "description", "cover_url",
        "is_public", "total_songs", "total_duration", "created_at", "updated_at"
    ]

    # Bổ sung các cột bị thiếu bằng None để tránh lỗi KeyError
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý khóa chính (PK) và khóa ngoại (FK)
    df["playlist_id"] = pd.to_numeric(df["playlist_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")

    # Ràng buộc NOT NULL (playlist_id, user_id, name)
    mask_valid = (
            df["playlist_id"].notna() & (df["playlist_id"] > 0) &
            df["user_id"].notna() & (df["user_id"] > 0) &
            df["name"].notna() & (df["name"].astype(str).str.strip() != "")
    )

    # 3. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu ID, user_id hoặc name)"
        log.info(f"    [playlists] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 4. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    # Nếu sau khi lọc không còn dòng nào hợp lệ thì trả về luôn
    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 cho ID (Hỗ trợ giá trị Null và an toàn khi lưu Parquet)
    df_clean["playlist_id"] = df_clean["playlist_id"].astype("Int64")
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")

    # 5. Xử lý kiểu chuỗi (String)
    df_clean["name"] = df_clean["name"].astype(str).str.strip()
    df_clean["description"] = df_clean["description"].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    df_clean["cover_url"] = df_clean["cover_url"].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # 6. Xử lý Ngày tháng (Timestamp)
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")
    df_clean["updated_at"] = pd.to_datetime(df_clean["updated_at"], errors="coerce")

    # 7. Xử lý Giá trị mặc định (Default values) theo Schema
    # is_public: DEFAULT false
    df_clean["is_public"] = df_clean["is_public"].fillna(False).astype(bool)

    # total_songs & total_duration: DEFAULT 0
    df_clean["total_songs"] = pd.to_numeric(df_clean["total_songs"], errors="coerce").fillna(0).astype(int)
    df_clean["total_duration"] = pd.to_numeric(df_clean["total_duration"], errors="coerce").fillna(0).astype(int)

    # 8. Loại bỏ bản ghi trùng lặp (Deduplicate)
    # Ưu tiên giữ lại bản ghi có 'updated_at' mới nhất nếu bị trùng playlist_id
    if "updated_at" in df_clean.columns:
        df_clean = df_clean.sort_values(by=["playlist_id", "updated_at"], ascending=[True, False])

    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["playlist_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [playlists] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp playlist_id.")

    # 9. Trả về đúng danh sách cột mục tiêu cho file sạch
    df_clean = df_clean[target_columns]

    # Trả về cả 2 DataFrame theo hợp đồng mới
    return df_clean, df_rejected