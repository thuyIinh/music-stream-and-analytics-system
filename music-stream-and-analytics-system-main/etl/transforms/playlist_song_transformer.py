"""
transforms/playlist_song_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: playlist_songs.
Bảng trung gian thể hiện quan hệ N-N giữa playlists và songs.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng playlist_songs.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [playlist_songs] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên DB Schema
    target_columns = ["id", "playlist_id", "song_id", "position", "added_at"]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý kiểu dữ liệu số (ID và Position)
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df["playlist_id"] = pd.to_numeric(df["playlist_id"], errors="coerce")
    df["song_id"] = pd.to_numeric(df["song_id"], errors="coerce")
    df["position"] = pd.to_numeric(df["position"], errors="coerce")

    # Ràng buộc NOT NULL cho các Khóa ngoại và Vị trí
    mask_valid = (
            df["playlist_id"].notna() & (df["playlist_id"] > 0) &
            df["song_id"].notna() & (df["song_id"] > 0) &
            df["position"].notna()
    )

    # 3. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu playlist_id, song_id hoặc position)"
        log.info(f"    [playlist_songs] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 4. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 cho toàn bộ các trường số nguyên
    df_clean["id"] = df_clean["id"].astype("Int64")
    df_clean["playlist_id"] = df_clean["playlist_id"].astype("Int64")
    df_clean["song_id"] = df_clean["song_id"].astype("Int64")
    df_clean["position"] = df_clean["position"].astype("Int64")

    # 5. Xử lý Ngày tháng và Giá trị mặc định (DEFAULT NOW)
    df_clean["added_at"] = pd.to_datetime(df_clean["added_at"], errors="coerce")
    # Nếu bị Null, tự động điền thời gian hiện tại lúc chạy pipeline
    df_clean["added_at"] = df_clean["added_at"].fillna(pd.Timestamp.now())

    # 6. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Bước 6a: Xóa trùng lặp theo Khóa chính (id) nếu có
    if df_clean["id"].notna().any():
        df_clean = df_clean.drop_duplicates(subset=["id"], keep="first")

    # Bước 6b: Xử lý ràng buộc UNIQUE (playlist_id, song_id)
    # Nếu một bài hát bị add 2 lần vào cùng 1 playlist, ta ưu tiên giữ lại lần add đầu tiên (added_at nhỏ nhất)
    df_clean = df_clean.sort_values(by=["playlist_id", "song_id", "added_at"], ascending=[True, True, True])
    df_clean = df_clean.drop_duplicates(subset=["playlist_id", "song_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(
            f"    [playlist_songs] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp (vi phạm PK hoặc UNIQUE composite key).")

    # 7. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected