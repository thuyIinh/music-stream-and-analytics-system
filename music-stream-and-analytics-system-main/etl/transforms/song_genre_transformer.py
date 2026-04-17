"""
transforms/song_genre_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: song_genres.
Bảng nối thuần túy thể hiện quan hệ N-N giữa bài hát (songs) và thể loại (genres).
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng song_genres.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [song_genres] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu
    target_columns = ["song_id", "genre_id"]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ
    df["song_id"] = pd.to_numeric(df["song_id"], errors="coerce")
    df["genre_id"] = pd.to_numeric(df["genre_id"], errors="coerce")

    # 3. Ràng buộc NOT NULL
    # Cả 2 cột đều là Foreign Key và cấu thành nên Primary Key nên bắt buộc phải có giá trị > 0
    mask_valid = (
            df["song_id"].notna() & (df["song_id"] > 0) &
            df["genre_id"].notna() & (df["genre_id"] > 0)
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu song_id hoặc genre_id)"
        log.info(f"    [song_genres] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 an toàn cho file Parquet
    df_clean["song_id"] = df_clean["song_id"].astype("Int64")
    df_clean["genre_id"] = df_clean["genre_id"].astype("Int64")

    # 6. Loại bỏ bản ghi trùng lặp (Deduplicate)
    # Khóa chính là tổ hợp (song_id, genre_id) nên ta sẽ drop duplicate trên cả 2 cột này
    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["song_id", "genre_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [song_genres] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp cặp (song_id, genre_id).")

    # 7. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected