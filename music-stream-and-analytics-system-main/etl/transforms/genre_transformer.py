"""
transforms/genre_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: genres.
Áp dụng chuẩn output mới: return df_clean, df_rejected
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng genres.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [genres] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()
    if "id" in df.columns and "genre_id" not in df.columns:
        df = df.rename(columns={"id": "genre_id"})
    # 1. Khai báo danh sách các cột mục tiêu
    target_columns = ["genre_id", "name", "description", "created_at"]

    # Bổ sung các cột bị thiếu bằng None để tránh lỗi KeyError
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý khóa chính sơ bộ để Validate
    df["genre_id"] = pd.to_numeric(df["genre_id"], errors="coerce")

    # 3. Ràng buộc NOT NULL
    # Lọc bỏ dòng lỗi: Không có ID hợp lệ hoặc thiếu tên thể loại
    mask_valid = (
        df["genre_id"].notna() & (df["genre_id"] > 0) &
        df["name"].notna() & (df["name"].astype(str).str.strip() != "")
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu genre_id hoặc name)"
        log.info(f"    [genres] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 cho khóa chính
    df_clean["genre_id"] = df_clean["genre_id"].astype("Int64")

    # 6. Chuẩn hóa chuỗi (String)
    # Tên thể loại cần được chuẩn hóa (vd: " pop ", "POP" -> "Pop") để đảm bảo tính Unique
    df_clean["name"] = df_clean["name"].astype(str).str.strip().str.title()
    df_clean["description"] = df_clean["description"].apply(
        lambda x: str(x).strip() if pd.notna(x) else None
    )

    # 7. Xử lý Ngày tháng (Timestamp)
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")

    # Fill DEFAULT NOW nếu cần (tùy chọn theo schema)
    df_clean["created_at"] = df_clean["created_at"].fillna(pd.Timestamp.now())

    # 8. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Ưu tiên 1: Loại bỏ trùng lặp theo Khóa chính (genre_id)
    df_clean = df_clean.drop_duplicates(subset=["genre_id"], keep="first")

    # Ưu tiên 2: Loại bỏ trùng lặp theo Tên (vì DB schema yêu cầu UNIQUE)
    df_clean = df_clean.drop_duplicates(subset=["name"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [genres] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp (theo ID hoặc Tên).")

    # 9. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected