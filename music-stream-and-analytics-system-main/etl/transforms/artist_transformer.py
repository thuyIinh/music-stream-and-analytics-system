"""
transforms/artist_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: artists.
Áp dụng chuẩn output mới: return df_clean, df_rejected
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng artists.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [artists] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên DB Schema
    target_columns = [
        "artist_id", "name", "bio", "image_url", "country",
        "is_active", "created_at", "updated_at"
    ]

    # Bổ sung các cột bị thiếu bằng None để tránh lỗi KeyError
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý khóa chính (Primary Key) sơ bộ để Validate
    df["artist_id"] = pd.to_numeric(df["artist_id"], errors="coerce")

    # Lọc bỏ các dòng có ID không hợp lệ hoặc thiếu tên nghệ sĩ (NOT NULL)
    mask_valid = (
        df["artist_id"].notna() & (df["artist_id"] > 0) &
        df["name"].notna() & (df["name"].astype(str).str.strip() != "")
    )

    # 3. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu artist_id hoặc name)"
        log.info(f"    [artists] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 4. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 cho ID (Hỗ trợ giá trị Null và an toàn khi lưu Parquet)
    df_clean["artist_id"] = df_clean["artist_id"].astype("Int64")

    # 5. Xử lý kiểu chuỗi (String)
    df_clean["name"] = df_clean["name"].astype(str).str.strip()
    df_clean["bio"] = df_clean["bio"].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    df_clean["image_url"] = df_clean["image_url"].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # Chuẩn hóa country: Viết hoa chữ cái đầu (VD: 'vietnam' -> 'Vietnam') nếu có dữ liệu
    df_clean["country"] = df_clean["country"].apply(
        lambda x: str(x).strip().title() if pd.notna(x) and str(x).strip() else None
    )

    # 6. Xử lý Ngày tháng (Timestamp)
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")
    df_clean["updated_at"] = pd.to_datetime(df_clean["updated_at"], errors="coerce")

    # 7. Xử lý Giá trị mặc định (Default values)
    # is_active: DEFAULT true. Nếu bị Null thì tự động gán là True
    df_clean["is_active"] = df_clean["is_active"].fillna(True).astype(bool)

    # 8. Loại bỏ bản ghi trùng lặp (Deduplicate)
    # Ưu tiên giữ lại bản ghi có 'updated_at' mới nhất nếu bị trùng artist_id
    if "updated_at" in df_clean.columns:
        df_clean = df_clean.sort_values(by=["artist_id", "updated_at"], ascending=[True, False])

    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["artist_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [artists] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp artist_id.")

    # 9. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected