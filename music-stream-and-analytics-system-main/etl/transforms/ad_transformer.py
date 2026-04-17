"""
transforms/ad_transformer.py
Module làm sạch và chuẩn hoá bảng Catalog: ads.
Áp dụng chuẩn output mới: return df_clean, df_rejected
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng ads.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [ads] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu phục vụ báo cáo
    target_columns = [
        "ad_id", "title", "sponsor", "is_active",
        "start_date", "end_date", "created_at"
    ]

    # Bổ sung các cột bị thiếu bằng None để tránh lỗi KeyError
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ để Validate
    df["ad_id"] = pd.to_numeric(df["ad_id"], errors="coerce")

    # 3. Ràng buộc NOT NULL
    # Giả định quảng cáo phải có ID hợp lệ và có tiêu đề
    mask_valid = (
        df["ad_id"].notna() & (df["ad_id"] > 0) &
        df["title"].notna() & (df["title"].astype(str).str.strip() != "")
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu ad_id hoặc title)"
        log.info(f"    [ads] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 an toàn cho file Parquet
    df_clean["ad_id"] = df_clean["ad_id"].astype("Int64")

    # 6. Chuẩn hóa chuỗi (String)
    df_clean["title"] = df_clean["title"].astype(str).str.strip()
    df_clean["sponsor"] = df_clean["sponsor"].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # 7. Xử lý Boolean và Giá trị mặc định
    # Nếu khuyết is_active, mặc định cho là True (đang chạy)
    df_clean["is_active"] = df_clean["is_active"].fillna(True).astype(bool)

    # 8. Xử lý Ngày tháng (Date/Timestamp)
    # start_date và end_date thường dùng chuẩn Date (YYYY-MM-DD)
    df_clean["start_date"] = pd.to_datetime(df_clean["start_date"], errors="coerce").dt.date
    df_clean["end_date"] = pd.to_datetime(df_clean["end_date"], errors="coerce").dt.date
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")

    # Fill DEFAULT NOW cho created_at nếu thiếu
    df_clean["created_at"] = df_clean["created_at"].fillna(pd.Timestamp.now())

    # 9. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Sắp xếp để ưu tiên lấy bản ghi mới nhất nếu bị trùng ad_id
    df_clean = df_clean.sort_values(by=["ad_id", "created_at"], ascending=[True, False])
    df_clean = df_clean.drop_duplicates(subset=["ad_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [ads] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp ad_id.")

    # 10. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected