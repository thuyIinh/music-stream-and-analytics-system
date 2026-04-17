"""
transforms/subscription_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: subscriptions.
Quản lý lịch sử đăng ký gói cước của người dùng.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng subscriptions.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [subscriptions] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên DB Schema
    target_columns = [
        "subscription_id", "user_id", "plan", "start_date",
        "end_date", "status", "created_at"
    ]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ cho các trường quan trọng để Validate
    df["subscription_id"] = pd.to_numeric(df["subscription_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")

    # 3. Ràng buộc NOT NULL theo Schema
    # Các trường bắt buộc: ID gói, user_id, tên gói (plan), ngày bắt đầu
    mask_valid = (
            df["subscription_id"].notna() & (df["subscription_id"] > 0) &
            df["user_id"].notna() & (df["user_id"] > 0) &
            df["plan"].notna() & (df["plan"].astype(str).str.strip() != "") &
            df["start_date"].notna()
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu ID, user_id, plan hoặc start_date)"
        log.info(f"    [subscriptions] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 cho khóa chính và khóa ngoại
    df_clean["subscription_id"] = df_clean["subscription_id"].astype("Int64")
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")

    # 6. Chuẩn hóa chuỗi (String) cho Plan và Status
    # Đưa tên gói (VD: 'Premium', ' VIP ') về chữ thường, cắt khoảng trắng -> 'premium', 'vip'
    df_clean["plan"] = df_clean["plan"].astype(str).str.strip().str.lower()

    # Status có DEFAULT 'active'. Nếu Null -> 'active'. Đồng thời đưa về chữ thường.
    df_clean["status"] = df_clean["status"].apply(
        lambda x: str(x).strip().lower() if pd.notna(x) and str(x).strip() else "active"
    )

    # 7. Xử lý Ngày tháng (Timestamp) và Giá trị mặc định
    df_clean["end_date"] = pd.to_datetime(df_clean["end_date"], errors="coerce")
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")

    # Nếu created_at bị thiếu, mô phỏng DEFAULT NOW() của database
    df_clean["created_at"] = df_clean["created_at"].fillna(pd.Timestamp.now())

    # 8. Loại bỏ bản ghi trùng lặp (Deduplicate)
    # Nếu cùng 1 subscription_id, ưu tiên giữ bản ghi có thời gian tạo mới nhất
    df_clean = df_clean.sort_values(by=["subscription_id", "created_at"], ascending=[True, False])

    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["subscription_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [subscriptions] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp subscription_id.")

    # 9. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected