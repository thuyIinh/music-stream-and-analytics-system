"""
transforms/payment_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Event: payments.
Xử lý sự kiện giao dịch thanh toán của người dùng.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng payments.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [payments] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên Schema
    target_columns = [
        "payment_id", "user_id", "amount", "currency", "plan",
        "duration_days", "status", "paid_at", "note"
    ]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ để Validate
    df["payment_id"] = pd.to_numeric(df["payment_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["duration_days"] = pd.to_numeric(df["duration_days"], errors="coerce")

    # 3. Ràng buộc NOT NULL và Logic Tài Chính
    # Phải có ID, User, Amount (>= 0), Plan và Duration
    mask_valid = (
            df["payment_id"].notna() & (df["payment_id"] > 0) &
            df["user_id"].notna() & (df["user_id"] > 0) &
            df["amount"].notna() & (df["amount"] >= 0) &
            df["plan"].notna() & (df["plan"].astype(str).str.strip() != "") &
            df["duration_days"].notna() & (df["duration_days"] > 0)
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL hoặc Dữ liệu tài chính âm (amount/duration_days)"
        log.info(f"    [payments] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu số nguyên an toàn
    df_clean["payment_id"] = df_clean["payment_id"].astype("Int64")
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")
    df_clean["duration_days"] = df_clean["duration_days"].astype("Int64")

    # Ép kiểu Float cho số tiền (chứa được phần thập phân 10,2 theo schema)
    df_clean["amount"] = df_clean["amount"].astype(float)

    # 6. Chuẩn hóa chuỗi (String) & Giá trị mặc định
    # currency: DEFAULT 'VND', đồng thời viết hoa (vd: 'vnd' -> 'VND')
    df_clean["currency"] = df_clean["currency"].apply(
        lambda x: str(x).strip().upper() if pd.notna(x) and str(x).strip() else "VND"
    )

    # status: DEFAULT 'success', đồng thời viết thường
    df_clean["status"] = df_clean["status"].apply(
        lambda x: str(x).strip().lower() if pd.notna(x) and str(x).strip() else "success"
    )

    # plan: Viết thường
    df_clean["plan"] = df_clean["plan"].astype(str).str.strip().str.lower()

    # note: Làm sạch khoảng trắng
    df_clean["note"] = df_clean["note"].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # 7. Xử lý Ngày tháng (Timestamp)
    df_clean["paid_at"] = pd.to_datetime(df_clean["paid_at"], errors="coerce")

    # Nếu hệ thống gửi sự kiện bị thiếu timestamp, lấy thời điểm hiện tại
    df_clean["paid_at"] = df_clean["paid_at"].fillna(pd.Timestamp.now())

    # 8. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Sắp xếp để giữ lại giao dịch ghi nhận sớm nhất nếu bị trùng payment_id
    df_clean = df_clean.sort_values(by=["payment_id", "paid_at"], ascending=[True, True])
    df_clean = df_clean.drop_duplicates(subset=["payment_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [payments] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp payment_id.")

    # 9. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected