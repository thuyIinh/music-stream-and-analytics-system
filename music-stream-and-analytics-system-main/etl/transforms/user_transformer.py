"""
transforms/user_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: users.
Lưu ý: Đã loại bỏ cột 'password_hash' để đảm bảo bảo mật dữ liệu PII tại Data Lake.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng users.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [users] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu (Loại bỏ password_hash vì lý do bảo mật)
    target_columns = [
        "user_id", "email", "display_name", "role", "plan",
        "is_active", "created_at", "updated_at", "last_login_at"
    ]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ để Validate
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")

    # 3. Ràng buộc NOT NULL theo Schema
    # Bắt buộc phải có user_id, email, và display_name
    mask_valid = (
            df["user_id"].notna() & (df["user_id"] > 0) &
            df["email"].notna() & (df["email"].astype(str).str.strip() != "") &
            df["display_name"].notna() & (df["display_name"].astype(str).str.strip() != "")
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu user_id, email hoặc display_name)"
        log.info(f"    [users] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu an toàn cho ID
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")

    # 6. Chuẩn hóa chuỗi (String)
    # Tên hiển thị: cắt khoảng trắng thừa
    df_clean["display_name"] = df_clean["display_name"].astype(str).str.strip()

    # Email: Cắt khoảng trắng và đưa về chữ thường (Quan trọng để nhận diện user duy nhất)
    df_clean["email"] = df_clean["email"].astype(str).str.strip().str.lower()

    # Role & Plan: Chuẩn hóa chữ thường và xử lý DEFAULT
    df_clean["role"] = df_clean["role"].apply(
        lambda x: str(x).strip().lower() if pd.notna(x) and str(x).strip() else "user"
    )
    df_clean["plan"] = df_clean["plan"].apply(
        lambda x: str(x).strip().lower() if pd.notna(x) and str(x).strip() else "free"
    )

    # 7. Xử lý Boolean DEFAULT true
    df_clean["is_active"] = df_clean["is_active"].fillna(True).astype(bool)

    # 8. Xử lý Ngày tháng (Timestamp)
    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")
    df_clean["updated_at"] = pd.to_datetime(df_clean["updated_at"], errors="coerce")
    df_clean["last_login_at"] = pd.to_datetime(df_clean["last_login_at"], errors="coerce")

    # Nếu created_at bị thiếu, mô phỏng DEFAULT NOW() của database
    df_clean["created_at"] = df_clean["created_at"].fillna(pd.Timestamp.now())

    # 9. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Ưu tiên lấy dòng có updated_at mới nhất
    if "updated_at" in df_clean.columns:
        df_clean = df_clean.sort_values(by=["user_id", "updated_at"], ascending=[True, False])

    # Bước 9a: UNIQUE theo user_id (Khóa chính)
    df_clean = df_clean.drop_duplicates(subset=["user_id"], keep="first")

    # Bước 9b: UNIQUE theo email (Vì Schema quy định email VARCHAR UNIQUE)
    df_clean = df_clean.drop_duplicates(subset=["email"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [users] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp (theo ID hoặc Email).")

    # 10. Trả về đúng danh sách cột mục tiêu (Silver layer)
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected