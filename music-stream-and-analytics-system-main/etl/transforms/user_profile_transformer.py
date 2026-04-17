"""
transforms/user_profile_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Catalog: user_profiles.
Quản lý thông tin cá nhân mở rộng (mối quan hệ 1-1 với users).
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng user_profiles.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [user_profiles] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu
    target_columns = [
        "profile_id", "user_id", "avatar_url", "bio", "country",
        "date_of_birth", "gender", "created_at", "updated_at"
    ]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ để Validate
    df["profile_id"] = pd.to_numeric(df["profile_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")

    # 3. Ràng buộc NOT NULL
    # Cả profile_id (Khóa chính) và user_id (Khóa ngoại) đều bắt buộc
    mask_valid = (
            df["profile_id"].notna() & (df["profile_id"] > 0) &
            df["user_id"].notna() & (df["user_id"] > 0)
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu profile_id hoặc user_id)"
        log.info(f"    [user_profiles] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 an toàn
    df_clean["profile_id"] = df_clean["profile_id"].astype("Int64")
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")

    # 6. Chuẩn hóa chuỗi (String)
    df_clean["avatar_url"] = df_clean["avatar_url"].apply(lambda x: str(x).strip() if pd.notna(x) else None)
    df_clean["bio"] = df_clean["bio"].apply(lambda x: str(x).strip() if pd.notna(x) else None)

    # Country & Gender: Viết hoa chữ cái đầu (VD: 'male' -> 'Male', 'vietnam' -> 'Vietnam')
    df_clean["country"] = df_clean["country"].apply(
        lambda x: str(x).strip().title() if pd.notna(x) and str(x).strip() else None
    )
    df_clean["gender"] = df_clean["gender"].apply(
        lambda x: str(x).strip().title() if pd.notna(x) and str(x).strip() else None
    )

    # 7. Xử lý Ngày tháng (Date/Timestamp)
    # date_of_birth chỉ lấy phần Date (YYYY-MM-DD), bỏ qua giờ phút
    df_clean["date_of_birth"] = pd.to_datetime(df_clean["date_of_birth"], errors="coerce").dt.date

    df_clean["created_at"] = pd.to_datetime(df_clean["created_at"], errors="coerce")
    df_clean["updated_at"] = pd.to_datetime(df_clean["updated_at"], errors="coerce")

    # Xử lý DEFAULT NOW()
    df_clean["created_at"] = df_clean["created_at"].fillna(pd.Timestamp.now())

    # 8. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Sắp xếp để ưu tiên giữ bản ghi mới cập nhật
    if "updated_at" in df_clean.columns:
        df_clean = df_clean.sort_values(by=["profile_id", "updated_at"], ascending=[True, False])

    # Bước 8a: Drop duplicate theo Khóa chính (profile_id)
    df_clean = df_clean.drop_duplicates(subset=["profile_id"], keep="first")

    # Bước 8b: Drop duplicate theo user_id (Bảo vệ ràng buộc UNIQUE 1-1)
    # Nếu hệ thống lỗi sinh ra 2 profile cho 1 user, ta vứt bỏ bản ghi cũ hơn.
    df_clean = df_clean.drop_duplicates(subset=["user_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(
            f"    [user_profiles] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp (theo profile_id hoặc user_id).")

    # 9. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected