"""
transforms/follow_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Event: follows.
Xử lý sự kiện người dùng theo dõi nghệ sĩ.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng follows.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [follows] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu dựa trên Schema
    target_columns = ["follow_id", "user_id", "artist_id", "followed_at"]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ để Validate
    df["follow_id"] = pd.to_numeric(df["follow_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")
    df["artist_id"] = pd.to_numeric(df["artist_id"], errors="coerce")

    # 3. Ràng buộc NOT NULL
    # Đối với sự kiện follow, nếu mất ID user hoặc ID artist thì dữ liệu hoàn toàn vô giá trị
    mask_valid = (
            df["follow_id"].notna() & (df["follow_id"] > 0) &
            df["user_id"].notna() & (df["user_id"] > 0) &
            df["artist_id"].notna() & (df["artist_id"] > 0)
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu follow_id, user_id hoặc artist_id)"
        log.info(f"    [follows] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 an toàn cho ID
    df_clean["follow_id"] = df_clean["follow_id"].astype("Int64")
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")
    df_clean["artist_id"] = df_clean["artist_id"].astype("Int64")

    # 6. Xử lý Ngày tháng (Timestamp) và Giá trị mặc định
    df_clean["followed_at"] = pd.to_datetime(df_clean["followed_at"], errors="coerce")

    # Nếu hệ thống gửi sự kiện bị thiếu timestamp, lấy thời điểm hiện tại chạy pipeline
    df_clean["followed_at"] = df_clean["followed_at"].fillna(pd.Timestamp.now())

    # 7. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Sắp xếp tăng dần theo thời gian: ưu tiên ghi nhận lần bấm "Follow" đầu tiên
    df_clean = df_clean.sort_values(by=["user_id", "artist_id", "followed_at"], ascending=[True, True, True])

    # Bước 7a: Xóa trùng lặp theo Khóa chính (follow_id) do lỗi mạng đẩy 2 lần
    df_clean = df_clean.drop_duplicates(subset=["follow_id"], keep="first")

    # Bước 7b: Xóa trùng lặp theo cặp (user_id, artist_id) để bảo vệ ràng buộc UNIQUE
    df_clean = df_clean.drop_duplicates(subset=["user_id", "artist_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [follows] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp (vi phạm PK hoặc UNIQUE).")

    # 8. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected