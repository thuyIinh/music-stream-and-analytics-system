"""
transforms/ad_impression_transformer.py
Module làm sạch và biến đổi dữ liệu cho bảng Event: ad_impressions.
Thuộc nhóm "Simple Events": Chỉ validate key, ép kiểu và khử trùng lặp.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng ad_impressions.
    Trả về Tuple: (Dữ liệu sạch, Dữ liệu lỗi)
    """
    if df is None or df.empty:
        log.warning("    [ad_impressions] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    # 1. Khai báo danh sách các cột mục tiêu
    target_columns = ["impression_id", "ad_id", "user_id", "shown_at", "is_clicked"]

    # Bổ sung các cột bị thiếu bằng None
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý khóa chính và khóa ngoại sơ bộ
    df["impression_id"] = pd.to_numeric(df["impression_id"], errors="coerce")
    df["ad_id"] = pd.to_numeric(df["ad_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")

    # 3. Ràng buộc NOT NULL
    # Đối với sự kiện hiển thị quảng cáo, thiếu ID quảng cáo hoặc ID user là dữ liệu rác hoàn toàn.
    mask_valid = (
            df["impression_id"].notna() & (df["impression_id"] > 0) &
            df["ad_id"].notna() & (df["ad_id"] > 0) &
            df["user_id"].notna() & (df["user_id"] > 0)
    )

    # 4. TÁCH DỮ LIỆU LỖI (Rejected)
    df_rejected = df[~mask_valid].copy()
    if not df_rejected.empty:
        df_rejected["reject_reason"] = "Vi phạm NOT NULL (thiếu impression_id, ad_id hoặc user_id)"
        log.info(f"    [ad_impressions] Tách {len(df_rejected):,} dòng lỗi vào file rejected.")

    # 5. TÁCH DỮ LIỆU SẠCH (Clean)
    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return df_clean[target_columns], df_rejected

    # Ép kiểu Int64 an toàn cho ID
    df_clean["impression_id"] = df_clean["impression_id"].astype("Int64")
    df_clean["ad_id"] = df_clean["ad_id"].astype("Int64")
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")

    # 6. Xử lý Ngày tháng (Timestamp) và Boolean
    df_clean["shown_at"] = pd.to_datetime(df_clean["shown_at"], errors="coerce")

    # Event thì nên có thời gian chuẩn, nhưng nếu hệ thống bắn thiếu thì fill DEFAULT NOW() theo schema
    df_clean["shown_at"] = df_clean["shown_at"].fillna(pd.Timestamp.now())

    # is_clicked: DEFAULT false
    df_clean["is_clicked"] = df_clean["is_clicked"].fillna(False).astype(bool)

    # 7. Loại bỏ bản ghi trùng lặp (Deduplicate)
    before_dedup = len(df_clean)

    # Ở dữ liệu event, nếu bị trùng impression_id (do hệ thống bắn log 2 lần), ta giữ lại bản ghi có thời gian hiển thị sớm nhất.
    df_clean = df_clean.sort_values(by=["impression_id", "shown_at"], ascending=[True, True])
    df_clean = df_clean.drop_duplicates(subset=["impression_id"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [ad_impressions] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp impression_id.")

    # 8. Trả về đúng danh sách cột mục tiêu
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected