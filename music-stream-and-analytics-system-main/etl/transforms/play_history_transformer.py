"""
transforms/play_history_transformer.py
Module làm sạch, biến đổi và phi chuẩn hóa dữ liệu Event: play_history.
Thực hiện Lookup RAM (với songs), tính derived metrics và Sessionization.
"""

import logging
import pandas as pd

log = logging.getLogger(__name__)

# Khoảng thời gian cắt session (phút)
SESSION_GAP_MINUTES = 30


def transform(df: pd.DataFrame, songs_df: pd.DataFrame = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Hàm làm sạch dữ liệu bảng play_history.
    Yêu cầu truyền thêm songs_df để thực hiện Lookup.
    """
    if df is None or df.empty:
        log.warning("    [play_history] DataFrame rỗng, không có gì để transform.")
        return df, pd.DataFrame()

    if songs_df is None or songs_df.empty:
        raise ValueError("[play_history] Lỗi nghiêm trọng: Tham số 'songs_df' bị trống. Không thể thực hiện Lookup!")


    # 1. Khai báo danh sách các cột mục tiêu cho tầng Silver (Đã Denormalize)
    target_columns = [
        "play_id", "user_id", "song_id", "played_at", "duration_played",
        "is_completed", "is_skipped", "source", "device_type",
        "artist_id", "genre_id", "duration", "completion_rate",
        "hour_of_day", "day_of_week", "session_id"
    ]

    for col in ["play_id", "source", "device_type"]:
        if col not in df.columns:
            df[col] = None

    # 2. Xử lý ép kiểu sơ bộ
    df["play_id"] = pd.to_numeric(df["play_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")
    df["song_id"] = pd.to_numeric(df["song_id"], errors="coerce")
    df["duration_played"] = pd.to_numeric(df["duration_played"], errors="coerce")
    df["played_at"] = pd.to_datetime(df["played_at"], errors="coerce")

    # 3. Ràng buộc cơ bản (Base Validation)
    mask_valid = (
            df["user_id"].notna() & (df["user_id"] > 0) &
            df["song_id"].notna() & (df["song_id"] > 0) &
            df["duration_played"].notna() & (df["duration_played"] >= 0) &
            df["played_at"].notna()
    )

    df_rejected_base = df[~mask_valid].copy()
    if not df_rejected_base.empty:
        df_rejected_base["reject_reason"] = "Vi phạm logic cơ sở (thiếu user, song, thời gian âm/null)"

    df_clean = df[mask_valid].copy()

    if df_clean.empty:
        return pd.DataFrame(columns=target_columns), df_rejected_base

    # 4. LOOKUP RAM (Denormalize với bảng songs)
    # Chuẩn bị lookup table từ songs_df
    songs_lkp = songs_df[["song_id", "duration", "artist_id", "genre_id"]].copy()
    songs_lkp["song_id"] = pd.to_numeric(songs_lkp["song_id"], errors="coerce")
    songs_lkp.dropna(subset=["song_id"], inplace=True)
    songs_lkp.drop_duplicates(subset=["song_id"], inplace=True)

    # Thực hiện Left Join
    df_clean = df_clean.merge(songs_lkp, on="song_id", how="left")

    # Lọc các dòng không tìm thấy bài hát (duration bị Null)
    mask_song_found = df_clean["duration"].notna() & (df_clean["duration"] > 0)

    df_rejected_lookup = df_clean[~mask_song_found].copy()
    if not df_rejected_lookup.empty:
        df_rejected_lookup["reject_reason"] = "Lookup thất bại: Không tìm thấy song_id trong Master Data"

    df_clean = df_clean[mask_song_found].copy()

    # Tổng hợp toàn bộ dữ liệu lỗi
    df_rejected = pd.concat([df_rejected_base, df_rejected_lookup], ignore_index=True)
    if not df_rejected.empty:
        log.info(f"    [play_history] Tách tổng cộng {len(df_rejected):,} dòng lỗi vào file rejected.")

    if df_clean.empty:
        return pd.DataFrame(columns=target_columns), df_rejected

    # 5. TÍNH TOÁN CÁC CỘT PHÁI SINH (Derived Columns)
    df_clean["duration"] = pd.to_numeric(df_clean["duration"])

    # completion_rate = duration_played / duration
    df_clean["completion_rate"] = (
            df_clean["duration_played"] / df_clean["duration"]
    ).clip(upper=1.0).round(4)

    # Tính is_completed và is_skipped
    df_clean["is_completed"] = df_clean["completion_rate"] >= 0.8
    df_clean["is_skipped"] = df_clean["completion_rate"] <= 0.3

    # Trích xuất thời gian
    df_clean["hour_of_day"] = df_clean["played_at"].dt.hour
    df_clean["day_of_week"] = df_clean["played_at"].dt.weekday

    # 6. SESSIONIZATION (Phân rã phiên nghe nhạc)
    # Sắp xếp theo User và Thời gian nghe
    df_clean = df_clean.sort_values(by=["user_id", "played_at"])

    # Lấy thời gian của bài hát trước đó
    df_clean["_prev_played"] = df_clean.groupby("user_id")["played_at"].shift(1)

    # Tính khoảng cách (gap) theo phút
    df_clean["_gap_min"] = (df_clean["played_at"] - df_clean["_prev_played"]).dt.total_seconds() / 60.0

    # Nếu là bài hát đầu tiên của User (gap bị NaN) -> Đặt gap lớn hơn 30 để tạo session mới
    df_clean["_gap_min"] = df_clean["_gap_min"].fillna(SESSION_GAP_MINUTES + 1)

    # Đánh dấu những điểm bắt đầu Session mới
    df_clean["_is_new_session"] = df_clean["_gap_min"] > SESSION_GAP_MINUTES

    # Tạo Session ID (Dùng hàm cumsum để đếm số session của từng user)
    date_str = df_clean["played_at"].dt.strftime("%Y%m%d").iloc[0]  # lấy ngày của batch
    df_clean["session_idx"] = df_clean.groupby("user_id")["_is_new_session"].cumsum().astype(str)
    df_clean["session_id"] = (
            "u" + df_clean["user_id"].astype(str)
            + "_" + date_str
            + "_s" + df_clean["session_idx"]
    )

    # Xóa các cột tạm phục vụ tính toán session
    df_clean.drop(columns=["_prev_played", "_gap_min", "_is_new_session", "session_idx"], inplace=True)

    # 7. Chuẩn hóa chuỗi & ID
    df_clean["play_id"] = df_clean["play_id"].astype("Int64")
    df_clean["user_id"] = df_clean["user_id"].astype("Int64")
    df_clean["song_id"] = df_clean["song_id"].astype("Int64")
    df_clean["artist_id"] = df_clean["artist_id"].astype("Int64")
    df_clean["genre_id"] = df_clean["genre_id"].astype("Int64")

    df_clean["source"] = df_clean["source"].astype(str).str.strip().str.lower()
    df_clean["device_type"] = df_clean["device_type"].apply(
        lambda x: str(x).strip().lower() if pd.notna(x) and str(x).strip() else "web"
    )

    # 8. Loại bỏ bản ghi trùng lặp
    before_dedup = len(df_clean)

    # Nếu có play_id, drop theo play_id. Nếu không, drop theo cặp (user, song, thời gian)
    if df_clean["play_id"].notna().any():
        df_clean = df_clean.drop_duplicates(subset=["play_id"], keep="first")
    else:
        df_clean = df_clean.drop_duplicates(subset=["user_id", "song_id", "played_at"], keep="first")

    if before_dedup - len(df_clean) > 0:
        log.info(f"    [play_history] Loại bỏ {before_dedup - len(df_clean):,} dòng trùng lặp.")

    # 9. Lọc đúng danh sách cột và trả về
    df_clean = df_clean[target_columns]

    return df_clean, df_rejected