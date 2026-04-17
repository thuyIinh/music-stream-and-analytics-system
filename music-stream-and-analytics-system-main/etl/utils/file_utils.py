import pandas as pd
from pathlib import Path


def save_to_parquet(data: list, columns: list, folder_path: Path, file_name: str):
    """
    Lưu dữ liệu dạng list of tuples thành file Parquet.
    """
    if not data:
        print(f"  [file_utils] Không có dữ liệu để lưu vào {file_name}")
        return 0

    # Đảm bảo thư mục tồn tại (ví dụ: Data_lake/bronze/2026-04-14)
    folder_path.mkdir(parents=True, exist_ok=True)

    file_path = folder_path / f"{file_name}.parquet"

    # Chuyển thành DataFrame và lưu
    df = pd.DataFrame(data, columns=columns)
    df.to_parquet(file_path, engine="pyarrow", index=False)

    return len(df)