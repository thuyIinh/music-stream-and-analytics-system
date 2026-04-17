"""
main.py — Entry point điều phối 5 pipeline tuần tự (Cách A - Decoupled)
=======================================================================
Luồng:
  P1 Extract  →  P2 Bronze→Silver  →  P3 Silver→Gold
  →  P4 Load DW  →  P5 Refresh Mart

Điểm then chốt:
  - Tách bạch hoàn toàn Database ra khỏi main.py.
  - main.py chỉ đóng vai trò "Bộ đếm giờ" và "Trạm trung chuyển" gọi các hàm.
  - start_time_extract được khởi tạo TRƯỚC P1 và truyền cho P5 để chốt _last_run.

Chạy: python main.py
"""

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from pipelines.p01_extract          import run as p01_run
from pipelines.p02_bronze_to_silver import run as p02_run
from pipelines.p03_silver_to_gold   import run as p03_run
from pipelines.p04_load_dw          import run as p04_run
from pipelines.p05_refresh_mart     import run as p05_run

# ─── logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main")


# ════════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ════════════════════════════════════════════════════════════════════════════

def main():
    log.info("╔" + "═" * 58 + "╗")
    log.info("║  ETL PIPELINE — MUSIC ANALYTICS SYSTEM (DECOUPLED)" + " " * 8 + "║")
    log.info("╚" + "═" * 58 + "╝")

    # 1. Chốt mốc thời gian bắt đầu
    # Đây là thời điểm dùng để ghi vào _last_run.json ở cuối P5.
    start_time_extract = datetime.now()
    log.info(f"  start_time_extract = {start_time_extract.isoformat()}")

    total_t0 = time.time()

    # 2. Định nghĩa danh sách các Pipeline
    # Lưu ý: Không truyền run_id nữa. File nào cần ghi log sẽ tự sinh ID của nó.
    PIPELINES = [
        ("P1 Extract",         lambda: p01_run(start_time=start_time_extract)),
        ("P2 Bronze→Silver",   lambda: p02_run()),
        ("P3 Silver→Gold",     lambda: p03_run()),
        ("P4 Load DW",         lambda: p04_run()),
        ("P5 Refresh Mart",    lambda: p05_run(start_time_extract=start_time_extract)),
    ]

    # 3. Chạy tuần tự
    for name, fn in PIPELINES:
        log.info(f"\n{'─'*60}")
        log.info(f"  ▶  {name}")
        log.info(f"{'─'*60}")
        t0 = time.time()
        try:
            fn()  # Gọi hàm run() của từng pipeline
            elapsed = round(time.time() - t0, 1)
            log.info(f"  ✓  {name} — {elapsed}s")
        except Exception as e:
            elapsed = round(time.time() - t0, 1)
            log.error(f"  ✗  {name} FAILED sau {elapsed}s: {e}")
            log.error("  Pipeline dừng do lỗi. Các bước tiếp theo đã bị hủy.")
            sys.exit(1)

    # 4. Tổng kết
    total_elapsed = round(time.time() - total_t0, 1)

    log.info("\n╔" + "═" * 58 + "╗")
    log.info("║  TOÀN BỘ ETL HOÀN THÀNH" + " " * 33 + "║")
    log.info(f"║  Tổng thời gian: {total_elapsed}s" + " " * (40 - len(str(total_elapsed))) + "║")
    log.info(f"║  next_extract  : {start_time_extract.isoformat()[:19]}" + " " * 19 + "║")
    log.info("╚" + "═" * 58 + "╝")


if __name__ == "__main__":
    main()