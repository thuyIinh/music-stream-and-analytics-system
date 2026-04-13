"""
run_all.py — chạy toàn bộ seed theo thứ tự dependency.

Cách dùng:
  DB_HOST=localhost DB_PORT=5432 DB_NAME=music_db \
  DB_USER=postgres DB_PASSWORD=postgres \
  python run_all.py

Ước tính thời gian:
  File 01-08: < 1 phút
  File 09 (500k plays): 3-5 phút
  File 10-12: < 2 phút
  Tổng: ~5-8 phút
"""

import time
import importlib
import sys, os

SEEDS = [
    ("seed_01_genres",               "seed_genres"),
    ("seed_02_artists",              "seed_artists"),
    ("seed_03_albums",               "seed_albums"),
    ("seed_04_users",                "seed_users"),
    ("seed_05_user_profiles",        "seed_profiles"),
    ("seed_06_songs",                "seed_songs"),
    ("seed_07_song_genres",          "seed_song_genres"),
    ("seed_08_subscriptions_payments","seed_subscriptions_payments"),
    ("seed_09_play_history",         "seed_play_history"),
    ("seed_10_likes_follows",        "seed_likes_follows"),
    ("seed_11_playlists",            "seed_playlists"),
    ("seed_12_ads_impressions",      "seed_ads_impressions"),
]

def main():
    # Đảm bảo import được từ thư mục seeds/
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    total_start = time.time()
    for module_name, func_name in SEEDS:
        print(f"\n{'='*55}")
        print(f"▶  Running {module_name}.{func_name}()")
        print(f"{'='*55}")
        t0 = time.time()
        try:
            mod = importlib.import_module(module_name)
            fn  = getattr(mod, func_name)
            fn()
        except Exception as e:
            print(f"[ERROR] {module_name}: {e}")
            raise
        elapsed = time.time() - t0
        print(f"   ✓ Done in {elapsed:.1f}s")

    total = time.time() - total_start
    print(f"\n{'='*55}")
    print(f"✅  All seeds completed in {total:.0f}s ({total/60:.1f} min)")
    print(f"{'='*55}")

if __name__ == "__main__":
    main()
