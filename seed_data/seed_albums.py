"""
FILE 03: seed_albums.py
Seed 250 album:
  Tier A (10): 5 album/người = 50
  Tier B (20): 3 album/người = 60
  Tier C (30): 2 album/người = 60
  Tier D (40): 2 album/người = 80
  Tổng = 250

release_date phân phối: 2022=40 | 2023=50 | 2024=60 | 2025=70 | 2026=30
Chạy: python seed_03_albums.py
"""

import sys, os, random
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import date, timedelta

random.seed(42)

ALBUM_PER_TIER = {"A": 5, "B": 3, "C": 2, "D": 2}

# Phân phối năm release: list các năm để sample
YEAR_POOL = (
    [2022] * 40 +
    [2023] * 50 +
    [2024] * 60 +
    [2025] * 70 +
    [2026] * 30
)
random.shuffle(YEAR_POOL)

ALBUM_TITLE_TEMPLATES = [
    "{artist} Vol. {n}",
    "Chapter {n}",
    "Journey {n}",
    "Những Cảm Xúc {n}",
    "Ký Ức {n}",
    "Âm Vang",
    "Bình Yên",
    "Hành Trình",
    "Nắng Mới",
    "Chiều Tối",
    "Tháng Năm",
    "Mùa Đông",
    "First Light",
    "Midnight Tales",
    "Golden Hour",
    "After Hours",
    "Resonance",
    "Eclipse",
    "Serenade",
    "Overture",
    "Prologue",
    "Epilogue",
    "Horizon",
    "Pulse",
    "Wavelength",
]

def rand_date_in_year(year: int) -> date:
    start = date(year, 1, 1)
    # 2026: chỉ đến tháng 3
    end = date(year, 3, 31) if year == 2026 else date(year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def seed_albums():
    conn = get_conn()
    cur  = conn.cursor()

    # Lấy danh sách artist_id theo thứ tự insert (index = tier)
    cur.execute("SELECT artist_id FROM artists ORDER BY artist_id")
    artist_ids = [row[0] for row in cur.fetchall()]

    # Map tier theo index
    def get_tier(idx):
        if idx < 10: return "A"
        if idx < 30: return "B"
        if idx < 60: return "C"
        return "D"

    year_iter = iter(YEAR_POOL)
    album_num = 0  # đếm toàn cục để lấy cover_url unique
    inserted  = 0

    for idx, artist_id in enumerate(artist_ids):
        tier       = get_tier(idx)
        num_albums = ALBUM_PER_TIER[tier]

        for i in range(num_albums):
            album_num += 1
            year      = next(year_iter)
            rel_date  = rand_date_in_year(year)

            # Tiêu đề album đa dạng
            tmpl  = random.choice(ALBUM_TITLE_TEMPLATES)
            title = tmpl.format(artist=f"Artist{artist_id}", n=i + 1)
            title = title[:255]

            cover     = f"/uploads/covers/album_{album_num}.jpg"
            n_tracks  = random.randint(8, 16)
            desc      = f"Album thứ {i+1} của nghệ sĩ, phát hành năm {year}."

            cur.execute(
                """
                INSERT INTO albums
                    (title, artist_id, cover_url, release_date,
                     total_tracks, description, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, true, %s, %s)
                """,
                (title, artist_id, cover, rel_date,
                 n_tracks, desc, rel_date, rel_date),
            )
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"[seed_albums] ✓ inserted={inserted}")

if __name__ == "__main__":
    seed_albums()
