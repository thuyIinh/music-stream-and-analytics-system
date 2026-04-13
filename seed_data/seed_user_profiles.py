"""
FILE 05: seed_user_profiles.py
Seed 500 profiles tương ứng 500 users.
DOB: 1990-95=20% | 1996-2000=35% | 2001-05=30% | 2006-08=10% | NULL=5%
Gender: male=45% | female=45% | other=5% | NULL=5%
Country đọc từ user_segments.csv (cùng thư mục).
Chạy: python seed_05_user_profiles.py
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import date, timedelta

random.seed(43)

DIR = os.path.dirname(os.path.abspath(__file__))

# Phân phối DOB
DOB_RANGES = [
    (date(1990, 1, 1), date(1995, 12, 31), 0.20),
    (date(1996, 1, 1), date(2000, 12, 31), 0.35),
    (date(2001, 1, 1), date(2005, 12, 31), 0.30),
    (date(2006, 1, 1), date(2008, 12, 31), 0.10),
]
# 5% NULL — handled by returning None

GENDER_CHOICES = (
    ["male"]   * 45 +
    ["female"] * 45 +
    ["other"]  *  5 +
    [None]     *  5
)

BIOS = [
    "Yêu âm nhạc và cuộc sống.",
    "Music is my therapy.",
    "Nghe nhạc mọi lúc mọi nơi.",
    "Chill vibes only.",
    "K-pop fan forever.",
    "Rock never dies.",
    "Sống cùng âm nhạc.",
    "Playlist curator.",
    "음악을 사랑해요.",
    "音楽が大好き。",
    None, None, None,  # 1/13 chance bio = NULL
]

def rand_dob():
    r = random.random()
    cumulative = 0.0
    for start, end, prob in DOB_RANGES:
        cumulative += prob
        if r < cumulative:
            delta = (end - start).days
            return start + timedelta(days=random.randint(0, delta))
    return None  # 5% NULL

def seed_profiles():
    conn = get_conn()
    cur  = conn.cursor()

    # Đọc country từ CSV
    csv_path = os.path.join(DIR, "user_segments.csv")
    country_map = {}  # user_id → country
    if os.path.exists(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                country_map[int(row["user_id"])] = row["country"]

    # Fallback country cho special users
    country_map.setdefault(1, "Vietnam")
    country_map.setdefault(2, "Vietnam")
    country_map.setdefault(3, "Vietnam")

    # Lấy tất cả user_id
    cur.execute("SELECT user_id, created_at FROM users ORDER BY user_id")
    users = cur.fetchall()

    inserted = 0
    batch = []
    for user_id, created_at in users:
        dob     = rand_dob()
        gender  = random.choice(GENDER_CHOICES)
        bio     = random.choice(BIOS)
        country = country_map.get(user_id, "Others")
        avatar  = f"/uploads/avatars/user_{user_id}.jpg"

        batch.append((user_id, avatar, bio, country, dob, gender, created_at, created_at))

    cur.executemany(
        """
        INSERT INTO user_profiles
            (user_id, avatar_url, bio, country, date_of_birth, gender, created_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        batch,
    )
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"[seed_profiles] ✓ inserted={inserted}")

if __name__ == "__main__":
    seed_profiles()
