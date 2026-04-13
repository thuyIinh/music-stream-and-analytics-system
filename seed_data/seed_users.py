"""
FILE 04: seed_users.py
Seed 500 users:
  - Free: 350 | Premium: 150
  - Tăng trưởng đều 24 tháng (2024-01 → 2025-12)
  - Segment: heavy/regular/casual/occasional/at_risk/churned
  - Country: VN=200 | US=150 | KR=50 | UK=40 | Others=60
  - Special: user_id 1,2,3
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import datetime, timedelta
from hashlib import md5

random.seed(42)

# ---------------------------------------------------------------------------
# Cấu hình phân phối
# ---------------------------------------------------------------------------

MONTHLY_COUNTS = []
base = 8
for m in range(24):
    MONTHLY_COUNTS.append(base)
    base = int(base * 1.055) + (1 if m % 3 == 0 else 0)

total_raw = sum(MONTHLY_COUNTS)
MONTHLY_COUNTS = [max(1, round(c * 497 / total_raw)) for c in MONTHLY_COUNTS]
diff = 497 - sum(MONTHLY_COUNTS)
MONTHLY_COUNTS[-1] += diff

COUNTRIES = (
    ["Vietnam"] * 200 +
    ["USA"]     * 150 +
    ["Korea"]   *  50 +
    ["UK"]      *  40 +
    ["Japan"]   *  15 +
    ["France"]  *  15 +
    ["Germany"] *  15 +
    ["Others"]  *  15
)
random.shuffle(COUNTRIES)

SEGMENT_POOL = (
    ["heavy"]      *  50 +
    ["regular"]    * 100 +
    ["casual"]     * 150 +
    ["occasional"] * 100 +
    ["at_risk"]    *  57 +
    ["churned"]    *  40
)
random.shuffle(SEGMENT_POOL)

PLAN_BY_SEGMENT = {
    "heavy":      0.70,
    "regular":    0.50,
    "casual":     0.25,
    "occasional": 0.10,
    "at_risk":    0.08,
    "churned":    0.05,
}

def fake_hash(email: str) -> str:
    return "$2b$12$" + md5(email.encode()).hexdigest()[:53]

def rand_name() -> str:
    first = random.choice([
        "Minh","Anh","Huy","Linh","Trang","Nam","Phong","Thu","Bảo","Thảo",
        "Alex","Sam","Jordan","Taylor","Morgan","Chris","Riley","Casey","Jamie","Drew",
        "Min","Jae","Hyun","Soo","Ji","Yu","Hana","Kenji","Sakura","Mei",
        "Emma","Liam","Noah","Olivia","James","Sophie","Lucas","Elsa","Hugo","Camille",
    ])
    last = random.choice([
        "Nguyen","Tran","Le","Pham","Hoang","Vu","Do","Dang","Bui","Ngo",
        "Kim","Park","Lee","Choi","Jung","Tanaka","Yamamoto","Sato","Smith","Johnson",
        "Williams","Brown","Martin","Bernard","Dupont","Garcia","Lopez","Müller",
    ])
    return f"{first} {last}"

def random_datetime(year, month, day=None):
    """Tạo datetime có giờ phút giây ngẫu nhiên"""
    if day is None:
        day = random.randint(1, 28)
    return datetime(
        year, month, day,
        random.randint(0, 23),
        random.randint(0, 59),
        random.randint(0, 59)
    )

def seed_users():
    conn = get_conn()
    cur = conn.cursor()

    # ---- 3 special users ----
    specials = [
        (1, "admin@example.com",   "Admin User",   "admin", "premium"),
        (2, "testuser@example.com","Test User",    "user",  "free"),
        (3, "premium@example.com", "Premium User", "user",  "premium"),
    ]
    for uid, email, dname, role, plan in specials:
        cur.execute(
            """
            INSERT INTO users
                (email, password_hash, display_name, role, plan,
                 is_active, created_at, updated_at, last_login_at)
            VALUES (%s,%s,%s,%s,%s,true,'2024-01-01 10:30:00','2024-01-01 10:30:00','2026-03-01 08:45:00')
            ON CONFLICT (email) DO NOTHING
            """,
            (email, fake_hash(email), dname, role, plan),
        )

    conn.commit()
    print("  → Inserted 3 special users")

    # ---- 497 regular users ----
    segment_iter = iter(SEGMENT_POOL)
    country_iter = iter(COUNTRIES)

    ref_date = datetime(2026, 3, 31, 23, 59, 59)   # có giờ để tính chính xác

    segments_out = []   # (user_id, segment, country)
    user_idx = 0

    for month_idx, count in enumerate(MONTHLY_COUNTS):
        year  = 2024 + month_idx // 12
        month = month_idx % 12 + 1

        for _ in range(count):
            segment = next(segment_iter)
            country = next(country_iter)
            plan_prob = PLAN_BY_SEGMENT[segment]
            plan = "premium" if random.random() < plan_prob else "free"

            # created_at
            created_at = random_datetime(year, month)

            # last_login_at
            if segment == "churned":
                days_ago = random.randint(31, 90)
            elif segment == "at_risk":
                days_ago = random.randint(15, 29)
            elif segment == "occasional":
                days_ago = random.randint(5, 14)
            else:
                days_ago = random.randint(0, 4)

            last_login = ref_date - timedelta(days=days_ago)

            # Đảm bảo last_login >= created_at
            if last_login < created_at:
                last_login = created_at + timedelta(days=random.randint(1, 10))

            # Thêm giờ phút giây cho last_login
            last_login = last_login.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )

            email = f"user_{1000 + user_idx}@music.com"
            name  = rand_name()

            cur.execute(
                """
                INSERT INTO users
                    (email, password_hash, display_name, role, plan,
                     is_active, created_at, updated_at, last_login_at)
                VALUES (%s,%s,%s,'user',%s,true,%s,%s,%s)
                ON CONFLICT (email) DO NOTHING
                RETURNING user_id
                """,
                (email, fake_hash(email), name, plan, created_at, created_at, last_login),
            )
            row = cur.fetchone()
            if row:
                segments_out.append((row[0], segment, country))

            user_idx += 1

        conn.commit()   # commit theo tháng

    cur.close()
    conn.close()

    # Ghi file user_segments.csv
    csv_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seeds")
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, "user_segments.csv")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "segment", "country"])
        writer.writerows(segments_out)

    print(f"[seed_users] ✓ inserted={len(segments_out)+3} users")
    print(f"              user_segments.csv created at: {csv_path}")

if __name__ == "__main__":
    seed_users()