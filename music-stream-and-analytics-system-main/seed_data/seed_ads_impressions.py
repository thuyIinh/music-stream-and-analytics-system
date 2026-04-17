"""
FILE 12: seed_ads_impressions.py
Seed 30 ads (10 năm 2025, 20 năm 2026) + 80,000 ad_impressions.
Chỉ cho 350 free users.
2024: 25,000 impressions | 2025: 55,000 impressions
Chạy: python seed_12_ads_impressions.py
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import date, datetime, timedelta
import psycopg2.extras

random.seed(50)
DIR = os.path.dirname(os.path.abspath(__file__))

SPONSORS = [
    "Vinamilk", "TH True Milk", "Grab", "Shopee", "Lazada",
    "Tiki", "VinFast", "Viettel", "MoMo", "ZaloPay",
    "Techcombank", "VPBank", "Biti's", "Highlands Coffee",
    "The Coffee House", "Trung Nguyên Legend", "PNJ", "DOJI",
    "FPT Shop", "Thế Giới Di Động",
]

PRODUCTS = [
    "Sữa tươi thanh trùng", "Ứng dụng giao xe", "Sale 11.11",
    "Mua sắm online", "Xe điện mới nhất", "Gói cước 5G",
    "Ví điện tử tiện lợi", "Tài khoản tiết kiệm",
    "Giày thể thao mới", "Cà phê ưu đãi",
    "Điện thoại flagship", "Trang sức vàng",
    "Mua ngay, trả sau", "Ưu đãi hội viên",
    "Flash sale hàng ngày",
]

# Phân phối impressions theo tháng (24 tháng, tổng 80,000)
# 2024: 25,000 | 2025: 55,000
IMP_2024 = [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2100, 2400]
IMP_2025 = [3000, 2800, 3200, 3500, 3800, 4000, 4200, 4500, 4800, 5000, 5500, 6700]
# Chuẩn lại tổng
def scale_to(lst, target):
    total = sum(lst)
    scaled = [max(100, round(v * target / total)) for v in lst]
    diff = target - sum(scaled)
    scaled[-1] += diff
    return scaled

IMP_2024 = scale_to(IMP_2024, 25_000)
IMP_2025 = scale_to(IMP_2025, 55_000)
MONTHLY_IMP = IMP_2024 + IMP_2025

def rand_date_range(year, is_2026=False):
    """Start date trong năm, end date 30-90 ngày sau."""
    if is_2026:
        start = date(2026, random.randint(1, 3), random.randint(1, 28))
    else:
        start = date(year, random.randint(1, 11), random.randint(1, 28))
    end = start + timedelta(days=random.randint(30, 90))
    return start, end

def seed_ads_impressions():
    conn = get_conn()
    cur  = conn.cursor()

    # ----------------------------------------------------------------
    # 30 ads
    # ----------------------------------------------------------------
    ad_ids = []
    for i in range(30):
        is_2026  = i >= 10
        year     = 2026 if is_2026 else 2025
        start, end = rand_date_range(year, is_2026)
        is_active = end >= date(2026, 3, 31)  # còn hạn so với "hôm nay"

        sponsor = random.choice(SPONSORS)
        product = random.choice(PRODUCTS)
        title   = f"{sponsor} - {product}"

        cur.execute(
            """
            INSERT INTO ads (title, sponsor, image_url, target_url,
                             is_active, start_date, end_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING ad_id
            """,
            (title, sponsor,
             f"/uploads/ads/ad_{i+1}.jpg",
             f"https://example.com/ads/{i+1}",
             is_active, start, end),
        )
        ad_ids.append(cur.fetchone()[0])

    conn.commit()
    print(f"  ads inserted={len(ad_ids)}")

    # ----------------------------------------------------------------
    # Free users
    # ----------------------------------------------------------------
    csv_path = os.path.join(DIR, "user_segments.csv")
    all_free_ids = []
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                all_free_ids.append(int(row["user_id"]))

    # Lấy free users từ DB (plan='free'), user_id > 3
    cur.execute("SELECT user_id FROM users WHERE plan='free' AND user_id > 3")
    db_free = [r[0] for r in cur.fetchall()]
    random.shuffle(db_free)
    free_350 = db_free[:350]

    if not free_350:
        free_350 = all_free_ids[:350]

    # ----------------------------------------------------------------
    # Impressions
    # ----------------------------------------------------------------
    # Ads theo năm: 2025 ads = ad_ids[:10], 2026 ads = ad_ids[10:]
    ads_2025 = ad_ids[:10]
    ads_2026 = ad_ids[10:]

    total_imp = 0
    BATCH_SIZE = 5_000

    batch = []

    def flush():
        nonlocal total_imp
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO ad_impressions (ad_id, user_id, shown_at, is_clicked)
            VALUES %s
            """,
            batch,
            page_size=BATCH_SIZE,
        )
        conn.commit()
        total_imp += len(batch)
        batch.clear()

    for month_idx, count in enumerate(MONTHLY_IMP):
        year  = 2024 + month_idx // 12
        month = month_idx % 12 + 1

        # Chọn ad pool phù hợp
        if year == 2024:
            # 2024: chưa có ads nào (ads bắt đầu từ 2025)
            # Dùng ads_2025 để giả lập (trong thực tế ads có thể chạy trước)
            ad_pool = ads_2025
        elif year == 2025:
            ad_pool = ads_2025
        else:
            ad_pool = ads_2026

        # Pattern giờ giống play_history
        HOUR_W = [2,1,.5,.5,.5,2,5,10,12,8,5,5,7,6,4,4,5,7,8,10,13,13,11,7]
        import calendar
        _, days_in = calendar.monthrange(year, month)

        users_sample = random.choices(free_350, k=count)
        ads_sample   = random.choices(ad_pool, k=count)

        for i in range(count):
            day    = random.randint(1, days_in)
            hour   = random.choices(range(24), weights=HOUR_W)[0]
            minute = random.randint(0, 59)
            shown  = datetime(year, month, day, hour, minute)

            is_clicked = random.random() < 0.05  # CTR 5%

            batch.append((ads_sample[i], users_sample[i], shown, is_clicked))

            if len(batch) >= BATCH_SIZE:
                flush()

        print(f"  [{year}-{month:02d}] impressions={count}")

    if batch:
        flush()

    cur.close()
    conn.close()
    print(f"[seed_ads_impressions] ✓ ads=30  impressions={total_imp:,}")

if __name__ == "__main__":
    seed_ads_impressions()
