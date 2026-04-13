"""
FILE 08: seed_subscriptions_payments.py
Seed subscriptions + payments cho 150 premium users.
Tổng ~800 giao dịch:
  - Premium 30 ngày  (59,000 VND): 640 giao dịch (80%)
  - Premium 365 ngày (590,000 VND): 160 giao dịch (20%)
Doanh thu tăng dần từ 2024-01 đến 2025-12.
Chạy: python seed_08_subscriptions_payments.py
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import datetime, timedelta

random.seed(46)

DIR = os.path.dirname(os.path.abspath(__file__))

# Phân phối số giao dịch theo tháng (24 tháng, tổng ~800)
# Tăng từ 5 → 100 giao dịch/tháng
TX_PER_MONTH = []
val = 5.0
for i in range(24):
    TX_PER_MONTH.append(max(5, int(val)))
    val *= 1.115
total = sum(TX_PER_MONTH)
# Scale về 800
TX_PER_MONTH = [max(1, round(v * 800 / total)) for v in TX_PER_MONTH]
diff = 800 - sum(TX_PER_MONTH)
TX_PER_MONTH[-1] += diff

PLAN_30  = ("premium_30days",  59_000,  30)
PLAN_365 = ("premium_365days", 590_000, 365)

def rand_paid_at(year, month):
    """Phân phối trong tháng: đầu tháng nhiều nhất."""
    r = random.random()
    if r < 0.40:
        day = random.randint(1, 5)
    elif r < 0.60:
        day = random.randint(6, 10)
    elif r < 0.85:
        day = random.randint(11, 20)
    else:
        day = random.randint(21, 28)
    hour   = random.randint(0, 23)
    minute = random.randint(0, 59)
    return datetime(year, month, day, hour, minute)

def seed_subscriptions_payments():
    conn = get_conn()
    cur  = conn.cursor()

    # Đọc user_segments để lấy premium users
    csv_path = os.path.join(DIR, "user_segments.csv")
    premium_user_ids = []
    if os.path.exists(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                premium_user_ids.append(int(row["user_id"]))

    # Lấy premium user_ids từ DB (plan='premium'), loại bỏ special (1,2,3)
    cur.execute(
        "SELECT user_id FROM users WHERE plan='premium' AND user_id > 3 ORDER BY user_id"
    )
    db_premium = [r[0] for r in cur.fetchall()]

    # Dùng DB list, giới hạn 150
    random.shuffle(db_premium)
    premium_150 = db_premium[:150]

    # Tạo pool giao dịch: 80% 30days, 20% 365days
    tx_types = [PLAN_30] * 640 + [PLAN_365] * 160
    random.shuffle(tx_types)
    tx_iter = iter(tx_types)

    pay_inserted = 0
    sub_inserted = 0

    for month_idx, tx_count in enumerate(TX_PER_MONTH):
        year  = 2024 + month_idx // 12
        month = month_idx % 12 + 1

        for _ in range(tx_count):
            try:
                plan_name, amount, duration = next(tx_iter)
            except StopIteration:
                break

            user_id  = random.choice(premium_150)
            paid_at  = rand_paid_at(year, month)
            end_date = paid_at + timedelta(days=duration)

            # Payment
            cur.execute(
                """
                INSERT INTO payments
                    (user_id, amount, currency, plan, duration_days,
                     status, paid_at, note)
                VALUES (%s,%s,'VND',%s,%s,'success',%s,%s)
                """,
                (user_id, amount, plan_name, duration,
                 paid_at, f"Auto renew {plan_name}"),
            )
            pay_inserted += 1

            # Subscription
            cur.execute(
                """
                INSERT INTO subscriptions
                    (user_id, plan, start_date, end_date, status, created_at)
                VALUES (%s,%s,%s,%s,'active',%s)
                """,
                (user_id, plan_name, paid_at, end_date, paid_at),
            )
            sub_inserted += 1

        conn.commit()

    cur.close()
    conn.close()
    print(f"[seed_subscriptions_payments] ✓ payments={pay_inserted}  subscriptions={sub_inserted}")

if __name__ == "__main__":
    seed_subscriptions_payments()
