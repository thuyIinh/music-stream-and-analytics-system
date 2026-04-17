"""
FILE 09: seed_play_history.py
Seed ~500,000 bản ghi play_history
Phiên bản đã sửa & tối ưu theo spec
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import datetime
import psycopg2.extras
import calendar

random.seed(47)
DIR = os.path.dirname(os.path.abspath(__file__))

# ------------------------------------------------------------------
# Số plays theo tháng (2024-01 đến 2025-12) - Tổng 500,000
# ------------------------------------------------------------------
MONTHLY_PLAYS = [
    8000, 9000, 10000, 11000, 12000, 14000, 15000, 17000, 18000, 20000, 22000, 25000,  # 2024
    20000, 18000, 20000, 22000, 24000, 26000, 28000, 30000, 28000, 30000, 32000, 41000   # 2025
]

# ------------------------------------------------------------------
# Pattern giờ trong ngày
# ------------------------------------------------------------------
HOUR_WEIGHTS = [2, 1, 0.5, 0.5, 0.5, 2, 5, 10, 12, 8, 5, 5, 7, 6, 4, 4, 5, 7, 8, 10, 13, 13, 11, 7]
HOUR_CUM = []
s = 0
for w in HOUR_WEIGHTS:
    s += w
    HOUR_CUM.append(s)
HOUR_TOTAL = s

def rand_hour():
    r = random.uniform(0, HOUR_TOTAL)
    for h, c in enumerate(HOUR_CUM):
        if r <= c:
            return h
    return 23

# ------------------------------------------------------------------
# Pattern ngày trong tuần
# ------------------------------------------------------------------
DAY_WEIGHTS = [1.0, 1.0, 1.1, 1.1, 1.3, 1.6, 1.5]   # Mon=0 → Sun=6

# ------------------------------------------------------------------
# Mood weight theo mùa/tháng
# ------------------------------------------------------------------
SEASON_MOOD = {
    1:  {"sad": 1.5, "calm": 1.3, "romantic": 1.2, "energetic": 0.8},
    2:  {"romantic": 2.0, "happy": 1.3, "sad": 0.8},
    3:  {"happy": 1.2, "energetic": 1.1},
    4:  {"happy": 1.2},
    5:  {"happy": 1.3, "energetic": 1.2},
    6:  {"energetic": 1.5, "happy": 1.4, "dance": 1.5},
    7:  {"energetic": 1.6, "happy": 1.5},
    8:  {"energetic": 1.5, "happy": 1.4},
    9:  {"calm": 1.2},
    10: {"calm": 1.2, "sad": 1.1},
    11: {"sad": 1.3, "calm": 1.4},
    12: {"happy": 1.5, "energetic": 1.6, "dance": 2.0, "sad": 1.2},
}

# ------------------------------------------------------------------
# Source weights
# ------------------------------------------------------------------
SOURCES_EARLY = ["home"]*4 + ["album"]*2 + ["playlist", "search", "search", "liked_songs", "recommendation"]
SOURCES_LATE  = ["home"]*3 + ["playlist"]*2 + ["recommendation"]*2 + ["album", "search", "liked_songs"]

DEVICES = ["web", "web", "web", "mobile", "mobile", "desktop"]

# ------------------------------------------------------------------
# Completion rate theo segment
# ------------------------------------------------------------------
COMPLETION = {
    "heavy":      {"completed": 0.75, "skipped": 0.10, "partial": 0.15},
    "regular":    {"completed": 0.65, "skipped": 0.15, "partial": 0.20},
    "casual":     {"completed": 0.55, "skipped": 0.25, "partial": 0.20},
    "occasional": {"completed": 0.45, "skipped": 0.35, "partial": 0.20},
    "at_risk":    {"completed": 0.45, "skipped": 0.35, "partial": 0.20},
    "churned":    {"completed": 0.45, "skipped": 0.35, "partial": 0.20},
}

def gen_duration(song_dur, outcome):
    if outcome == "completed":
        return int(song_dur * random.uniform(0.80, 1.00))
    elif outcome == "skipped":
        return int(song_dur * random.uniform(0.05, 0.30))
    else:
        return int(song_dur * random.uniform(0.30, 0.80))

def pick_outcome(segment):
    c = COMPLETION.get(segment, COMPLETION["casual"])
    r = random.random()
    if r < c["completed"]:
        return "completed"
    elif r < c["completed"] + c["skipped"]:
        return "skipped"
    return "partial"

# ------------------------------------------------------------------
# Load dữ liệu
# ------------------------------------------------------------------
def load_data():
    conn = get_conn()
    cur = conn.cursor()

    # User segments
    csv_path = os.path.join(DIR, "user_segments.csv")
    user_segs = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                user_segs[int(row["user_id"])] = row["segment"]

    user_segs[1] = "heavy"
    user_segs[2] = "regular"
    user_segs[3] = "casual"

    # Song weights + duration + mood
    weights_path = os.path.join(DIR, "song_weights.csv")
    song_weights = []
    song_dur_map = {}
    song_mood = {}

    if os.path.exists(weights_path):
        with open(weights_path, newline="") as f:
            for row in csv.DictReader(f):
                sid = int(row["song_id"])
                w = float(row["hot_weight"])
                song_weights.append((sid, w))
    else:
        cur.execute("SELECT song_id, 5.0 FROM songs")
        song_weights = [(r[0], 5.0) for r in cur.fetchall()]

    cur.execute("SELECT song_id, duration FROM songs")
    song_dur_map = {sid: dur for sid, dur in cur.fetchall()}

    cur.execute("SELECT song_id, mood FROM songs")
    song_mood = {r[0]: r[1] for r in cur.fetchall()}

    cur.close()
    conn.close()
    return user_segs, song_weights, song_dur_map, song_mood

def weighted_song_sample(song_weights, season_mood_map, song_mood, k=1, dom_dom_id=None):
    ids = [sw[0] for sw in song_weights]
    weights = []
    for sid, w in song_weights:
        mood = song_mood.get(sid, "happy")
        mood_bonus = season_mood_map.get(mood, 1.0)
        weights.append(w * mood_bonus)

    chosen = random.choices(ids, weights=weights, k=k)

    # Ưu tiên mạnh bài "Đom Đóm" cho demo
    if dom_dom_id:
        for i in range(len(chosen)):
            if random.random() < 0.085:        # ~8.5% chance
                chosen[i] = dom_dom_id
    return chosen

# ------------------------------------------------------------------
# Main Seed
# ------------------------------------------------------------------
def seed_play_history():
    user_segs, song_weights, song_dur_map, song_mood = load_data()

    # Nhóm user theo segment
    seg_users = {}
    for uid, seg in user_segs.items():
        seg_users.setdefault(seg, []).append(uid)

    conn = get_conn()
    cur = conn.cursor()

    # Lấy song_id=1 (Đom Đóm)
    cur.execute("SELECT song_id FROM songs ORDER BY song_id LIMIT 1")
    dom_dom_id = cur.fetchone()[0]

    total_inserted = 0
    batch = []
    BATCH_SIZE = 10_000

    def flush():
        nonlocal total_inserted
        if not batch:
            return
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO play_history 
            (user_id, song_id, played_at, duration_played, is_completed, is_skipped, source, device_type)
            VALUES %s
            """,
            batch,
            page_size=BATCH_SIZE
        )
        conn.commit()
        total_inserted += len(batch)
        print(f"  → Flushed {len(batch):,} records | Total: {total_inserted:,}")
        batch.clear()

    print("Starting seeding play_history...")

    for month_idx in range(24):                     # Chỉ chạy đúng 24 tháng
        target_plays = MONTHLY_PLAYS[month_idx]
        year = 2024 + (month_idx // 12)
        month = (month_idx % 12) + 1

        print(f"[{year}-{month:02d}] Generating {target_plays:,} plays...")

        season_mood_map = SEASON_MOOD.get(month, {})

        # Source weights
        is_late = (year == 2025 and month >= 7)
        sources_pool = SOURCES_LATE if is_late else SOURCES_EARLY

        # at_risk & churned chỉ active trong 2024
        active_segs = list(seg_users.keys())
        if year >= 2025 and month >= 10:
            active_segs = [s for s in active_segs if s not in ("at_risk", "churned")]

        # Weight theo segment
        SEG_WEIGHT = {
            "heavy": 8.0, "regular": 4.0, "casual": 2.0,
            "occasional": 0.6, "at_risk": 0.4, "churned": 0.1
        }

        user_pool = []
        user_wts = []
        for seg in active_segs:
            w = SEG_WEIGHT.get(seg, 1.0)
            for uid in seg_users.get(seg, []):
                user_pool.append(uid)
                user_wts.append(w)

        if not user_pool:
            continue

        # Pre-sample
        users_sample = random.choices(user_pool, weights=user_wts, k=target_plays)
        songs_sample = weighted_song_sample(song_weights, season_mood_map, song_mood,
                                           k=target_plays, dom_dom_id=dom_dom_id)

        # Sample ngày theo trọng số thứ trong tuần
        _, days_in_month = calendar.monthrange(year, month)
        all_days = []
        for d in range(1, days_in_month + 1):
            dow = datetime(year, month, d).weekday()
            all_days.append((d, DAY_WEIGHTS[dow]))

        day_vals = [d for d, _ in all_days]
        day_wts = [w for _, w in all_days]
        days_sample = random.choices(day_vals, weights=day_wts, k=target_plays)

        for i in range(target_plays):
            user_id = users_sample[i]
            song_id = songs_sample[i]
            day = days_sample[i]

            hour = rand_hour()
            minute = random.randint(0, 59)
            second = random.randint(0, 59)

            played_at = datetime(year, month, day, hour, minute, second)

            segment = user_segs.get(user_id, "casual")
            outcome = pick_outcome(segment)
            dur = song_dur_map.get(song_id, 210)
            dur_play = gen_duration(dur, outcome)

            is_completed = (outcome == "completed")
            is_skipped = (outcome == "skipped")

            source = random.choice(sources_pool)
            device = random.choice(DEVICES)

            batch.append((
                user_id, song_id, played_at, dur_play,
                is_completed, is_skipped, source, device
            ))

            if len(batch) >= BATCH_SIZE:
                flush()

        if batch:
            flush()

    # Update play_count cho bảng songs
    print("Updating songs.play_count ...")
    cur.execute("""
        UPDATE songs s 
        SET play_count = sub.cnt
        FROM (
            SELECT song_id, COUNT(*) as cnt 
            FROM play_history 
            GROUP BY song_id
        ) sub 
        WHERE s.song_id = sub.song_id
    """)
    conn.commit()

    cur.close()
    conn.close()
    print(f"\n[seed_play_history] ✓ HOÀN THÀNH! Total plays inserted: {total_inserted:,}")

if __name__ == "__main__":
    seed_play_history()