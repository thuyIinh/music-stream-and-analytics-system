"""
FILE 10: seed_likes_follows.py (FIXED - followed_at distribution)
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import datetime, timedelta
import psycopg2.extras

random.seed(48)
DIR = os.path.dirname(os.path.abspath(__file__))

SEED_START = datetime(2024, 1, 1)
SEED_END   = datetime(2025, 12, 31)
SEED_RANGE_DAYS = (SEED_END - SEED_START).days

LIKES_PER_SEG = {
    "heavy":      200,
    "regular":     80,
    "casual":      30,
    "occasional":  12,
    "at_risk":      5,
    "churned":      2,
}
FOLLOWS_PER_SEG = {
    "heavy":      20,
    "regular":    12,
    "casual":       6,
    "occasional":   3,
    "at_risk":      1,
    "churned":      1,
}

def to_naive(dt):
    if dt is None:
        return None  # FIX: trả None thay vì SEED_START
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt

def random_fallback_date():
    """Tạo ngày ngẫu nhiên trong khoảng SEED_START → SEED_END có giờ phút giây"""
    days = random.randint(0, SEED_RANGE_DAYS)
    base = SEED_START + timedelta(days=days)
    return base + timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

def load_segments():
    csv_path = os.path.join(DIR, "seeds", "user_segments.csv")
    print(f"DEBUG: Looking for CSV at: {csv_path}")
    print(f"DEBUG: File exists: {os.path.exists(csv_path)}")
    # ← sửa lại thế này
    segs = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                segs[int(row["user_id"])] = row["segment"]
    segs[1] = "heavy"
    segs[2] = "regular"
    segs[3] = "casual"
    return segs

def seed_likes_follows():
    conn = get_conn()
    cur  = conn.cursor()

    user_segs = load_segments()
    print(f"DEBUG: Loaded {len(user_segs)} users from segments")
    print("Loading top songs per user ...")
    cur.execute("""
        SELECT user_id, song_id, COUNT(*) as cnt
        FROM play_history
        GROUP BY user_id, song_id
        ORDER BY user_id, cnt DESC
    """)
    user_top_songs = {}
    for uid, sid, cnt in cur.fetchall():
        if uid not in user_top_songs:
            user_top_songs[uid] = []
        if len(user_top_songs[uid]) < 50:
            user_top_songs[uid].append(sid)

    print("Loading top genres per user ...")
    cur.execute("""
        SELECT ph.user_id, s.genre_id, COUNT(*) as cnt
        FROM play_history ph
        JOIN songs s ON s.song_id = ph.song_id
        WHERE s.genre_id IS NOT NULL
        GROUP BY ph.user_id, s.genre_id
        ORDER BY ph.user_id, cnt DESC
    """)
    user_top_genres = {}
    for uid, gid, cnt in cur.fetchall():
        if uid not in user_top_genres:
            user_top_genres[uid] = []
        if len(user_top_genres[uid]) < 3:
            user_top_genres[uid].append(gid)

    print("Loading hot songs by genre ...")
    cur.execute("""
        SELECT s.song_id, s.genre_id
        FROM play_history ph
        JOIN songs s ON s.song_id = ph.song_id
        WHERE s.genre_id IS NOT NULL
        GROUP BY s.song_id, s.genre_id
        ORDER BY COUNT(*) DESC
        LIMIT 500
    """)
    hot_songs_by_genre = {}
    hot_songs_all      = []
    for sid, gid in cur.fetchall():
        hot_songs_all.append(sid)
        if gid not in hot_songs_by_genre:
            hot_songs_by_genre[gid] = []
        hot_songs_by_genre[gid].append(sid)

    def get_hot_songs_for_user(uid):
        genres = user_top_genres.get(uid, [])
        pool = []
        for gid in genres:
            pool.extend(hot_songs_by_genre.get(gid, []))
        return pool if pool else hot_songs_all

    cur.execute("SELECT song_id FROM songs")
    all_songs = [r[0] for r in cur.fetchall()]

    print("Loading earliest play dates ...")
    cur.execute("""
        SELECT user_id, song_id, MIN(played_at)
        FROM play_history
        GROUP BY user_id, song_id
    """)
    first_play = {}
    for uid, sid, ts in cur.fetchall():
        first_play[(uid, sid)] = to_naive(ts)

    print("Loading hot artists (Tier A+B by follower_count) ...")
    cur.execute("""
        SELECT artist_id FROM artists
        ORDER BY follower_count DESC
        LIMIT 30
    """)
    hot_artists = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT artist_id FROM artists")
    all_artists = [r[0] for r in cur.fetchall()]

    print("Loading top artists per user ...")
    cur.execute("""
        SELECT ph.user_id, s.artist_id, COUNT(*) as cnt
        FROM play_history ph
        JOIN songs s ON s.song_id = ph.song_id
        GROUP BY ph.user_id, s.artist_id
        ORDER BY ph.user_id, cnt DESC
    """)
    user_top_artists = {}
    for uid, aid, cnt in cur.fetchall():
        if uid not in user_top_artists:
            user_top_artists[uid] = []
        if len(user_top_artists[uid]) < 20:
            user_top_artists[uid].append(aid)

    print("Loading user created_at ...")
    cur.execute("SELECT user_id, created_at FROM users")
    user_created_at = {uid: to_naive(ts) for uid, ts in cur.fetchall()}

    # ----------------------------------------------------------------
    # LIKES
    # ----------------------------------------------------------------
    print("Generating likes ...")
    like_batch = []
    seen_likes = set()

    for uid, seg in user_segs.items():
        target = LIKES_PER_SEG.get(seg, 5)
        target = max(1, int(target * random.uniform(0.7, 1.3)))

        top_songs      = user_top_songs.get(uid, [])
        hot_songs_user = get_hot_songs_for_user(uid)
        chosen         = set()

        for _ in range(target * 4):
            if len(chosen) >= target:
                break

            r = random.random()
            if r < 0.70 and top_songs:
                sid = random.choice(top_songs)
            elif r < 0.90 and hot_songs_user:
                sid = random.choice(hot_songs_user)
            else:
                sid = random.choice(all_songs)

            if sid in chosen or (uid, sid) in seen_likes:
                continue
            chosen.add(sid)
            seen_likes.add((uid, sid))

            fp = first_play.get((uid, sid))

            if fp and fp <= SEED_END:
                max_days = min(30, (SEED_END - fp).days)  # Không cho vượt quá SEED_END

                liked_at = fp + timedelta(
                    days=random.randint(0, max_days),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )

                # Bảo hiểm lần cuối
                if liked_at > SEED_END:
                    liked_at = SEED_END - timedelta(seconds=random.randint(0, 3600 * 24))

            else:
                # fallback
                liked_at = random_fallback_date()

            like_batch.append((uid, sid, liked_at))

    like_ins = 0
    for i in range(0, len(like_batch), 2000):
        rows = like_batch[i:i+2000]
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO likes (user_id, song_id, liked_at) VALUES %s ON CONFLICT DO NOTHING",
            rows,
        )
        like_ins += cur.rowcount
        conn.commit()

    print(f"  likes inserted={like_ins}")

    # ----------------------------------------------------------------
    # FOLLOWS
    # ----------------------------------------------------------------
    print("Generating follows ...")
    follow_batch = []
    seen_follows = set()

    for uid, seg in user_segs.items():
        target = FOLLOWS_PER_SEG.get(seg, 1)
        target = max(1, int(target * random.uniform(0.7, 1.3)))

        top_arts = user_top_artists.get(uid, [])
        chosen   = set()

        # FIX: xử lý u_created đúng cách
        u_created = user_created_at.get(uid)
        if u_created is None or u_created >= SEED_END:
            u_created = SEED_START + timedelta(days=random.randint(0, 365))
        u_created = max(u_created, SEED_START)  # không trước 2024
        follow_range_days = max(30, (SEED_END - u_created).days)  # tối thiểu 30 ngày

        for _ in range(target * 4):
            if len(chosen) >= target:
                break

            r = random.random()
            if r < 0.60 and top_arts:
                aid = random.choice(top_arts)
            elif r < 0.90 and hot_artists:
                aid = random.choice(hot_artists)
            else:
                aid = random.choice(all_artists)

            if aid in chosen or (uid, aid) in seen_follows:
                continue
            chosen.add(aid)
            seen_follows.add((uid, aid))

            days_offset = random.randint(0, follow_range_days)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            seconds = random.randint(0, 59)

            followed_at = u_created + timedelta(
                days=days_offset,
                hours=hours,
                minutes=minutes,
                seconds=seconds
            )

            # Đảm bảo không vượt SEED_END
            if followed_at > SEED_END:
                followed_at = SEED_END - timedelta(seconds=random.randint(0, 86400))

            follow_batch.append((uid, aid, followed_at))

    follow_ins = 0
    for i in range(0, len(follow_batch), 2000):
        rows = follow_batch[i:i+2000]
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO follows (user_id, artist_id, followed_at) VALUES %s ON CONFLICT DO NOTHING",
            rows,
        )
        follow_ins += cur.rowcount
        conn.commit()

    print(f"  follows inserted={follow_ins}")

    # Update like_count
    cur.execute("""
        UPDATE songs s SET like_count = sub.cnt
        FROM (
            SELECT song_id, COUNT(*) as cnt
            FROM likes
            GROUP BY song_id
        ) sub
        WHERE s.song_id = sub.song_id
    """)
    conn.commit()

    cur.close()
    conn.close()
    print(f"[seed_likes_follows] ✓ likes={like_ins}  follows={follow_ins}")

if __name__ == "__main__":
    seed_likes_follows()