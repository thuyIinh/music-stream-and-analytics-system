"""
FILE 11: seed_playlists.py
Seed 1,000 playlists + 15,000 playlist_songs.
Tên playlist có pattern thực tế.
80% bài từ play_history user, 20% random trong genre yêu thích.
Chạy: python seed_11_playlists.py
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import datetime, timedelta
import psycopg2.extras

random.seed(49)
DIR = os.path.dirname(os.path.abspath(__file__))

PLAYLIST_PER_SEG = {
    "heavy":      6,
    "regular":    3,
    "casual":     2,
    "occasional": 1,   # 50% chance
    "at_risk":    0,
    "churned":    0,
}

NAME_TEMPLATES = [
    "Nhạc buổi sáng",
    "Tiếng mẹ chửi cực chill",
    "Chill thôi",
    "Workout playlist",
    "Top {genre} của tao",
    "Nhạc học bài",
    "Driving mix",
    "Nhạc mưa",
    "Late night vibes",
    "Best of {year}",
    "Nhạc yêu thích",
    "Party time",
    "Relaxing Sunday",
    "My favorites",
    "{mood} mood",
    "Playlist #{number}",
    "Những bài hay nhất",
    "Nghe mãi không chán",
    "Tâm trạng hôm nay",
    "Repeat liên tục",
]

MOODS = ["happy", "chill", "sad", "energetic", "romantic", "calm"]
GENRES_LIST = ["V-Pop", "Pop", "K-Pop", "Rock", "Electronic", "R&B", "Acoustic", "Hip-hop", "Lo-fi"]

def rand_name(n):
    tmpl = random.choice(NAME_TEMPLATES)
    return tmpl.format(
        genre=random.choice(GENRES_LIST),
        year=random.randint(2022, 2025),
        mood=random.choice(MOODS),
        number=n,
    )[:255]

def seed_playlists():
    conn = get_conn()
    cur  = conn.cursor()

    # Load segments
    csv_path = os.path.join(DIR, "user_segments.csv")
    user_segs = {}
    if os.path.exists(csv_path):
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                user_segs[int(row["user_id"])] = row["segment"]
    user_segs[1] = "heavy"
    user_segs[2] = "regular"
    user_segs[3] = "casual"

    # Top songs per user (từ play_history)
    print("Loading user top songs ...")
    cur.execute("""
        SELECT user_id, song_id FROM (
            SELECT user_id, song_id,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) rn
            FROM play_history GROUP BY user_id, song_id
        ) sub WHERE rn <= 80
    """)
    user_top = {}
    for uid, sid in cur.fetchall():
        user_top.setdefault(uid, []).append(sid)

    # Songs per genre
    cur.execute("SELECT s.song_id, g.name FROM songs s JOIN genres g ON g.id=s.genre_id")
    genre_songs = {}
    for sid, gname in cur.fetchall():
        genre_songs.setdefault(gname, []).append(sid)
    all_songs = list({sid for sids in genre_songs.values() for sid in sids})

    # created_at reference per user
    cur.execute("SELECT user_id, created_at FROM users")
    user_created = {r[0]: r[1] for r in cur.fetchall()}

    pl_inserted  = 0
    ps_inserted  = 0
    playlist_num = 0

    for uid, seg in user_segs.items():
        n_pl = PLAYLIST_PER_SEG.get(seg, 0)
        if seg == "occasional":
            if random.random() > 0.5:
                n_pl = 0

        for _ in range(n_pl):
            playlist_num += 1
            name = rand_name(playlist_num)

            base_created = user_created.get(uid, datetime(2024, 1, 1))
            days_offset  = random.randint(0, 300)
            created_at   = base_created + timedelta(days=days_offset)

            is_public = random.random() < 0.4

            # Số bài
            r = random.random()
            if r < 0.20:
                n_songs = random.randint(5, 8)
            elif r < 0.60:
                n_songs = random.randint(9, 15)
            elif r < 0.90:
                n_songs = random.randint(16, 25)
            else:
                n_songs = random.randint(26, 50)

            # Insert playlist
            cur.execute(
                """
                INSERT INTO playlists
                    (user_id, name, is_public, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING playlist_id
                """,
                (uid, name, is_public, created_at, created_at),
            )
            pl_id = cur.fetchone()[0]
            pl_inserted += 1

            # Chọn bài
            top = user_top.get(uid, [])
            chosen = []
            seen   = set()

            for _ in range(n_songs * 3):
                if len(chosen) >= n_songs:
                    break
                if random.random() < 0.80 and top:
                    sid = random.choice(top)
                else:
                    sid = random.choice(all_songs)
                if sid not in seen:
                    seen.add(sid)
                    chosen.append(sid)

            # Insert playlist_songs
            song_rows = []
            for pos, sid in enumerate(chosen, start=1):
                added_at = created_at + timedelta(days=random.randint(0, 30))
                song_rows.append((pl_id, sid, pos, added_at))

            if song_rows:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO playlist_songs (playlist_id, song_id, position, added_at)
                    VALUES %s ON CONFLICT DO NOTHING
                    """,
                    song_rows,
                )
                ps_inserted += cur.rowcount

            # Update total_songs + total_duration trên playlist
            cur.execute("""
                UPDATE playlists SET
                    total_songs    = (SELECT COUNT(*) FROM playlist_songs WHERE playlist_id=%s),
                    total_duration = (SELECT COALESCE(SUM(s.duration),0)
                                      FROM playlist_songs ps JOIN songs s ON s.song_id=ps.song_id
                                      WHERE ps.playlist_id=%s)
                WHERE playlist_id=%s
            """, (pl_id, pl_id, pl_id))

        if pl_inserted % 100 == 0 and pl_inserted > 0:
            conn.commit()
            print(f"  ... playlists={pl_inserted}  songs={ps_inserted}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"[seed_playlists] ✓ playlists={pl_inserted}  playlist_songs={ps_inserted}")

if __name__ == "__main__":
    seed_playlists()
