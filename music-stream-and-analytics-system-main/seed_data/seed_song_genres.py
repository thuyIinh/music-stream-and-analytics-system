"""
FILE 07: seed_song_genres.py
Seed bảng song_genres:
  40% bài: 1 genre  |  45% bài: 2 genre  |  15% bài: 3 genre
Combination hợp lý theo genre chính.
Chạy: python seed_07_song_genres.py
"""

import sys, os, random
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn

random.seed(45)

# Cặp genre hợp lý: genre_chính → [genre_phụ có thể kết hợp]
COMPATIBLE = {
    "Pop":         ["Dance", "R&B", "Soul"],
    "V-Pop":       ["R&B", "Acoustic", "Pop"],
    "K-Pop":       ["Dance", "Electronic", "Pop"],
    "Rock":        ["Alternative", "Indie", "Metal"],
    "Electronic":  ["Dance", "Ambient", "Lo-fi"],
    "R&B":         ["Soul", "Hip-hop", "Pop"],
    "Hip-hop":     ["Funk", "R&B", "Electronic"],
    "Acoustic":    ["Indie", "Folk", "Alternative"],
    "Lo-fi":       ["Ambient", "Jazz", "Acoustic"],
    "Dance":       ["Electronic", "Pop", "Funk"],
    "Jazz":        ["Blues", "Soul", "Funk"],
    "Indie":       ["Alternative", "Acoustic", "Rock"],
    "Alternative": ["Rock", "Indie", "Electronic"],
    "Blues":       ["Jazz", "Soul", "Rock"],
    "Soul":        ["R&B", "Gospel", "Blues"],
    "J-Pop":       ["Pop", "Electronic"],
    "Latin":       ["Dance", "Pop"],
    "Country":     ["Acoustic", "Rock"],
    "Metal":       ["Rock", "Alternative"],
    "Funk":        ["R&B", "Soul", "Dance"],
    "Ambient":     ["Lo-fi", "Electronic"],
    "Gospel":      ["Soul", "R&B"],
    "Classical":   ["Ambient"],
    "Reggae":      ["World Music"],
    "World Music": ["Acoustic"],
}

def seed_song_genres():
    conn = get_conn()
    cur  = conn.cursor()

    # Lấy genre_id map
    cur.execute("SELECT id, name FROM genres")
    genre_map = {name: gid for gid, name in cur.fetchall()}

    # Lấy songs với genre chính
    cur.execute("SELECT song_id, genre_id FROM songs ORDER BY song_id")
    songs = cur.fetchall()

    # Reverse map: genre_id → name
    gid_to_name = {v: k for k, v in genre_map.items()}

    batch = []
    for song_id, main_genre_id in songs:
        r = random.random()
        if r < 0.40:
            n_extra = 0
        elif r < 0.85:   # 40+45
            n_extra = 1
        else:
            n_extra = 2

        # Luôn thêm genre chính
        genres_for_song = set()
        if main_genre_id:
            genres_for_song.add(main_genre_id)

        if n_extra > 0:
            main_name = gid_to_name.get(main_genre_id, "Pop")
            candidates = COMPATIBLE.get(main_name, [])
            # Filter candidates tồn tại trong DB
            valid = [genre_map[g] for g in candidates if g in genre_map]
            random.shuffle(valid)
            for gid in valid[:n_extra]:
                genres_for_song.add(gid)

        for gid in genres_for_song:
            batch.append((song_id, gid))

    # Insert batch, bỏ qua duplicate
    inserted = 0
    chunk = 500
    for i in range(0, len(batch), chunk):
        cur.executemany(
            """
            INSERT INTO song_genres (song_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            batch[i:i+chunk],
        )
        inserted += cur.rowcount
        conn.commit()

    cur.close()
    conn.close()
    print(f"[seed_song_genres] ✓ inserted={inserted} / {len(batch)} pairs")

if __name__ == "__main__":
    seed_song_genres()
