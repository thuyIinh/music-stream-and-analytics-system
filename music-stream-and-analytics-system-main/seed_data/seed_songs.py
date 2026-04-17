"""
FILE 06: seed_songs.py
Seed 1,000 bài hát với phân phối genre/mood/language/duration/hot_weight.
hot_weight KHÔNG lưu vào DB — lưu ra file seeds/song_weights.csv để seed_09 dùng.
Bài "Đom Đóm" (song_id sẽ được ghi vào song_weights.csv đặc biệt) dùng cho demo play.
Chạy: python seed_06_songs.py
"""

import sys, os, random, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import date, timedelta

random.seed(44)

DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Phân phối genre theo số bài (tổng = 1000)
# ---------------------------------------------------------------------------
GENRE_DIST = {
    "V-Pop":       180,
    "Pop":         150,
    "K-Pop":       120,
    "Rock":         80,
    "Electronic":   70,
    "R&B":          60,
    "Acoustic":     60,
    "Hip-hop":      50,
    "Lo-fi":        50,
    "Dance":        40,
    "Jazz":         30,
    "Indie":        30,
    "Alternative":  25,
    # còn lại 55 chia đều cho các genre còn lại
    "Blues":         8,
    "Reggae":        6,
    "Soul":          8,
    "J-Pop":         8,
    "Latin":         7,
    "Country":       6,
    "Metal":         5,
    "Funk":          4,
    "Ambient":       3,
}

# ---------------------------------------------------------------------------
# Phân phối mood (tổng = 1000)
# ---------------------------------------------------------------------------
MOOD_DIST = {
    "happy":      200,
    "energetic":  180,
    "romantic":   170,
    "calm":       150,
    "sad":        120,
    "aggressive":  80,
    "peaceful":    60,
    "hypnotic":    40,
}

# ---------------------------------------------------------------------------
# Phân phối language
# ---------------------------------------------------------------------------
LANG_DIST = {
    "en": 450,
    "vi": 300,
    "ko": 150,
    "ja":  50,
    "fr":  30,
    "es":  20,
}

# ---------------------------------------------------------------------------
# Phân phối duration (giây)
# ---------------------------------------------------------------------------
DURATION_POOLS = (
    [(random.randint(90,  150)) for _ in range(50)]  +
    [(random.randint(151, 210)) for _ in range(300)] +
    [(random.randint(211, 270)) for _ in range(400)] +
    [(random.randint(271, 330)) for _ in range(180)] +
    [(random.randint(331, 420)) for _ in range(70)]
)
random.shuffle(DURATION_POOLS)

# ---------------------------------------------------------------------------
# release_date pool
# ---------------------------------------------------------------------------
YEAR_POOL = [2022]*100 + [2023]*150 + [2024]*200 + [2025]*300 + [2026]*250
random.shuffle(YEAR_POOL)

def rand_date_in_year(year):
    start = date(year, 1, 1)
    end   = date(year, 3, 31) if year == 2026 else date(year, 12, 31)
    return start + timedelta(days=random.randint(0, (end-start).days))

# ---------------------------------------------------------------------------
# Tên bài hát mẫu
# ---------------------------------------------------------------------------
VI_TITLES = [
    "Đom Đóm","Chúng Ta Của Hiện Tại","Hoa Nở Không Màu","Waiting For You",
    "Có Chắc Yêu Là Đây","Muộn Rồi Mà Sao Còn","Bước Qua Nhau","Ngày Mai",
    "Mắt Nai","Tháng Năm","Người Lạ Ơi","Yêu Một Người Có Lẽ","Nhớ Mãi",
    "Đừng Làm Trái Tim Anh Đau","Cơn Mưa Qua","Kể Tôi Nghe","Lời Hứa",
    "Cho Em Một Lần","Buông","Xa Thôi","Giã Từ","Về Đây","Mình Yêu Nhau Đi",
    "Thương Nhớ","Chờ Em","Từ Ngày Em Đến","Bay","Sao Chưa Thấy Về",
    "Một Cõi Tâm Tư","Anh Ơi Ở Lại","Tìm","Nắng Ấm Xa Dần",
]

EN_TITLES = [
    "Midnight Rain","Blinding Lights","Levitating","Peaches","Stay",
    "Easy On Me","Heat Waves","Montero","Good 4 U","Drivers License",
    "Industry Baby","Shivers","Need To Know","Leave The Door Open",
    "Butter","Permission To Dance","Dynamite","Life Goes On","Savage Love",
    "Watermelon Sugar","Adore You","Golden","Treat People With Kindness",
    "As It Was","Late Night Talking","Satellite","Daylight","Lover",
    "Cardigan","Folklore","Exile","August","Seven","Cruel Summer",
    "Anti-Hero","Bejeweled","Lavender Haze","Karma","Marjorie",
    "Fearless","Love Story","You Belong With Me","Back To December",
    "Safe & Sound","The 1","Illicit Affairs","This Is Me Trying",
    "Champagne Problems","Gold Rush","Cowboy Like Me","Long Live",
]

KO_TITLES = [
    "봄날 (Spring Day)","DNA","FAKE LOVE","Boy With Luv","ON",
    "Dynamite KR","Butter KR","Permission KR","Life Goes On KR",
    "Pink Venom","Shut Down","Yeah Yeah Yeah","Ready For Love",
    "Next Level","Savage","Girls","Drama","Supernova","Whiplash",
    "Hype Boy","Attention","Cookie","OMG","ETA","New Jeans",
    "After LIKE","I AM","Kitsch","Love Dive","Eleven",
]

JA_TITLES = [
    "Lemon","Uchiage Hanabi","Gurenge","Homura","Usseewa",
    "Idol","Specialz","Yoru ni Kakeru","Blessing","Omokage",
    "Pretender","Shizukani Joshi","Kaikai Kitan","Yasashii Suisei",
]

FR_TITLES = [
    "La Vie en Rose","Non, je ne regrette rien","Papaoutai",
    "Te","Tout oublier","Manhattan Kaboul","Je veux","Formidable",
    "Ta fête","Quelqu'un m'a dit","L'Envie d'aimer",
]

ES_TITLES = [
    "Despacito","Taki Taki","MIA","Dakiti","Hawái",
    "DÁKITI","Te Boté","Mayores","Attention","Boom",
    "La Gasolina","X (EQUIS)","Con Calma","China",
    "Callaita","Safaera","Si Veo a Tu Mamá","Porfa",
]

LANG_TITLES = {"en": EN_TITLES, "vi": VI_TITLES, "ko": KO_TITLES,
               "ja": JA_TITLES, "fr": FR_TITLES, "es": ES_TITLES}

def make_title_pool(lang, count):
    base = LANG_TITLES.get(lang, EN_TITLES)
    pool = []
    idx  = 0
    while len(pool) < count:
        pool.append(f"{base[idx % len(base)]} {idx // len(base) + 1}" if idx >= len(base)
                    else base[idx])
        idx += 1
    return pool

# ---------------------------------------------------------------------------
# Build song list
# ---------------------------------------------------------------------------
def build_song_list():
    genres = []
    for g, cnt in GENRE_DIST.items():
        genres += [g] * cnt

    moods = []
    for m, cnt in MOOD_DIST.items():
        moods += [m] * cnt

    langs = []
    for l, cnt in LANG_DIST.items():
        langs += [l] * cnt

    random.shuffle(genres)
    random.shuffle(moods)
    random.shuffle(langs)

    # Pre-generate titles per lang
    title_pools = {}
    for lang, cnt in LANG_DIST.items():
        title_pools[lang] = make_title_pool(lang, cnt + 10)

    lang_counters = {l: 0 for l in LANG_DIST}

    songs = []
    for i in range(1000):
        lang  = langs[i]
        title = title_pools[lang][lang_counters[lang]]
        lang_counters[lang] += 1

        songs.append({
            "title":   title,
            "genre":   genres[i],
            "mood":    moods[i],
            "lang":    lang,
            "duration": DURATION_POOLS[i],
            "year":    YEAR_POOL[i],
        })

    return songs

# hot_weight assignment
def assign_weight(idx):
    if idx < 50:   return 100   # siêu hot
    if idx < 200:  return 30    # khá hot
    if idx < 500:  return 8     # trung bình
    if idx < 800:  return 2     # ít nghe
    return 0.5                  # rất ít

def seed_songs():
    conn = get_conn()
    cur  = conn.cursor()

    # genre_id map
    cur.execute("SELECT id, name FROM genres")
    genre_map = {name: gid for gid, name in cur.fetchall()}

    # artist_id list & tier
    cur.execute("SELECT artist_id FROM artists ORDER BY artist_id")
    artist_ids = [r[0] for r in cur.fetchall()]

    def get_tier(idx):
        if idx < 10: return "A"
        if idx < 30: return "B"
        if idx < 60: return "C"
        return "D"

    SONGS_PER_TIER = {"A": (20, 25), "B": (10, 15), "C": (5, 8), "D": (3, 5)}

    # Gán số bài cho từng artist
    artist_song_counts = []
    total = 0
    for idx, aid in enumerate(artist_ids):
        lo, hi = SONGS_PER_TIER[get_tier(idx)]
        cnt = random.randint(lo, hi)
        artist_song_counts.append((aid, cnt))
        total += cnt

    # Scale về đúng 1000 nếu lệch
    # (chấp nhận ±5% — không cần scale cứng)

    songs_data = build_song_list()

    # Albums: lấy album_id theo artist
    cur.execute("SELECT album_id, artist_id FROM albums ORDER BY artist_id, album_id")
    album_rows = cur.fetchall()
    artist_albums = {}
    for alb_id, art_id in album_rows:
        artist_albums.setdefault(art_id, []).append(alb_id)

    song_idx   = 0
    weights_out = []  # (song_id, hot_weight)

    # Shuffle để weight không theo artist
    weight_order = list(range(1000))
    random.shuffle(weight_order)

    inserted = 0
    for art_idx, (artist_id, count) in enumerate(artist_song_counts):
        albums = artist_albums.get(artist_id, [None])

        for _ in range(count):
            if song_idx >= len(songs_data):
                break
            s = songs_data[song_idx]

            genre_id  = genre_map.get(s["genre"])
            album_id  = random.choice(albums)
            rel_date  = rand_date_in_year(s["year"])
            file_url  = f"/audio/song_{song_idx+1}.mp3"
            cover_url = f"/uploads/covers/song_{song_idx+1}.jpg"

            # Bài đầu tiên (song_idx==0) luôn là "Đom Đóm" để demo
            if song_idx == 0:
                title = "Đom Đóm"
                artist_id_use = artist_ids[0]  # gắn cho Tier A đầu tiên
            else:
                title = s["title"]
                artist_id_use = artist_id

            cur.execute(
                """
                INSERT INTO songs
                    (title, artist_id, album_id, genre_id, duration,
                     file_url, cover_url, mood, language, is_active,
                     release_date, created_at, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s,%s)
                RETURNING song_id
                """,
                (title, artist_id_use, album_id, genre_id, s["duration"],
                 file_url, cover_url, s["mood"], s["lang"],
                 rel_date, rel_date, rel_date),
            )
            row = cur.fetchone()
            if row:
                real_weight = assign_weight(weight_order[song_idx])
                # Bonus weight cho bài mới 2025-2026
                if s["year"] >= 2025:
                    real_weight = real_weight * 1.2
                weights_out.append((row[0], real_weight))
                inserted += 1

            song_idx += 1

        if inserted % 200 == 0:
            conn.commit()

    conn.commit()
    cur.close()
    conn.close()

    # Ghi CSV weights
    csv_path = os.path.join(DIR, "song_weights.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["song_id", "hot_weight"])
        writer.writerows(weights_out)

    print(f"[seed_songs] ✓ inserted={inserted}  weights_csv={csv_path}")

if __name__ == "__main__":
    seed_songs()
