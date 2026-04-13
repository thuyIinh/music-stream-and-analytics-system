"""
FILE 02: seed_artists.py
Seed 100 nghệ sĩ với phân phối:
  - Vietnam 30 | USA 30 | Korea 15 | UK 10 | Japan 5 | France 4 | Others 6
  - Tier A(10) follower 50k-100k | B(20) 10k-50k | C(30) 1k-10k | D(40) 100-1k
Chạy: python seed_02_artists.py
"""

import sys, os, random
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn
from datetime import datetime, timedelta

random.seed(42)

# ---------------------------------------------------------------------------
# Dữ liệu nghệ sĩ: (name, country, primary_genre_hint)
# ---------------------------------------------------------------------------
ARTISTS_VN = [
    ("Sơn Tùng M-TP",   "Vietnam", "V-Pop"),
    ("Mỹ Tâm",          "Vietnam", "V-Pop"),
    ("Đen Vâu",         "Vietnam", "Hip-hop"),
    ("Hoàng Thùy Linh", "Vietnam", "V-Pop"),
    ("Tlinh",           "Vietnam", "Hip-hop"),
    ("Bích Phương",     "Vietnam", "V-Pop"),
    ("Trúc Nhân",       "Vietnam", "V-Pop"),
    ("Vũ Cát Tường",    "Vietnam", "V-Pop"),
    ("Noo Phước Thịnh", "Vietnam", "V-Pop"),
    ("Dương Hoàng Yến", "Vietnam", "V-Pop"),
    ("HIEUTHUHAI",      "Vietnam", "Hip-hop"),
    ("tlinh",           "Vietnam", "R&B"),
    ("Obito",           "Vietnam", "R&B"),
    ("Wxrdie",          "Vietnam", "Hip-hop"),
    ("Wren Evans",      "Vietnam", "R&B"),
    ("Thái Đinh",       "Vietnam", "Acoustic"),
    ("Hà Anh Tuấn",     "Vietnam", "V-Pop"),
    ("Phan Mạnh Quỳnh", "Vietnam", "V-Pop"),
    ("Phương Ly",       "Vietnam", "V-Pop"),
    ("Vũ.",             "Vietnam", "Acoustic"),
    ("Ngọt",            "Vietnam", "Indie"),
    ("Chillies",        "Vietnam", "Rock"),
    ("Da LAB",          "Vietnam", "V-Pop"),
    ("Bảo Anh",         "Vietnam", "V-Pop"),
    ("Phí Phương Anh",  "Vietnam", "V-Pop"),
    ("Lou Hoàng",       "Vietnam", "V-Pop"),
    ("Duc Phuc",        "Vietnam", "V-Pop"),
    ("Only C",          "Vietnam", "V-Pop"),
    ("Erik",            "Vietnam", "V-Pop"),
    ("Min",             "Vietnam", "V-Pop"),
]

ARTISTS_US = [
    ("Taylor Swift",    "USA", "Pop"),
    ("Ed Sheeran",      "USA", "Pop"),
    ("Billie Eilish",   "USA", "Alternative"),
    ("Post Malone",     "USA", "Hip-hop"),
    ("Drake",           "USA", "Hip-hop"),
    ("Kendrick Lamar",  "USA", "Hip-hop"),
    ("Olivia Rodrigo",  "USA", "Pop"),
    ("SZA",             "USA", "R&B"),
    ("The Weeknd",      "USA", "R&B"),
    ("Ariana Grande",   "USA", "Pop"),
    ("Bruno Mars",      "USA", "Pop"),
    ("Beyoncé",         "USA", "R&B"),
    ("Dua Lipa",        "USA", "Dance"),
    ("Harry Styles",    "USA", "Pop"),
    ("Morgan Wallen",   "USA", "Country"),
    ("Luke Combs",      "USA", "Country"),
    ("Zac Brown Band",  "USA", "Country"),
    ("John Mayer",      "USA", "Rock"),
    ("Arctic Monkeys",  "USA", "Rock"),
    ("Foo Fighters",    "USA", "Rock"),
    ("Jack Johnson",    "USA", "Acoustic"),
    ("Norah Jones",     "USA", "Jazz"),
    ("Chet Baker",      "USA", "Jazz"),
    ("Kendall Grey",    "USA", "Metal"),
    ("Slipknot",        "USA", "Metal"),
    ("Tyler the Creator", "USA", "Hip-hop"),
    ("Frank Ocean",     "USA", "R&B"),
    ("Khalid",          "USA", "R&B"),
    ("Lizzo",           "USA", "Pop"),
    ("Charlie Puth",    "USA", "Pop"),
]

ARTISTS_KR = [
    ("BTS",             "Korea", "K-Pop"),
    ("BLACKPINK",       "Korea", "K-Pop"),
    ("aespa",           "Korea", "K-Pop"),
    ("NewJeans",        "Korea", "K-Pop"),
    ("IVE",             "Korea", "K-Pop"),
    ("Stray Kids",      "Korea", "K-Pop"),
    ("(G)I-DLE",        "Korea", "K-Pop"),
    ("TWICE",           "Korea", "K-Pop"),
    ("EXO",             "Korea", "K-Pop"),
    ("Red Velvet",      "Korea", "K-Pop"),
    ("NCT 127",         "Korea", "K-Pop"),
    ("SEVENTEEN",       "Korea", "K-Pop"),
    ("IU",              "Korea", "K-Pop"),
    ("Zico",            "Korea", "Hip-hop"),
    ("Dean",            "Korea", "R&B"),
]

ARTISTS_UK = [
    ("Coldplay",        "UK", "Rock"),
    ("Radiohead",       "UK", "Alternative"),
    ("Adele",           "UK", "Pop"),
    ("Sam Smith",       "UK", "Pop"),
    ("The 1975",        "UK", "Indie"),
    ("Arctic Monkeys UK", "UK", "Indie"),
    ("Florence + Machine", "UK", "Alternative"),
    ("Amy Winehouse",   "UK", "Soul"),
    ("James Blake",     "UK", "Electronic"),
    ("FKA twigs",       "UK", "Electronic"),
]

ARTISTS_JP = [
    ("Kenshi Yonezu",   "Japan", "J-Pop"),
    ("Hikaru Utada",    "Japan", "J-Pop"),
    ("Ado",             "Japan", "J-Pop"),
    ("Yoasobi",         "Japan", "J-Pop"),
    ("Yorushika",       "Japan", "Ambient"),
]

ARTISTS_FR = [
    ("Daft Punk",       "France", "Electronic"),
    ("Stromae",         "France", "Electronic"),
    ("Christine and the Queens", "France", "Pop"),
    ("Angèle",          "France", "Pop"),
]

ARTISTS_OTHER = [
    ("Bad Bunny",       "Puerto Rico", "Latin"),
    ("J Balvin",        "Colombia",    "Latin"),
    ("Rosalía",         "Spain",       "Latin"),
    ("Burna Boy",       "Nigeria",     "Afrobeats"),
    ("Wizkid",          "Nigeria",     "Afrobeats"),
    ("Tones and I",     "Australia",   "Pop"),
]

ALL_ARTISTS = (
    ARTISTS_VN + ARTISTS_US + ARTISTS_KR +
    ARTISTS_UK + ARTISTS_JP + ARTISTS_FR + ARTISTS_OTHER
)  # tổng = 30+30+15+10+5+4+6 = 100

# Tên tier theo thứ tự artist (index 0-99)
# Tier A: 10 đầu, B: 20 tiếp, C: 30 tiếp, D: 40 cuối
def get_tier(idx: int) -> str:
    if idx < 10:  return "A"
    if idx < 30:  return "B"
    if idx < 60:  return "C"
    return "D"

FOLLOWER_RANGE = {
    "A": (50_000,  100_000),
    "B": (10_000,   50_000),
    "C": (1_000,    10_000),
    "D": (100,       1_000),
}

BIO_TEMPLATES = [
    "{name} là một trong những nghệ sĩ nổi bật nhất đến từ {country}. "
    "Với phong cách âm nhạc độc đáo, {name} đã chinh phục hàng triệu khán giả trên toàn thế giới. "
    "Các sáng tác của {name} luôn mang đậm dấu ấn cá nhân và cảm xúc chân thật.",

    "{name} bắt đầu sự nghiệp âm nhạc từ rất sớm và nhanh chóng trở thành ngôi sao tại {country}. "
    "Phong cách kết hợp nhiều thể loại đã tạo nên âm thanh riêng biệt không thể nhầm lẫn. "
    "Đến nay {name} đã phát hành nhiều album được đón nhận nồng nhiệt từ người hâm mộ.",

    "Nghệ sĩ {name} đến từ {country} với niềm đam mê âm nhạc từ thuở nhỏ. "
    "Âm nhạc của {name} phản ánh những câu chuyện đời thường và cảm xúc sâu sắc. "
    "Sự nghiệp phát triển mạnh mẽ đã đưa {name} đến nhiều sân khấu lớn trong và ngoài nước.",
]

def rand_date(start_year=2022, end_year=2026):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year,   3, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def seed_artists():
    conn = get_conn()
    cur  = conn.cursor()

    # Load genre name → genre_id
    cur.execute("SELECT id, name FROM genres")
    genre_map = {name: gid for gid, name in cur.fetchall()}

    inserted = 0
    for idx, (name, country, _genre) in enumerate(ALL_ARTISTS):
        tier = get_tier(idx)
        bio  = random.choice(BIO_TEMPLATES).format(name=name, country=country)
        img  = f"/uploads/covers/artist_{idx+1}.jpg"
        created_at = rand_date()

        cur.execute(
            """
            INSERT INTO artists (name, bio, image_url, country, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, true, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (name, bio, img, country, created_at, created_at),
        )
        inserted += cur.rowcount

    conn.commit()
    cur.close()
    conn.close()
    print(f"[seed_artists] ✓ inserted={inserted}")

if __name__ == "__main__":
    seed_artists()
