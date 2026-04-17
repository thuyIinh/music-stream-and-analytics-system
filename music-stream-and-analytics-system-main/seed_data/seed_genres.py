"""
FILE 01: seed_genres.py
Seed 25 genres vào bảng genres.
Chạy: python seed_01_genres.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seed_data.config import get_conn

GENRES = [
    ("Pop",         "Nhạc đại chúng, giai điệu bắt tai, dễ nghe và phổ biến toàn cầu."),
    ("Rock",        "Guitar điện, trống mạnh, năng lượng cao và mạnh mẽ."),
    ("Jazz",        "Ứng tấu, phức tạp về hòa âm, nguồn gốc từ New Orleans đầu thế kỷ 20."),
    ("Electronic",  "Nhạc máy tính, synthesizer, sản xuất hoàn toàn bằng thiết bị điện tử."),
    ("Acoustic",    "Nhạc mộc, không khuếch đại điện tử, âm thanh tự nhiên và gần gũi."),
    ("R&B",         "Rhythm and blues, soul hiện đại, kết hợp giữa nhịp điệu và giai điệu cảm xúc."),
    ("Hip-hop",     "Rap, beat nặng, văn hóa đường phố và lời ca kể chuyện."),
    ("Classical",   "Nhạc cổ điển phương Tây, sáng tác từ thế kỷ 17-19, biểu diễn bằng nhạc cụ truyền thống."),
    ("Country",     "Nhạc đồng quê Mỹ, guitar thùng, lời ca về cuộc sống nông thôn và tình yêu."),
    ("Metal",       "Rock nặng, guitar méo, tiết tấu nhanh và âm lượng lớn."),
    ("Indie",       "Nhạc độc lập, không thuộc hãng đĩa lớn, phong cách đa dạng và thực nghiệm."),
    ("Blues",       "Gốc rễ nhạc Mỹ, xuất phát từ cộng đồng người Mỹ gốc Phi, cảm xúc chân thật."),
    ("Reggae",      "Nhạc Jamaica, nhịp offbeat đặc trưng, lời ca về hòa bình và tự do."),
    ("Soul",        "Cảm xúc sâu sắc, gốc R&B cổ, giọng hát nội tâm và mạnh mẽ."),
    ("J-Pop",       "Nhạc pop Nhật Bản, kết hợp âm nhạc phương Tây với văn hóa Nhật Bản độc đáo."),
    ("Latin",       "Nhạc Latin Mỹ, tiết tấu sôi động, bao gồm salsa, reggaeton và bossa nova."),
    ("K-Pop",       "Nhạc pop Hàn Quốc, sản xuất chuyên nghiệp, kết hợp múa và hình ảnh bắt mắt."),
    ("V-Pop",       "Nhạc pop Việt Nam, kết hợp âm nhạc hiện đại với bản sắc văn hóa Việt."),
    ("Dance",       "Nhạc sàn, BPM cao, thiết kế để nhảy múa tại các câu lạc bộ và lễ hội."),
    ("Alternative", "Ngoài mainstream, phong cách thực nghiệm và không theo khuôn mẫu thương mại."),
    ("Lo-fi",       "Chất lượng âm thanh thấp cố ý, chill và thư giãn, phổ biến khi học hoặc làm việc."),
    ("Ambient",     "Nhạc nền, thiền định, tạo không gian âm thanh thay vì giai điệu rõ ràng."),
    ("Funk",        "Groove, bass nổi bật, tiết tấu syncopated và năng lượng vũ trường."),
    ("Gospel",      "Nhạc tôn giáo Cơ Đốc, hợp xướng mạnh mẽ, lời ca ngợi ca và đức tin."),
    ("World Music", "Nhạc dân tộc toàn cầu, kết hợp các nền văn hóa âm nhạc truyền thống từ khắp thế giới."),
]

def seed_genres():
    conn = get_conn()
    cur  = conn.cursor()

    inserted = 0
    skipped  = 0

    for name, description in GENRES:
        cur.execute(
            """
            INSERT INTO genres (name, description)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING
            """,
            (name, description),
        )
        if cur.rowcount:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"[seed_genres] ✓ inserted={inserted}  skipped={skipped}")

if __name__ == "__main__":
    seed_genres()
