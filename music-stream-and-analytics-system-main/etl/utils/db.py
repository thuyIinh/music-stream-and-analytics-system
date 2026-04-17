import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import psycopg2
from config.settings import DB_URL

def get_conn():
    """Tạo và trả về đối tượng kết nối PostgreSQL."""
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối Database: {e}")
        raise