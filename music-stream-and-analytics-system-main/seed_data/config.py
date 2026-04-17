import psycopg2

DB_CONFIG = {
    "host":     "aws-1-ap-southeast-1.pooler.supabase.com",  # Session Pooler
    "port":     5432,                                         # port 5432, KHÔNG phải 6543
    "dbname":   "postgres",
    "user":     "postgres.mjmeexzmkzfgrdzvpadd",              # giữ nguyên
    "password": "quanghuy238",
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)