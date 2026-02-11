import os
import psycopg2

DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = int(os.environ.get("DB_PORT", 5432))

try:
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode="require"   # <- important for Supabase
    )
    print("✅ DB connection successful!", flush=True)
    conn.close()
except Exception as e:
    print(f"❌ DB connection failed: {e}", flush=True)
    raise
