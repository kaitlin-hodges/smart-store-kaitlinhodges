import sqlite3
import pathlib

DB_PATH = pathlib.Path("src/analytics_project/dw/smart_sales.sqlite")

print(f"Connecting to: {DB_PATH.resolve()}")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()

print("\n=== TABLES FOUND ===")
for t in tables:
    print(t)

conn.close()
