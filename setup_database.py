import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dotenv import load_dotenv
from settings import *

# Debug Check
print(f"DB_NAME: {DB_NAME}")

try:
    conn = psycopg2.connect(
        dbname='postgres',
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # Check Existing Database
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(f"CREATE DATABASE {DB_NAME};")
        print(f"Database '{DB_NAME}' created successfully!")
    else:
        print(f"Database '{DB_NAME}' already exists.")

    cursor.close()
    conn.close()

except Exception as e:
    print("Error:", e)
