import psycopg2
from settings import *

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)

cursor = conn.cursor()

create_table_sql = """
CREATE TABLE IF NOT EXISTS amazon_sales (
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    discounted_price DOUBLE PRECISION,
    actual_price DOUBLE PRECISION,
    discount_percentage DOUBLE PRECISION,
    rating DOUBLE PRECISION,
    rating_count INTEGER,
    about_product TEXT,
    user_id TEXT,
    user_name TEXT,
    review_id TEXT,
    review_title TEXT,
    review_content TEXT,
    img_link TEXT,
    product_link TEXT
);
"""

cursor.execute(create_table_sql)
conn.commit()

print("Table amazon_sales created!")
cursor.close()
conn.close()
