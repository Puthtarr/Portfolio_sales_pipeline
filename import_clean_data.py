import pandas as pd
from sqlalchemy import create_engine
from settings import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DATA_LAKE_CURATED_PATH
import urllib.parse

curated_file = DATA_LAKE_CURATED_PATH + 'Clean_data.csv'

# Load curated data
df = pd.read_csv(curated_file)

# Encode password
password_encoded = urllib.parse.quote_plus(DB_PASSWORD)

# Connect PostgreSQL
engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
)

try:
    df.to_sql('amazon_sales', con=engine, if_exists='append', index=False)
    print("Imported into PostgreSQL successfully!")
except Exception as e:
    print(f"Import Error: {e}")

engine.dispose()
