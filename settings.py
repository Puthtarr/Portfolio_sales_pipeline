import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Config
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Directory Paths
BASE_DIR = os.getcwd() + os.sep
DATA_LAKE_PATH = os.path.join(BASE_DIR, 'data_lake')
DATA_LAKE_RAW_PATH = os.path.join(DATA_LAKE_PATH, 'raw') + os.sep
DATA_LAKE_PROCESSED_PATH = os.path.join(DATA_LAKE_PATH, 'processed') + os.sep
DATA_LAKE_CURATED_PATH = os.path.join(DATA_LAKE_PATH, 'curated') + os.sep

# Optional: Function สำหรับ setup folder structure
def create_data_lake_structure():
    folders = ['raw', 'processed', 'curated']
    for fol in folders:
        path = os.path.join(DATA_LAKE_PATH, fol)
        os.makedirs(path, exist_ok=True)