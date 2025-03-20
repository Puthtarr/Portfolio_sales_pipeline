from google.cloud import bigquery
import os
from settings import *

# Fix path to key
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_account.json'

# Create Client
client = bigquery.Client()

# Set clean_dataset and Table name
project_id = 'portfolio-sales-pipeline'
dataset_id = 'sales_dataset'
table_id = 'amazon_sales'

# Check Dataset if not exist -> Create New
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
try:
    client.get_dataset(dataset_ref)
    print(f"Dataset '{dataset_id}' already exists.")
except Exception as e:
    print(f"⚠Dataset '{dataset_id}' not found. Creating new dataset...")
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"  # เปลี่ยนเป็น "asia-southeast1" ได้ถ้าใช้งานในไทย
    dataset = client.create_dataset(dataset)
    print(f"Created dataset '{dataset.dataset_id}' successfully.")

# Set Table Reference
table_ref = dataset_ref.table(table_id)

file_path = DATA_LAKE_CURATED_PATH + 'Clean_data.csv'
print(f"Preparing to upload file: {file_path} ➜ BigQuery table: {dataset_id}.{table_id}")

try:
    with open(file_path, "rb") as source_file:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # ลบของเก่า อัปเดตใหม่
        )

        load_job = client.load_table_from_file(
            source_file, table_ref, job_config=job_config
        )

        print("Starting load job...")
        load_job.result()  # รอจนงานเสร็จสมบูรณ์
        print("Data loaded successfully!")

except Exception as e:
    print(f"Load job failed: {e}")

# Check Row
try:
    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {dataset_id}.{table_id}.")
except Exception as e:
    print(f"Error fetching table info: {e}")