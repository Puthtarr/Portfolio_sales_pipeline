# 📦 Amazon Sales Data Pipeline Project

This is a **portfolio project** for the **Data Engineer** role, demonstrating how to build a complete **Data Pipeline** from data extraction to data warehousing and visualization.

---

## 🚀 Project Overview
The project simulates a real-world **ETL (Extract, Transform, Load)** process, focusing on:

1. **Extract** data from Kaggle  
2. **Clean & Transform** data with PySpark  
3. **Validate & Curate** data before storage  
4. **Import** curated data into a local **PostgreSQL Data Warehouse**  
5. **Visualize** insights using **Looker Studio**

---

## 🏗️ Pipeline Architecture
RAW ➜ CLEAN ➜ VALIDATE ➜ CURATED ➜ IMPORT ➜ BI/Analytics

- **Raw Layer** : Raw data downloaded from Kaggle  
- **Processed Layer** : Cleaned and validated data  
- **Curated Layer** : Final data ready for Data Warehouse and BI tools 

---

## 🛠️ Tech Stack

| Tool           | Purpose                      |
|----------------|------------------------------|
| Python         | Main programming language    |
| PySpark        | Data cleaning & processing   |
| PostgreSQL     | Local Data Warehouse         |
| pgAdmin        | Database Management GUI      |
| Looker Studio  | BI & Visualization           |
| Git & GitHub   | Version Control              |

---

## 🔧 Steps & How to Run
### Step 1: Extract Data from Kaggle

- Download Dataset  
  ➡️ [Kaggle Dataset Link](https://www.kaggle.com/code/mehakiftikhar/amazon-sales-dataset-eda)  
- Dataset is saved into `data_lake/raw`  
- **Run the script**:  
```bash```
python kaggle.py

### Step 2: Clean Data with PySpark
- Clean nulls, incorrect data types, and duplicates
- Convert Spark ➜ Pandas ➜ CSV (Parquet can't be used on Windows NativeIO)
- **Run the script**:  
```bash```
python clean_data.py

### Step 3: Validate & Curate
- Validate schema, data ranges (discount, rating), and uniqueness
- Export validated data to data_lake/curated
- **Run the script**:  
```bash```
python validate_data.py

### Step 4: Setup PostgreSQL Data Warehouse
Create sales_dwh Database
- **Run the script**:  
```bash```
python setup_database.py

### Step 5: Create Table in PostgreSQL
- Create amazon_sales Table in PostgreSQL
- **Run the script**:  
```bash```
python create_table.py


### Step 6: Import Curated Data into PostgreSQL
- Load curated data into PostgreSQL
- **Run the script**:  
```bash```
python import_clean_data.py

### Step 7: Load Curated Data into BigQuery (Optional Cloud)
- Load curated data (Clean_data.csv) into BigQuery
- **Run the script**: 
```bash```
python load_clean_data_to_bigquery.py


### Step 8: BI & Analytics Dashboard (Looker Studio)
- Connect to PostgreSQL or BigQuery in Looker Studio
- Create interactive dashboards to visualize:
- - Sales Performance
- - Discounts Analysis
- - Customer Ratings
- - Product Categories
- - Top Products by Revenue
- Example filters:
- - Date Range
- - Category
- - Price Range

## 📊 Live Dashboard (Looker Studio)
[👉 Click to View Dashboard](https://lookerstudio.google.com/reporting/xxxxxxxxxxxx)

This dashboard visualizes Amazon sales data with interactive filters and KPIs.

✅ Requirements
- pyspark==3.5.1
- pandas
- psycopg2-binary
- python-dotenv
- kagglehub

🙌 Author
Puthtarr | GitHub