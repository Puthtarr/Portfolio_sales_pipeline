# Sales Pipeline
this is my portfolio project for Data Engineer
The project is about how can i make a data pipeline since extract data on kaggle, clean it and put it to the datawarehouse (local) then use Looker Studio to create visualization

**RAW ➜ CLEAN ➜ VALIDATE ➜ CURATED ➜ IMPORT ➜ BI/Analytics**

Step 1. Download Dataset From kaggle 
LINK >> https://www.kaggle.com/code/mehakiftikhar/amazon-sales-dataset-eda
use local as data lake (Create Folder code in settings.py)

Step 2. Clean Data with PySpark
Clear Data type, Null, Dupplicate values and convert spark to pandas for write to CSV (because my OS cannot nativeIO then it can't write parquet file)
and Save CSV to Local

Step 3. 