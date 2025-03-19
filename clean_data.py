from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, expr, split, explode
from settings import *

# Create Spark Session
spark = SparkSession.builder.appName("Amazon Sales Data Cleansing").getOrCreate()

# Check Spark Version
print(f"Spark version: {spark.version}")

# Load Raw Dataset
raw_data_path = 'data_lake/raw/karkavelrajaj-amazon-sales-dataset/amazon.csv'
df_raw = spark.read.csv(raw_data_path, header=True, inferSchema=True)

# Check Schema
df_raw.printSchema()
df_raw.show(5)

# STEP 1: Clean numeric fields
df_clean = df_raw.withColumn('discounted_price', regexp_replace(col('discounted_price'), '₹|,', '').cast('double')) \
    .withColumn('actual_price', regexp_replace(col('actual_price'), '₹|,', '').cast('double')) \
    .withColumn('rating', col('rating').cast('double')) \
    .withColumn('rating_count', regexp_replace(col('rating_count'), ',', '').cast('int')) \
    .withColumn('discount_percentage', regexp_replace(col('discount_percentage'), '%', '').cast('double'))

# STEP 2: Clean text fields (Trim)
df_clean = df_clean.withColumn('product_name', trim(col('product_name'))) \
    .withColumn('category', trim(col('category'))) \
    .withColumn('review_title', trim(col('review_title'))) \
    .withColumn('review_content', trim(col('review_content')))

# STEP 3: Remove rows with null product_id / product_name
df_clean = df_clean.dropna(subset=['product_id', 'product_name'])

# STEP 4: Remove rows with null discounted_price / actual_price
df_clean = df_clean.dropna(subset=['discounted_price', 'actual_price'])

# STEP 5: Filter value ranges
df_clean = df_clean.filter((col('rating') >= 0) & (col('rating') <= 5))
df_clean = df_clean.filter((col('discount_percentage') >= 0) & (col('discount_percentage') <= 100))

# STEP 6: Business Logic - discounted_price < actual_price
df_clean = df_clean.filter(col('discounted_price') < col('actual_price'))

# STEP 7: Add discount_value column
df_clean = df_clean.withColumn('discount_value', expr('actual_price - discounted_price'))

# STEP 8: Explode review_id ถ้ามี comma
df_clean = df_clean.withColumn('review_id', split(col('review_id'), ',')) \
    .withColumn('review_id', explode(col('review_id')))

# STEP 9: Remove duplicate review_id
df_clean = df_clean.dropDuplicates(['review_id'])

# STEP 10: Final Schema & Preview Clean Data
df_clean.printSchema()

df_clean.select(
    'product_id', 'product_name', 'category',
    'actual_price', 'discounted_price', 'discount_value',
    'discount_percentage', 'rating', 'rating_count'
).show(10, truncate=False)

# STEP 11: Convert to Pandas ➜ Save CSV (For window)
df_pandas = df_clean.toPandas()

output_path = DATA_LAKE_PROCESSED_PATH
clean_filename = 'Clean_data.csv'
df_pandas.to_csv(output_path + clean_filename, index=False)

print(f"Cleaned CSV saved to: {output_path}{clean_filename}")
spark.stop()
