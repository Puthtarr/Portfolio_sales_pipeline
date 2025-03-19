import pandas as pd
from settings import DATA_LAKE_PROCESSED_PATH, DATA_LAKE_CURATED_PATH
import shutil

# Load file
csv_file = DATA_LAKE_PROCESSED_PATH + 'Clean_data.csv'
df = pd.read_csv(csv_file)

# Data Validation / Quality Check
validation_passed = True
errors = []

# Drop discount_value column if unnecessary
if 'discount_value' in df.columns:
    df = df.drop(columns=['discount_value'])

# Drop rows with missing discounted_price / actual_price
missing_discounted = df['discounted_price'].isnull().sum()
missing_actual = df['actual_price'].isnull().sum()

if missing_discounted > 0 or missing_actual > 0:
    errors.append(f"Missing discounted_price: {missing_discounted}, actual_price: {missing_actual}")
    df = df.dropna(subset=['discounted_price', 'actual_price'])

# Filter rating 0-5
if not df['rating'].between(0, 5).all():
    errors.append("Rating values out of 0-5 detected.")
df = df[df['rating'].between(0, 5)]

# Filter discount_percentage 0-100
if not df['discount_percentage'].between(0, 100).all():
    errors.append("Discount percentage out of 0-100 detected.")
df = df[df['discount_percentage'].between(0, 100)]

# discounted_price < actual_price
if not (df['discounted_price'] < df['actual_price']).all():
    errors.append("Found discounted_price >= actual_price.")
df = df[df['discounted_price'] < df['actual_price']]

# Explode review_id if needed
df['review_id'] = df['review_id'].astype(str).str.split(',')
df = df.explode('review_id')

# Drop duplicate review_id
duplicates = df['review_id'].duplicated().sum()
if duplicates > 0:
    errors.append(f"Found {duplicates} duplicated review_id entries.")
df = df.drop_duplicates(subset=['review_id'])

# Validation result
if errors:
    print("Data Validation FAILED! Issues found:")
    for e in errors:
        print(f"- {e}")
else:
    curated_file = DATA_LAKE_CURATED_PATH + 'Clean_data.csv'
    df.to_csv(curated_file, index=False)
    print(f"Data validated and moved to curated: {curated_file}")
