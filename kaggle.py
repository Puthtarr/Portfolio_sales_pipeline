import os
import shutil
import kagglehub
from settings import *

dataset_handle = "karkavelrajaj/amazon-sales-dataset"

# Downlaod Dataset From Kaggle
downloaded_path = kagglehub.dataset_download(handle=dataset_handle)

# Check Default Download Path
print("Downloaded path:", downloaded_path)
# Output = Downloaded path: C:\Users\ASUS\.cache\kagglehub\datasets\karkavelrajaj\amazon-sales-dataset\versions\1

# Fix target Folder
dataset_folder_name = dataset_handle.replace("/", "-")
target_path = os.path.join(DATA_LAKE_RAW_PATH, dataset_folder_name)
print(target_path)

if os.path.exists(target_path):
    shutil.rmtree(target_path)

# Move dataset from default download path to raw path
shutil.move(downloaded_path, target_path)

print(f"Dataset is now stored at: {target_path}")
