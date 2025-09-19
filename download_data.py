import os, glob, shutil
import kagglehub

# 1) Download the Kaggle dataset to your local cache
src = kagglehub.dataset_download("rohanrao/nifty50-stock-market-data")
print("Downloaded to:", src)

# 2) Copy all CSVs into ./data inside your project
dst = os.path.join(os.getcwd(), "data")
os.makedirs(dst, exist_ok=True)

copied = 0
for f in glob.glob(os.path.join(src, "*.csv")):
    shutil.copy(f, dst)
    copied += 1

print(f"Copied {copied} CSV files to {dst}")
