
import pandas as pd
from langdetect import detect
from dateutil import parser
import os

IN_PATH = "scrape_output/merged_play_reviews.csv"
OUT_PATH = "data/play_reviews_clean.csv"
os.makedirs("data", exist_ok=True)

df = pd.read_csv(IN_PATH, parse_dates=['at'], infer_datetime_format=True)
# Deduplicate by review_id
df = df.drop_duplicates(subset=['review_id'])

# Basic cleaning
df['content'] = df['content'].fillna("").astype(str)
df['content_clean'] = df['content'].str.replace(r'\s+', ' ', regex=True).str.strip()
# Detect language (fast check)
def safe_detect(s):
    try:
        return detect(s) if s and len(s) > 2 else "unknown"
    except:
        return "unknown"
df['lang'] = df['content_clean'].apply(safe_detect)

# Normalize date to YYYY-MM-DD
df['date'] = pd.to_datetime(df['at'], errors='coerce')
df['date'] = df['date'].dt.date

# Keep required columns (plus helpful extras)
out = df.rename(columns={"content":"review", "score":"rating"})[
    ['review_id','review','rating','date','bank','source','content_clean','lang','review_created_version','thumbs_up_count','reply_content','replied_at']
]

# Count missing
missing_pct = out.isna().mean()
print("Missing percentage per column:")
print(missing_pct)

out.to_csv(OUT_PATH, index=False)
print("Saved cleaned CSV:", OUT_PATH)
