
import os, time, json, csv
from datetime import datetime
from google_play_scraper import reviews
import pandas as pd


APPS = {
    "CBE":    {"pkg": "com.combanketh.mobilebanking", "app_name": "Commercial Bank of Ethiopia Mobile"},
    "BOA":    {"pkg": "com.boa.boaMobileBanking",    "app_name": "Bank of Abyssinia Mobile"},
    "Dashen": {"pkg": "com.dashen.dashensuperapp",   "app_name": "Dashen Bank Mobile"}
}

OUT_DIR = "scrape_output"
MIN_PER_BANK = 400
BATCH = 200
PAUSE_BETWEEN_BATCHES = 1.0
PAUSE_BETWEEN_APPS = 2.0
STATE_FILE = os.path.join(OUT_DIR, "scrape_state.json")
os.makedirs(OUT_DIR, exist_ok=True)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f: return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f: json.dump(state, f, ensure_ascii=False, indent=2)

def safe_csv_append(path, rows):
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(rows)

def clean_text(s):
    if s is None: return ""
    return " ".join(str(s).split()).strip()

def main():
    state = load_state()
    scrape_run_id = datetime.utcnow().isoformat()
    for bank, meta in APPS.items():
        pkg = meta["pkg"]
        app_name = meta["app_name"]
        out_csv = os.path.join(OUT_DIR, f"{bank}_reviews.csv")
        bank_state = state.get(bank, {"continuation_token": None, "fetched": 0})
        token = bank_state.get("continuation_token")
        fetched = bank_state.get("fetched", 0)
        print(f"Starting {bank}: already fetched {fetched}")

        attempts = 0
        while fetched < MIN_PER_BANK:
            try:
                batch, token = reviews(
                    pkg,
                    lang='en',
                    country='et',
                    count=BATCH,
                    continuation_token=token
                )
            except Exception as e:
                print("Fetch error:", e)
                attempts += 1
                if attempts > 5:
                    print("Too many errors; aborting this bank.")
                    break
                time.sleep(5); continue

            if not batch:
                print("No more reviews returned.")
                break

            rows = []
            for rev in batch:
                rows.append({
                    "review_id": rev.get("reviewId"),
                    "bank": bank,
                    "app_name": app_name,
                    "pkg": pkg,
                    "score": rev.get("score"),
                    "content": rev.get("content"),
                    "content_clean": clean_text(rev.get("content")),
                    "at": rev.get("at").isoformat() if rev.get("at") else None,
                    "review_created_version": rev.get("reviewCreatedVersion"),
                    "reply_content": rev.get("replyContent"),
                    "replied_at": rev.get("repliedAt").isoformat() if rev.get("repliedAt") else None,
                    "thumbs_up_count": rev.get("thumbsUpCount"),
                    "source": "google_play",
                    "scrape_run_id": scrape_run_id
                })

            safe_csv_append(out_csv, rows)
            fetched += len(rows)
            # state[bank] = {"continuation_token": token, "fetched": fetched}
            state[bank] = {"continuation_token": str(token) if token else None, "fetched": fetched}

            save_state(state)
            print(f"{bank}: fetched +{len(rows)} => total {fetched}")
            time.sleep(PAUSE_BETWEEN_BATCHES)
        print(f"Done {bank}. fetched: {fetched}")
        time.sleep(PAUSE_BETWEEN_APPS)


    merged_path = os.path.join(OUT_DIR, "merged_play_reviews.csv")
    dfs = []
    for bank in APPS.keys():
        p = os.path.join(OUT_DIR, f"{bank}_reviews.csv")
        if os.path.exists(p): dfs.append(pd.read_csv(p))
    if dfs:
        merged = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['review_id'])
        merged.to_csv(merged_path, index=False)
        print("Merged saved:", merged_path)
    else:
        print("No data files found.")

if __name__ == "__main__":
    main()
