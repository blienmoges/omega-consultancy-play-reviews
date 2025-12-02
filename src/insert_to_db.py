import pandas as pd
import psycopg2

# 1. Load cleaned reviews
df = pd.read_csv("data/play_reviews_clean.csv")

# 2. Map bank to app_name
bank_to_app = {
    "CBE": "Commercial Bank of Ethiopia Mobile",
    "BOA": "Bank of Abyssinia Mobile",
    "Dashen": "Dashen Bank Mobile"
}

df['app_name'] = df['bank'].map(bank_to_app)

# 3. Database connection parameters
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "bank_reviews",
    "user": "Myuser",      # replace with your PostgreSQL username
    "password": "12345678" # replace with your PostgreSQL password
}

# 4. Connect to PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

# 5. Create tables if not exist
cur.execute("""
CREATE TABLE IF NOT EXISTS banks (
    bank_id SERIAL PRIMARY KEY,
    bank_name TEXT NOT NULL,
    app_name TEXT NOT NULL
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS reviews (
    review_id TEXT PRIMARY KEY,
    bank_id INTEGER REFERENCES banks(bank_id),
    review_text TEXT,
    rating INTEGER,
    review_date DATE,
    source TEXT
);
""")
conn.commit()

# 6. Insert banks
banks = df[['bank', 'app_name']].drop_duplicates().values.tolist()
for bank_name, app_name in banks:
    cur.execute("""
        INSERT INTO banks (bank_name, app_name)
        VALUES (%s, %s)
        ON CONFLICT (bank_name) DO NOTHING;
    """, (bank_name, app_name))
conn.commit()

# 7. Fetch bank ids to map reviews
cur.execute("SELECT bank_id, bank_name FROM banks;")
bank_map = {row[1]: row[0] for row in cur.fetchall()}

# 8. Insert reviews
for _, row in df.iterrows():
    bank_id = bank_map[row['bank']]
    cur.execute("""
        INSERT INTO reviews (review_id, bank_id, review_text, rating, review_date, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO NOTHING;
    """, (row['review_id'], bank_id, row['review'], row['rating'], row['date'], row['source']))

conn.commit()
cur.close()
conn.close()

print("Data inserted successfully!")
