import pandas as pd
import mysql.connector
import sys

# ── Config ────────────────────────────────────
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB   = "spotify_db"
MYSQL_USER = "spotify_user"
MYSQL_PASS = "spotify_pass"
CSV_PATH   = "data/dataset.csv"

# ── Connect ───────────────────────────────────
print("Connecting to MySQL...")
conn = mysql.connector.connect(
    host=MYSQL_HOST, port=MYSQL_PORT,
    database=MYSQL_DB, user=MYSQL_USER, password=MYSQL_PASS
)
cursor = conn.cursor()

# ── Load CSV ──────────────────────────────────
print(f"Reading {CSV_PATH}...")
df = pd.read_csv(CSV_PATH, index_col=0)
print(f"  {len(df)} rows loaded")

# ── 1. Insert genres ─────────────────────────
print("Inserting genres...")
genres = df["track_genre"].dropna().unique()
cursor.executemany(
    "INSERT IGNORE INTO genres (genre_name) VALUES (%s)",
    [(g,) for g in genres]
)
conn.commit()

# build genre lookup: name -> id
cursor.execute("SELECT genre_id, genre_name FROM genres")
genre_map = {name: gid for gid, name in cursor.fetchall()}
print(f"  {len(genre_map)} genres")

# ── 2. Insert artists ────────────────────────
print("Inserting artists...")
all_artists = set()
for artists_str in df["artists"].dropna():
    for a in artists_str.split(";"):
        all_artists.add(a.strip())

cursor.executemany(
    "INSERT IGNORE INTO artists (artist_name) VALUES (%s)",
    [(a,) for a in all_artists]
)
conn.commit()

# build artist lookup: name -> id
cursor.execute("SELECT artist_id, artist_name FROM artists")
artist_map = {name: aid for aid, name in cursor.fetchall()}
print(f"  {len(artist_map)} artists")

# ── 3. Insert tracks ─────────────────────────
print("Inserting tracks...")
track_sql = """
    INSERT IGNORE INTO tracks
        (id, track_id, track_name, album_name, popularity, duration_ms,
         explicit, danceability, energy, `key`, loudness, `mode`,
         speechiness, acousticness, instrumentalness, liveness,
         valence, tempo, time_signature, genre_id)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

def safe_int(v):
    try:
        return int(v) if pd.notna(v) else None
    except (ValueError, TypeError):
        return None

def safe_float(v):
    try:
        f = float(v)
        return None if pd.isna(f) else f
    except (ValueError, TypeError):
        return None

def safe_bool(v):
    if pd.isna(v):
        return None
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"

track_rows = []
for idx, row in df.iterrows():
    track_rows.append((
        int(idx),
        str(row["track_id"]) if pd.notna(row["track_id"]) else None,
        str(row["track_name"]) if pd.notna(row["track_name"]) else None,
        str(row["album_name"]) if pd.notna(row["album_name"]) else None,
        safe_int(row["popularity"]),
        safe_int(row["duration_ms"]),
        safe_bool(row["explicit"]),
        safe_float(row["danceability"]),
        safe_float(row["energy"]),
        safe_int(row["key"]),
        safe_float(row["loudness"]),
        safe_int(row["mode"]),
        safe_float(row["speechiness"]),
        safe_float(row["acousticness"]),
        safe_float(row["instrumentalness"]),
        safe_float(row["liveness"]),
        safe_float(row["valence"]),
        safe_float(row["tempo"]),
        safe_int(row["time_signature"]),
        genre_map.get(row["track_genre"]),
    ))

# batch insert 5000 at a time
BATCH = 5000
for i in range(0, len(track_rows), BATCH):
    cursor.executemany(track_sql, track_rows[i:i+BATCH])
    conn.commit()
    print(f"  tracks: {min(i+BATCH, len(track_rows))}/{len(track_rows)}")

# ── 4. Insert track_artists junction ─────────
print("Inserting track_artists...")
ta_sql = "INSERT IGNORE INTO track_artists (track_id, artist_id) VALUES (%s, %s)"
ta_rows = []
for idx, row in df.iterrows():
    if pd.notna(row["artists"]):
        for a in row["artists"].split(";"):
            aid = artist_map.get(a.strip())
            if aid:
                ta_rows.append((int(idx), aid))

for i in range(0, len(ta_rows), BATCH):
    cursor.executemany(ta_sql, ta_rows[i:i+BATCH])
    conn.commit()
    print(f"  track_artists: {min(i+BATCH, len(ta_rows))}/{len(ta_rows)}")

# ── Done ──────────────────────────────────────
cursor.close()
conn.close()
print("Done! All data loaded into MySQL.")
