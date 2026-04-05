import os
import json
from collections import defaultdict
from datetime import datetime
from flask import Flask, render_template, request, jsonify
import mysql.connector
from pyhive import hive
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)
app.secret_key = "spotify-bigdata-secret"

# ── Config ────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "database": os.getenv("MYSQL_DB", "spotify_db"),
    "user": os.getenv("MYSQL_USER", "spotify_user"),
    "password": os.getenv("MYSQL_PASSWORD", "spotify_pass"),
}
HIVE_HOST = os.getenv("HIVE_HOST", "hive")
HIVE_PORT = int(os.getenv("HIVE_PORT", 10000))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# ── Helpers ───────────────────────────────────
def get_db():
    return mysql.connector.connect(**DB_CONFIG)


def get_hive():
    return hive.connect(host=HIVE_HOST, port=HIVE_PORT, auth="NONE")


def hive_query(query):
    conn = get_hive()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0].split(".")[-1] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows


_producer = None

def get_producer():
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            pass
    return _producer


def send_event(event_type, data):
    producer = get_producer()
    if producer:
        producer.send("spotify-events", {"event": event_type, **data})


# ── Load Hive queries from queries.hql ────────
HIVE_QUERIES = {}

def load_hive_queries():
    with open("/hive/queries.hql", "r") as f:
        content = f.read()
    # Parse named queries: lines starting with "-- ── Q<n>:" are query headers
    parts = content.split("-- ── ")
    for part in parts[1:]:  # skip everything before first query
        lines = part.strip().split("\n")
        header = lines[0]  # e.g. "Q1: Top 20 most popular tracks ───────────"
        name = header.split(":")[0].strip()  # e.g. "Q1"
        # collect SQL lines (skip header and trailing comment lines)
        sql_lines = []
        for line in lines[1:]:
            if line.startswith("-- ──"):
                break
            sql_lines.append(line)
        sql = "\n".join(sql_lines).strip().rstrip(";")
        if sql:
            HIVE_QUERIES[name] = sql

load_hive_queries()


def run_hive(query_name):
    return hive_query(HIVE_QUERIES[query_name])


# ── Routes ────────────────────────────────────
@app.route("/")
def index():
    top_tracks = run_hive("Q1")
    genre_stats = run_hive("Q3")
    return render_template("index.html", top_tracks=top_tracks, genre_stats=genre_stats)


@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    artists = []

    if q:
        send_event("search", {"query": q})
        # Search uses MySQL (fast indexed lookup)
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT artist_id, artist_name FROM artists WHERE artist_name LIKE %s LIMIT 20",
            (f"%{q}%",),
        )
        artists = cursor.fetchall()
        cursor.close()
        conn.close()

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(artists)

    return render_template("search.html", query=q, artists=artists)


@app.route("/artist/<name>")
def artist_page(name):
    # Artist lookup from MySQL
    conn = get_db()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM artists WHERE artist_name = %s", (name,))
    artist = cursor.fetchone()

    tracks = []
    stats = {}

    if artist:
        send_event("artist_view", {"artist": name})

        # Track details from MySQL (has the junction table)
        cursor.execute("""
            SELECT t.track_name, t.album_name, t.popularity, t.duration_ms,
                   t.danceability, t.energy, t.loudness, t.tempo, t.valence,
                   t.acousticness, t.speechiness, t.instrumentalness,
                   t.liveness, t.explicit, g.genre_name
            FROM tracks t
            JOIN track_artists ta ON t.id = ta.track_id
            JOIN genres g ON t.genre_id = g.genre_id
            WHERE ta.artist_id = %s
            ORDER BY t.popularity DESC
        """, (artist["artist_id"],))
        tracks = cursor.fetchall()

        if tracks:
            n = len(tracks)
            stats = {
                "total_tracks": n,
                "avg_popularity": round(sum(t["popularity"] or 0 for t in tracks) / n, 1),
                "avg_energy": round(sum(t["energy"] or 0 for t in tracks) / n, 3),
                "avg_danceability": round(sum(t["danceability"] or 0 for t in tracks) / n, 3),
                "avg_tempo": round(sum(t["tempo"] or 0 for t in tracks) / n, 1),
                "avg_valence": round(sum(t["valence"] or 0 for t in tracks) / n, 3),
                "avg_acousticness": round(sum(t["acousticness"] or 0 for t in tracks) / n, 3),
                "avg_speechiness": round(sum(t["speechiness"] or 0 for t in tracks) / n, 3),
                "avg_liveness": round(sum(t["liveness"] or 0 for t in tracks) / n, 3),
                "avg_instrumentalness": round(sum(t["instrumentalness"] or 0 for t in tracks) / n, 3),
                "genres": list(set(t["genre_name"] for t in tracks if t["genre_name"])),
            }

    cursor.close()
    conn.close()
    return render_template("artist.html", artist=artist, tracks=tracks, stats=stats)


@app.route("/upload")
def upload_page():
    return render_template("upload.html")


def consume_all_events():
    """Read all events from Kafka topic spotify-personal."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        "spotify-personal",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    events = [msg.value for msg in consumer]
    consumer.close()
    return events


def compute_analytics(all_events):
    """Compute personal analytics from a list of streaming events."""
    artist_ms = defaultdict(int)
    track_ms = defaultdict(int)
    year_ms = defaultdict(int)
    hour_counts = defaultdict(int)
    platform_counts = defaultdict(int)
    skipped_artists = defaultdict(int)
    total_ms = 0

    for e in all_events:
        ms = e.get("ms_played", 0) or 0
        artist = e.get("master_metadata_album_artist_name") or ""
        track = e.get("master_metadata_track_name") or ""
        ts = e.get("ts", "")
        platform = e.get("platform", "unknown") or "unknown"
        skipped = e.get("skipped", False)

        if not artist and not track:
            continue

        total_ms += ms
        if artist:
            artist_ms[artist] += ms
        if track and artist:
            track_ms[f"{track} — {artist}"] += ms

        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                year_ms[dt.year] += ms
                hour_counts[dt.hour] += 1
            except ValueError:
                pass

        platform_counts[platform] += 1

        if skipped and artist:
            skipped_artists[artist] += 1

    analytics = {
        "total_hours": round(total_ms / 3_600_000, 1),
        "total_events": len(all_events),
        "top_artists": sorted(artist_ms.items(), key=lambda x: -x[1])[:10],
        "top_tracks": sorted(track_ms.items(), key=lambda x: -x[1])[:10],
        "hours_by_year": sorted(year_ms.items()),
        "hour_of_day": [hour_counts.get(h, 0) for h in range(24)],
        "platforms": sorted(platform_counts.items(), key=lambda x: -x[1])[:8],
        "most_skipped": sorted(skipped_artists.items(), key=lambda x: -x[1])[:10],
    }

    analytics["top_artists"] = [(a, round(ms / 3_600_000, 1)) for a, ms in analytics["top_artists"]]
    analytics["top_tracks"] = [(t, round(ms / 3_600_000, 1)) for t, ms in analytics["top_tracks"]]
    analytics["hours_by_year"] = [(y, round(ms / 3_600_000, 1)) for y, ms in analytics["hours_by_year"]]

    return analytics


@app.route("/personal")
def personal():
    try:
        events = consume_all_events()
    except Exception:
        events = []

    if not events:
        return render_template("personal.html", analytics=None)

    analytics = compute_analytics(events)
    return render_template("personal.html", analytics=analytics)


@app.route("/kafka-preview")
def kafka_preview():
    from kafka import KafkaConsumer
    try:
        consumer = KafkaConsumer(
            "spotify-personal",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            consumer_timeout_ms=3000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        # Seek to end minus 10 messages
        consumer.poll(timeout_ms=1000)
        partitions = consumer.assignment()
        for tp in partitions:
            end = consumer.end_offsets([tp])[tp]
            start = max(0, end - 10)
            consumer.seek(tp, start)

        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= 10:
                break
        consumer.close()
    except Exception:
        messages = []

    return jsonify(messages)


WEBHDFS = "http://namenode:9870/webhdfs/v1"


@app.route("/flume")
def flume_page():
    return render_template("flume.html")


@app.route("/flume-data")
def flume_data():
    import requests
    result = {"files": [], "logs": []}
    try:
        # List log directories
        r = requests.get(f"{WEBHDFS}/logs/spotify?op=LISTSTATUS", timeout=5)
        if r.status_code == 200:
            dirs = r.json().get("FileStatuses", {}).get("FileStatus", [])
            for d in sorted(dirs, key=lambda x: x["pathSuffix"], reverse=True)[:5]:
                date_dir = d["pathSuffix"]
                # List files in each date dir
                r2 = requests.get(f"{WEBHDFS}/logs/spotify/{date_dir}?op=LISTSTATUS", timeout=5)
                if r2.status_code == 200:
                    files = r2.json().get("FileStatuses", {}).get("FileStatus", [])
                    for f in files:
                        result["files"].append({
                            "date": date_dir,
                            "name": f["pathSuffix"],
                            "size": f["length"],
                        })

        # Read latest log file content
        if result["files"]:
            latest = result["files"][0]
            path = f"/logs/spotify/{latest['date']}/{latest['name']}"
            r3 = requests.get(f"{WEBHDFS}{path}?op=OPEN&length=4096", timeout=5, allow_redirects=True)
            if r3.status_code == 200:
                lines = r3.text.strip().split("\n")
                result["logs"] = lines[-20:]  # last 20 lines
    except Exception:
        pass

    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
