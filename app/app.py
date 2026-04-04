import os
import json
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
    columns = [desc[0] for desc in cursor.description]
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


# ── Routes ────────────────────────────────────
@app.route("/")
def index():
    # Top 20 tracks from Hive (HDFS data)
    top_tracks = hive_query("""
        SELECT track_name, popularity, energy, danceability
        FROM tracks
        ORDER BY popularity DESC
        LIMIT 20
    """)

    # Genre stats from Hive (HDFS data)
    genre_stats = hive_query("""
        SELECT g.genre_name,
               COUNT(*) AS track_count,
               ROUND(AVG(t.popularity), 1) AS avg_popularity
        FROM tracks t
        JOIN genres g ON t.genre_id = g.genre_id
        GROUP BY g.genre_name
        ORDER BY track_count DESC
        LIMIT 15
    """)

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


@app.route("/play", methods=["POST"])
def play():
    data = request.get_json()
    send_event("play", {"track": data.get("track"), "artist": data.get("artist")})
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
