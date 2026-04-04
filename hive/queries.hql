-- ─────────────────────────────────────────────
--  Spotify Big Data Project — Hive Queries
--  Run: docker exec -it spotify_hive hive -f /hive/queries.hql
--  Or open hive shell and paste individual queries
-- ─────────────────────────────────────────────

-- ═══════════════════════════════════════════════
--  PART 1: EXTERNAL TABLES (run once)
-- ═══════════════════════════════════════════════

-- Column order matches Sqoop's alphabetical export
CREATE EXTERNAL TABLE IF NOT EXISTS tracks (
  acousticness      FLOAT,
  album_name        STRING,
  danceability      FLOAT,
  duration_ms       INT,
  energy            FLOAT,
  explicit          BOOLEAN,
  genre_id          INT,
  id                INT,
  instrumentalness  FLOAT,
  `key`             INT,
  liveness          FLOAT,
  loudness          FLOAT,
  `mode`            INT,
  popularity        INT,
  speechiness       FLOAT,
  tempo             FLOAT,
  time_signature    INT,
  track_id          STRING,
  track_name        STRING,
  valence           FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/spotify/tracks/';

CREATE EXTERNAL TABLE IF NOT EXISTS artists (
  artist_id   INT,
  artist_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/spotify/artists/';

CREATE EXTERNAL TABLE IF NOT EXISTS genres (
  genre_id   INT,
  genre_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/spotify/genres/';

-- ═══════════════════════════════════════════════
--  PART 2: ANALYTICS QUERIES
--  Copy-paste individual queries as needed
-- ═══════════════════════════════════════════════

-- ── Q1: Top 20 most popular tracks (deduplicated) ───────────
SELECT track_name,
       MAX(popularity) AS popularity,
       MAX(energy) AS energy,
       MAX(danceability) AS danceability
FROM tracks
GROUP BY track_name
ORDER BY popularity DESC
LIMIT 20;

-- ── Q2: Average energy by genre ──────────────
SELECT g.genre_name, ROUND(AVG(t.energy), 3) AS avg_energy
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_energy DESC;

-- ── Q3: Genre audio profile (top 15 by avg popularity) ──
SELECT g.genre_name,
       ROUND(AVG(t.popularity), 1)     AS avg_popularity,
       ROUND(AVG(t.energy), 3)         AS avg_energy,
       ROUND(AVG(t.danceability), 3)   AS avg_danceability
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_popularity DESC
LIMIT 15;

-- ── Q4: Top 10 genres by number of tracks ────
SELECT g.genre_name, COUNT(*) AS track_count
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY track_count DESC
LIMIT 10;

-- ── Q5: Explicit vs non-explicit track stats ─
SELECT explicit,
       COUNT(*)                      AS track_count,
       ROUND(AVG(popularity), 1)     AS avg_popularity,
       ROUND(AVG(energy), 3)         AS avg_energy
FROM tracks
GROUP BY explicit;

-- ── Q6: Average tempo by genre ───────────────
SELECT g.genre_name, ROUND(AVG(t.tempo), 1) AS avg_tempo
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_tempo DESC
LIMIT 10;

-- ── Q7: Most danceable genres ────────────────
SELECT g.genre_name, ROUND(AVG(t.danceability), 3) AS avg_danceability
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_danceability DESC
LIMIT 10;

-- ── Q8: Loudest genres ──────────────────────
SELECT g.genre_name, ROUND(AVG(t.loudness), 2) AS avg_loudness
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_loudness DESC
LIMIT 10;

-- ── Q9: Track duration distribution (short/medium/long) ──
SELECT
  CASE
    WHEN duration_ms < 180000  THEN 'short (<3min)'
    WHEN duration_ms < 300000  THEN 'medium (3-5min)'
    ELSE 'long (>5min)'
  END AS duration_bucket,
  COUNT(*) AS track_count
FROM tracks
GROUP BY
  CASE
    WHEN duration_ms < 180000  THEN 'short (<3min)'
    WHEN duration_ms < 300000  THEN 'medium (3-5min)'
    ELSE 'long (>5min)'
  END;

-- ── Q10: Acousticness vs Energy correlation by genre ──
SELECT g.genre_name,
       ROUND(AVG(t.acousticness), 3) AS avg_acousticness,
       ROUND(AVG(t.energy), 3)       AS avg_energy
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY avg_acousticness DESC
LIMIT 10;
