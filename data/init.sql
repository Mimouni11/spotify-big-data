-- ─────────────────────────────────────────────
--  Spotify Big Data Project — MySQL Schema
--  Auto-runs on first docker-compose up
-- ─────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS spotify_db;
USE spotify_db;

-- ── 1. Artists ──────────────────────────────
CREATE TABLE IF NOT EXISTS artists (
  artist_id   INT AUTO_INCREMENT PRIMARY KEY,
  artist_name VARCHAR(255) NOT NULL UNIQUE
);

-- ── 2. Genres ───────────────────────────────
CREATE TABLE IF NOT EXISTS genres (
  genre_id   INT AUTO_INCREMENT PRIMARY KEY,
  genre_name VARCHAR(100) NOT NULL UNIQUE
);

-- ── 3. Tracks (main fact table) ─────────────
CREATE TABLE IF NOT EXISTS tracks (
  id                INT PRIMARY KEY,
  track_id          VARCHAR(62) NOT NULL,
  track_name        VARCHAR(500),
  album_name        VARCHAR(500),
  popularity        INT,
  duration_ms       INT,
  explicit          BOOLEAN,
  danceability      FLOAT,
  energy            FLOAT,
  `key`             INT,
  loudness          FLOAT,
  `mode`            INT,
  speechiness       FLOAT,
  acousticness      FLOAT,
  instrumentalness  FLOAT,
  liveness          FLOAT,
  valence           FLOAT,
  tempo             FLOAT,
  time_signature    INT,
  genre_id          INT,
  FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

-- ── 4. Track-Artist junction (many-to-many) ─
--  One track can have multiple artists (e.g. "Ingrid Michaelson;ZAYN")
CREATE TABLE IF NOT EXISTS track_artists (
  track_id   INT NOT NULL,
  artist_id  INT NOT NULL,
  PRIMARY KEY (track_id, artist_id),
  FOREIGN KEY (track_id)  REFERENCES tracks(id),
  FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);
