CREATE SCHEMA IF NOT EXISTS spotify;

CREATE TABLE IF NOT EXISTS spotify.artist (
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR NOT NULL,
    external_url VARCHAR
);

CREATE TABLE IF NOT EXISTS spotify.album (
    album_id VARCHAR PRIMARY KEY,
    album_name VARCHAR NOT NULL,
    release_date DATE,
    total_tracks BIGINT,
    url VARCHAR
);

CREATE TABLE IF NOT EXISTS spotify.song (
    song_id VARCHAR PRIMARY KEY,
    song_name VARCHAR NOT NULL,
    duration_ms BIGINT,
    url VARCHAR,
    popularity BIGINT,
    song_added DATE,
    album_id VARCHAR REFERENCES spotify.album(album_id),
    artist_id VARCHAR REFERENCES spotify.artist(artist_id)
);