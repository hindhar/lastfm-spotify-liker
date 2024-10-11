# database.py

import sqlite3
import logging
from datetime import datetime, timezone
from .utils import normalize_string

class Database:
    def __init__(self, db_file='db/lastfm_history.db'):
        self.db_file = db_file
        self.create_table()

    def connect(self):
        return sqlite3.connect(self.db_file)

    def create_table(self):
        with self.connect() as conn:
            c = conn.cursor()
            c.execute('''
                CREATE TABLE IF NOT EXISTS tracks (
                    id INTEGER PRIMARY KEY,
                    artist TEXT,
                    name TEXT,
                    album TEXT,
                    listen_count INTEGER,
                    last_listened DATETIME,
                    mbid TEXT,
                    UNIQUE(artist, name)
                )
            ''')
        self.add_processed_column()

    def add_or_update_track(self, track):
        artist = normalize_string(track['artist'])
        name = normalize_string(track['name'])
        album = normalize_string(track.get('album', ''))
        date = track['date'].astimezone(timezone.utc).isoformat()
        query = '''
        INSERT INTO tracks (artist, name, album, listen_count, last_listened, mbid)
        VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(artist, name) DO UPDATE SET
        listen_count = listen_count + 1,
        last_listened = ?,
        album = COALESCE(?, album),
        mbid = COALESCE(?, mbid)
        '''
        with self.connect() as conn:
            conn.execute(query, (
                artist,
                name,
                album,
                date,
                track.get('mbid', ''),
                date,
                album,
                track.get('mbid', '')
            ))

    def get_last_update_time(self):
        query = 'SELECT MAX(last_listened) FROM tracks'
        with self.connect() as conn:
            result = conn.execute(query).fetchone()
        if result and result[0]:
            # Ensure the returned datetime is timezone-aware and in UTC
            return datetime.fromisoformat(result[0]).replace(tzinfo=timezone.utc)
        return None

    def get_frequently_played_tracks(self, min_count=5):
        query = '''
        SELECT artist, name, listen_count
        FROM tracks
        WHERE listen_count >= ? AND processed = 0
        ORDER BY listen_count DESC
        '''
        with self.connect() as conn:
            return conn.execute(query, (min_count,)).fetchall()

    def mark_tracks_as_processed(self, tracks):
        with self.connect() as conn:
            c = conn.cursor()
            for artist, name, _ in tracks:
                c.execute('''
                    UPDATE tracks
                    SET processed = 1
                    WHERE artist = ? AND name = ?
                ''', (artist, name))
            conn.commit()

    def add_processed_column(self):
        with self.connect() as conn:
            c = conn.cursor()
            try:
                c.execute('ALTER TABLE tracks ADD COLUMN processed BOOLEAN DEFAULT 0')
                conn.commit()
                logging.info("Added 'processed' column to tracks table")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logging.info("'processed' column already exists in tracks table")
                else:
                    raise
