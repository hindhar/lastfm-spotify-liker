# database.py

import sqlite3
import logging
from datetime import datetime, timezone
from src.utils import normalize_string
from typing import List, Dict, Optional

class Database:
    def __init__(self, db_file: str = 'db/lastfm_history.db'):
        """Initialize the Database class."""
        self.db_file = db_file
        self.create_table()
        self.add_processed_column()  # Add this line to ensure the 'processed' column exists

    def connect(self) -> sqlite3.Connection:
        """Connect to the SQLite database."""
        return sqlite3.connect(self.db_file)

    def create_table(self) -> None:
        """Create the tracks table in the database if it doesn't exist."""
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
                    processed BOOLEAN DEFAULT 0,
                    UNIQUE(artist, name)
                )
            ''')
            conn.commit()
        logging.info("Initialized tracks table in Last.fm database.")

    def add_or_update_track(self, track: Dict) -> None:
        """Add a new track or update an existing track in the database."""
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
        logging.info(f"Added/Updated track in database: {artist} - {name}")

    def get_last_update_time(self) -> Optional[datetime]:
        """Get the last time a track was listened to."""
        query = 'SELECT MAX(last_listened) FROM tracks'
        with self.connect() as conn:
            result = conn.execute(query).fetchone()
        if result and result[0]:
            return datetime.fromisoformat(result[0]).replace(tzinfo=timezone.utc)
        return None

    def get_frequently_played_tracks(self, min_count: int = 5) -> List[tuple]:
        """Get tracks that have been played frequently."""
        query = '''
        SELECT artist, name, listen_count
        FROM tracks
        WHERE listen_count >= ? AND processed = 0
        ORDER BY listen_count DESC
        '''
        with self.connect() as conn:
            return conn.execute(query, (min_count,)).fetchall()

    def mark_tracks_as_processed(self, tracks: List[tuple]) -> None:
        """Mark tracks as processed."""
        with self.connect() as conn:
            c = conn.cursor()
            for artist, name, _ in tracks:
                c.execute('''
                    UPDATE tracks
                    SET processed = 1
                    WHERE artist = ? AND name = ?
                ''', (artist, name))
            conn.commit()
        logging.info(f"Marked {len(tracks)} tracks as processed.")

    def get_albums_since(self, last_update: datetime) -> List[Dict]:
        """Retrieve all albums listened to since the last update."""
        query = """
        SELECT DISTINCT album, artist 
        FROM tracks 
        WHERE last_listened > ?
        """
        with self.connect() as conn:
            c = conn.cursor()
            c.execute(query, (last_update.isoformat(),))
            albums = [{'name': row[0], 'artist': row[1]} for row in c.fetchall()]
        logging.info(f"Retrieved {len(albums)} albums since last update.")
        return albums

    def get_all_albums(self) -> List[Dict]:
        """Retrieve all unique albums from the tracks table."""
        query = "SELECT DISTINCT album, artist FROM tracks"
        with self.connect() as conn:
            c = conn.cursor()
            c.execute(query)
            albums = [{'name': row[0], 'artist': row[1]} for row in c.fetchall()]
        logging.info(f"Retrieved all albums from the Last.fm database: {len(albums)} albums.")
        return albums

    def add_processed_column(self) -> None:
        """Add the 'processed' column to the tracks table if it doesn't exist."""
        with self.connect() as conn:
            c = conn.cursor()
            try:
                c.execute('ALTER TABLE tracks ADD COLUMN processed BOOLEAN DEFAULT 0')
                conn.commit()
                logging.info("Added 'processed' column to tracks table.")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    logging.info("'processed' column already exists in tracks table.")
                else:
                    logging.error(f"Error adding 'processed' column: {e}", exc_info=True)
                    raise
