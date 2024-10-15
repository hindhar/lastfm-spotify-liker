#!/usr/bin/env python3
# album_saver.py

import os
import sys
import logging
import sqlite3
from datetime import datetime, timezone
from dotenv import load_dotenv
from typing import List, Set, Optional
import time
import spotipy

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from src.database import Database
from src.spotify_operations import SpotifyOperations
from src.utils import normalize_string, get_user_input_with_timeout

# Load environment variables
load_dotenv()

LASTFM_DB_FILE = os.getenv('LASTFM_DB_FILE', 'db/lastfm_history.db')
SPOTIFY_DB_FILE = os.getenv('SPOTIFY_DB_FILE', 'db/spotify_liked_songs.db')
ALBUM_SAVER_DB_FILE = os.path.join(project_root, 'db', 'album_saver.db')

# Ensure the 'logs' directory exists
logs_dir = os.path.join(project_root, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join(logs_dir, 'album_saver.log'),
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class AlbumSaver:
    def __init__(self):
        """Initialize the AlbumSaver class."""
        self.lastfm_db = Database(db_file=LASTFM_DB_FILE)
        self.spotify_ops = SpotifyOperations(db_file=SPOTIFY_DB_FILE)
        self.sp = self.spotify_ops.sp  # Use the Spotify client from SpotifyOperations
        self.create_album_saver_table()
        self.saved_albums = self.get_all_saved_albums()  # Load saved albums from local database

    def create_album_saver_table(self) -> None:
        """Create the necessary tables in the album_saver database."""
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS saved_albums (
                id TEXT PRIMARY KEY,
                name TEXT,
                artist TEXT,
                last_checked DATETIME
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        conn.commit()
        conn.close()
        logging.info("Initialized album_saver database tables.")

    def fetch_saved_albums(self) -> None:
        """Fetch saved albums from Spotify and store them in the album_saver database."""
        offset = 0
        limit = 50
        all_albums = []

        logging.info("Fetching saved albums from Spotify...")
        while True:
            results = self.sp.current_user_saved_albums(limit=limit, offset=offset)
            albums = results['items']
            if not albums:
                break
            all_albums.extend(albums)
            offset += limit
            time.sleep(0.1)  # Respect rate limits

        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()

        for item in all_albums:
            album = item['album']
            name = normalize_string(album['name'])
            artist = normalize_string(album['artists'][0]['name'])
            album_id = album['id']
            added_at = item['added_at']
            c.execute("INSERT OR REPLACE INTO saved_albums VALUES (?, ?, ?, ?)",
                      (album_id, name, artist, added_at))

        conn.commit()
        conn.close()

        logging.info(f"Fetched and stored {len(all_albums)} saved albums.")

    def get_all_saved_albums(self) -> Set[tuple]:
        """Retrieve all saved albums (normalized name and artist) from the album_saver database."""
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute("SELECT name, artist FROM saved_albums")
        saved_albums = set((row[0], row[1]) for row in c.fetchall())
        conn.close()
        logging.info(f"Retrieved {len(saved_albums)} saved albums from album_saver database.")
        return saved_albums

    def get_last_update_time(self) -> Optional[datetime]:
        """Retrieve the last update time from the metadata table."""
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute("SELECT value FROM metadata WHERE key = 'last_update'")
        result = c.fetchone()
        conn.close()
        if result and result[0]:
            return datetime.fromisoformat(result[0]).astimezone(timezone.utc)
        return None

    def set_last_update_time(self, update_time: datetime) -> None:
        """Set the last update time in the metadata table."""
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            ('last_update', update_time.astimezone(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
        logging.info(f"Set last update time to: {update_time.isoformat()}")

    def process_albums(self, force_full_check: bool = False) -> None:
        """Main method to process albums and save them to Spotify if they meet criteria."""
        last_update = self.get_last_update_time()
        if force_full_check or last_update is None:
            logging.info("Performing full album check.")
            albums_to_check = self.lastfm_db.get_all_albums()
        else:
            logging.info(f"Checking albums updated since {last_update.isoformat()}")
            albums_to_check = self.lastfm_db.get_albums_since(last_update)

        logging.info(f"Found {len(albums_to_check)} albums to check.")

        new_albums_added = 0
        for index, album in enumerate(albums_to_check, 1):
            try:
                album_name = album['name']
                artist_name = album['artist']
                normalized_album = normalize_string(album_name)
                normalized_artist = normalize_string(artist_name)

                # Skip 'Various Artists'
                if 'various artists' in normalized_artist.lower():
                    logging.info(f"Skipping 'Various Artists' album: {artist_name} - {album_name}")
                    continue

                logging.info(f"Processing album {index}/{len(albums_to_check)}: {artist_name} - {album_name}")

                # Check if album meets criteria based on Last.fm data
                if self.check_album_conditions(album_name, artist_name):
                    logging.info(f"Album meets criteria: {artist_name} - {album_name}")
                    # Check if album is already saved on Spotify
                    if (normalized_album, normalized_artist) not in self.saved_albums:
                        # Search for album on Spotify
                        album_id = self.spotify_ops.search_album(album_name, artist_name)
                        if album_id:
                            self.save_album_to_library(album_id, album_name, artist_name)
                            new_albums_added += 1
                            self.saved_albums.add((normalized_album, normalized_artist))  # Update the saved albums set
                        else:
                            logging.info(f"Album not found on Spotify: {artist_name} - {album_name}")
                    else:
                        logging.info(f"Album already saved on Spotify: {artist_name} - {album_name}")
                else:
                    logging.info(f"Album does not meet criteria: {artist_name} - {album_name}")
            except Exception as e:
                logging.error(f"Error processing album {artist_name} - {album_name}: {e}", exc_info=True)
                continue  # Continue processing next album

        logging.info(f"Processed {len(albums_to_check)} albums. Added {new_albums_added} new albums.")
        self.set_last_update_time(datetime.now(timezone.utc))

    def check_album_conditions(self, album_name: str, artist_name: str) -> bool:
        """Check if an album meets the criteria to be saved based on Last.fm data."""
        logging.info(f"Starting to check album conditions for album: {artist_name} - {album_name}")
        try:
            conn = sqlite3.connect(LASTFM_DB_FILE)
            c = conn.cursor()
            c.execute(
                "SELECT name, listen_count FROM tracks WHERE album = ? AND artist = ?",
                (album_name, artist_name)
            )
            tracks = c.fetchall()
            conn.close()
            total_tracks = len(tracks)
            if total_tracks == 0:
                logging.info(f"No tracks found for album {artist_name} - {album_name} in Last.fm database.")
                return False
            if total_tracks < 6:
                logging.info(f"Album has fewer than 6 tracks ({total_tracks}); skipping.")
                return False
            logging.info(f"Found {total_tracks} tracks for album {artist_name} - {album_name}")

            listened_tracks = sum(1 for track in tracks if track[1] > 0)
            tracks_listened_3_times = sum(1 for track in tracks if track[1] >= 3)

            condition1 = listened_tracks >= 0.75 * total_tracks
            condition2 = tracks_listened_3_times >= 3

            logging.info(f"Listened tracks: {listened_tracks}/{total_tracks}, "
                         f"Tracks listened 3+ times: {tracks_listened_3_times}")
            logging.info(f"Condition 1 met: {condition1}, Condition 2 met: {condition2}")

            return condition1 or condition2
        except Exception as e:
            logging.error(f"Error checking album conditions for {artist_name} - {album_name}: {e}", exc_info=True)
            return False

    def save_album_to_library(self, album_id: str, album_name: str, artist_name: str) -> None:
        """Save an album to the Spotify library."""
        try:
            logging.info(f"Saving album: {artist_name} - {album_name}")
            # Save the album to the library
            self.sp.current_user_saved_albums_add([album_id])
            logging.info(f"Album saved: {artist_name} - {album_name}")
            self.update_saved_album(album_id, album_name, artist_name)
        except Exception as e:
            logging.error(f"Error saving album {album_id}: {e}", exc_info=True)

    def update_saved_album(self, album_id: str, album_name: str, artist_name: str) -> None:
        """Update the saved_albums table with the new album."""
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        now = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        c.execute(
            "INSERT OR REPLACE INTO saved_albums (id, name, artist, last_checked) VALUES (?, ?, ?, ?)",
            (album_id, normalize_string(album_name), normalize_string(artist_name), now)
        )
        conn.commit()
        conn.close()
        logging.info(f"Updated album_saver database for album: {artist_name} - {album_name}")

def main():
    try:
        album_saver = AlbumSaver()

        # Prompt the user for full or update check
        prompt = "Do you want to perform a full check or an update check? (Enter 'full' or 'update' within 10 seconds, default is 'update'): "
        user_choice = get_user_input_with_timeout(prompt, timeout=10)

        if user_choice.lower() == 'full':
            force_full_check = True
            logging.info("User selected full check.")
        else:
            force_full_check = False
            logging.info("Proceeding with update check.")

        # Fetch saved albums from Spotify and update local database
        album_saver.fetch_saved_albums()
        album_saver.saved_albums = album_saver.get_all_saved_albums()  # Refresh saved albums set

        album_saver.process_albums(force_full_check)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
