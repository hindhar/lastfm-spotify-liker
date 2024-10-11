# album_saver.py

import os
import sys

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from src.database import Database
from src.spotify_operations import SpotifyOperations
from src.utils import normalize_string

import logging
import sqlite3
from datetime import datetime, timezone
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import random

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='logs/album_saver.log', filemode='a')

# Load environment variables
load_dotenv()

LASTFM_DB_FILE = os.getenv('LASTFM_DB_FILE', 'db/lastfm_history.db')
SPOTIFY_DB_FILE = os.getenv('SPOTIFY_DB_FILE', 'db/spotify_liked_songs.db')
ALBUM_SAVER_DB_FILE = os.getenv('ALBUM_SAVER_DB_FILE', 'db/album_saver.db')

class AlbumSaver:
    def __init__(self):
        self.lastfm_db = Database(db_file=LASTFM_DB_FILE)
        self.spotify_ops = SpotifyOperations(db_file=SPOTIFY_DB_FILE)
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read user-library-modify"))
        self.create_album_saver_table()

    def create_album_saver_table(self):
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS saved_albums
                     (id TEXT PRIMARY KEY, name TEXT, artist TEXT, last_checked DATETIME)''')
        c.execute('''CREATE TABLE IF NOT EXISTS metadata
                     (key TEXT PRIMARY KEY, value TEXT)''')
        conn.commit()
        conn.close()

    def get_album_tracks(self, album_id):
        tracks = []
        results = self.sp.album_tracks(album_id)
        tracks.extend(results['items'])
        while results['next']:
            results = self.sp.next(results)
            tracks.extend(results['items'])
        return tracks

    def check_album_conditions(self, album_id):
        album = self.sp.album(album_id)
        if 'various artists' in album['artists'][0]['name'].lower():
            return False

        album_tracks = self.get_album_tracks(album_id)
        total_tracks = len(album_tracks)
        
        if total_tracks <= 3:
            return False  # Don't add albums with 3 or fewer tracks

        listened_tracks = 0
        tracks_listened_3_times = 0

        for track in album_tracks:
            listen_count = self.get_track_listen_count(track['name'], track['artists'][0]['name'])
            if listen_count > 0:
                listened_tracks += 1
            if listen_count >= 3:
                tracks_listened_3_times += 1

        if total_tracks <= 6:
            return listened_tracks == total_tracks  # All tracks must be listened to at least once

        # For albums with 7 or more tracks
        condition1 = listened_tracks >= 0.75 * total_tracks
        condition2 = tracks_listened_3_times >= 3

        return condition1 or condition2

    def get_track_listen_count(self, track_name, artist_name):
        normalized_track = normalize_string(track_name)
        normalized_artist = normalize_string(artist_name)
        
        conn = sqlite3.connect(LASTFM_DB_FILE)
        c = conn.cursor()
        c.execute("SELECT listen_count FROM tracks WHERE name = ? AND artist = ?", 
                  (normalized_track, normalized_artist))
        result = c.fetchone()
        conn.close()

        return result[0] if result else 0

    def save_album_to_library(self, album_id):
        try:
            album = self.sp.album(album_id)
            album_name = album['name']
            artist_name = album['artists'][0]['name']

            # Check for duplicates
            duplicates = self.get_duplicate_albums(album_name, artist_name)
            if duplicates:
                chosen_album = self.choose_album_to_keep(duplicates + [album_id])
                if chosen_album != album_id:
                    logging.info(f"Not saving album {album_id} as a better version exists")
                    return
                else:
                    # Remove other duplicates
                    for dup in duplicates:
                        if dup != album_id:
                            self.sp.current_user_saved_albums_delete([dup])
                            logging.info(f"Removed duplicate album {dup}")

            self.sp.current_user_saved_albums_add([album_id])
            logging.info(f"Saved album {album_id} to library")
            self.update_saved_album(album_id, album_name, artist_name)
        except Exception as e:
            logging.error(f"Error saving album {album_id}: {e}")

    def get_duplicate_albums(self, album_name, artist_name):
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute("SELECT id FROM saved_albums WHERE name = ? AND artist = ?", (album_name, artist_name))
        duplicates = [row[0] for row in c.fetchall()]
        conn.close()
        return duplicates

    def choose_album_to_keep(self, album_ids):
        max_liked_songs = -1
        albums_with_max = []

        for album_id in album_ids:
            liked_songs = self.count_liked_songs(album_id)
            if liked_songs > max_liked_songs:
                max_liked_songs = liked_songs
                albums_with_max = [album_id]
            elif liked_songs == max_liked_songs:
                albums_with_max.append(album_id)

        return random.choice(albums_with_max)

    def count_liked_songs(self, album_id):
        tracks = self.get_album_tracks(album_id)
        liked_count = 0
        for track in tracks:
            if self.sp.current_user_saved_tracks_contains([track['id']])[0]:
                liked_count += 1
        return liked_count

    def update_saved_album(self, album_id, album_name, artist_name):
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        c.execute("INSERT OR REPLACE INTO saved_albums (id, name, artist, last_checked) VALUES (?, ?, ?, ?)",
                  (album_id, album_name, artist_name, now))
        conn.commit()
        conn.close()

    def get_last_update_time(self):
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute("SELECT value FROM metadata WHERE key = 'last_update'")
        result = c.fetchone()
        conn.close()
        if result and result[0]:
            return datetime.fromisoformat(result[0]).astimezone(timezone.utc)
        return None

    def set_last_update_time(self, update_time):
        conn = sqlite3.connect(ALBUM_SAVER_DB_FILE)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                  ('last_update', update_time.astimezone(timezone.utc).isoformat()))
        conn.commit()
        conn.close()

    def process_albums(self, force_full_check=False):
        last_update = self.get_last_update_time()
        
        if last_update is None or force_full_check:
            logging.info("Performing full album check")
            album_ids = self.spotify_ops.get_all_album_ids()
        else:
            logging.info(f"Checking albums updated since {last_update}")
            album_ids = self.spotify_ops.get_album_ids_since(last_update)

        for album_id in album_ids:
            if self.check_album_conditions(album_id):
                self.save_album_to_library(album_id)
            else:
                album = self.sp.album(album_id)
                self.update_saved_album(album_id, album['name'], album['artists'][0]['name'])  # Mark as checked even if not saved

        self.set_last_update_time(datetime.now(timezone.utc))

def main():
    try:
        album_saver = AlbumSaver()
        force_full_check = len(sys.argv) > 1 and sys.argv[1] == '--full'
        album_saver.process_albums(force_full_check)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()