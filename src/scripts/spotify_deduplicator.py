import os
import sys

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from src.utils import normalize_string

import logging
import time
from datetime import datetime, timezone
import sqlite3
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from rapidfuzz import fuzz, process

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='logs/spotify_deduplicator.log', filemode='a')

# Load environment variables
load_dotenv()

# Database file path
SPOTIFY_DB_FILE = os.getenv('SPOTIFY_DB_FILE', 'db/spotify_liked_songs.db')

class SpotifyDeduplicator:
    def __init__(self):
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read user-library-modify"))

    def fetch_all_liked_songs(self):
        offset = 0
        limit = 50
        all_tracks = []

        while True:
            results = self.sp.current_user_saved_tracks(limit=limit, offset=offset)
            tracks = results['items']
            if len(tracks) == 0:
                break
            all_tracks.extend(tracks)
            offset += limit

        logging.info(f"Fetched {len(all_tracks)} liked songs from Spotify.")
        return all_tracks

    def group_duplicates(self, tracks):
        track_map = {}
        for item in tracks:
            track = item['track']
            name = normalize_string(track['name'])
            artist = normalize_string(track['artists'][0]['name'])
            key = f"{artist} {name}"
            if key not in track_map:
                track_map[key] = []
            track_map[key].append(track)
        duplicates = {k: v for k, v in track_map.items() if len(v) > 1}
        logging.info(f"Found {len(duplicates)} groups of duplicate tracks.")
        return duplicates

    def select_preferred_track(self, tracks):
        # Apply your rules to select the preferred track
        # Rule 1: Prefer remastered versions
        remastered_tracks = [t for t in tracks if 'remaster' in t['name'].lower()]
        if remastered_tracks:
            tracks = remastered_tracks

        # Rule 2: Prefer tracks from deluxe/longer albums
        if len(tracks) > 1:
            album_lengths = {}
            for t in tracks:
                album_id = t['album']['id']
                if album_id not in album_lengths:
                    album = self.sp.album(album_id)
                    album_lengths[album_id] = len(album['tracks']['items'])
                t['album_length'] = album_lengths[album_id]
            max_length = max(t['album_length'] for t in tracks)
            tracks = [t for t in tracks if t['album_length'] == max_length]

        # Return the first track if multiple remain
        return tracks[0]

    def deduplicate(self):
        all_tracks = self.fetch_all_liked_songs()
        duplicates = self.group_duplicates(all_tracks)
        tracks_to_remove = []
        for key, tracks in duplicates.items():
            preferred_track = self.select_preferred_track(tracks)
            for t in tracks:
                if t['id'] != preferred_track['id']:
                    tracks_to_remove.append(t['id'])
                    logging.info(f"Removing duplicate track: {t['name']} by {t['artists'][0]['name']}")
        if tracks_to_remove:
            batch_size = 50
            for i in range(0, len(tracks_to_remove), batch_size):
                batch = tracks_to_remove[i:i+batch_size]
                retry_count = 0
                while retry_count < 3:  # Maximum 3 retries
                    try:
                        self.sp.current_user_saved_tracks_delete(tracks=batch)
                        break  # Success, exit the retry loop
                    except spotipy.exceptions.SpotifyException as e:
                        if e.http_status == 429:
                            retry_after = int(e.headers.get('Retry-After', 5))
                            logging.warning(f"Rate limited by Spotify. Retrying after {retry_after} seconds.")
                            time.sleep(retry_after)
                            retry_count += 1
                        else:
                            logging.error(f"Error removing tracks: {e}")
                            break  # Exit the retry loop for non-rate-limiting errors
                    except Exception as e:
                        logging.error(f"Unexpected error removing tracks: {e}")
                        break
                time.sleep(0.1)
            logging.info(f"Removed {len(tracks_to_remove)} duplicate tracks.")
        else:
            logging.info("No duplicates found to remove.")

def main():
    try:
        deduplicator = SpotifyDeduplicator()
        deduplicator.deduplicate()
    except KeyboardInterrupt:
        logging.info("Program interrupted by user. Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
