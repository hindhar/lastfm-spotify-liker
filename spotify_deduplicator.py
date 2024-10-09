import os
import sys
import logging
import time
import re
from datetime import datetime, timezone
import sqlite3
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from rapidfuzz import fuzz, process

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SpotifyDeduplicator:
    def __init__(self):
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read user-library-modify"))

    def normalize_string(self, s):
        s = s.lower().strip()
        # Remove content in parentheses or brackets
        s = re.sub(r'\s*[\(\[\{].*?[\)\]\}]', '', s)
        # Remove version-specific keywords
        keywords = ['remastered', 'live', 'acoustic', 'mono', 'stereo', 'version', 'edit', 'feat.', 'featuring', 'from']
        for keyword in keywords:
            s = s.replace(keyword, '')
        # Remove extra punctuation
        s = re.sub(r'[^a-zA-Z0-9\s]', '', s)
        # Remove extra whitespace
        s = re.sub(r'\s+', ' ', s)
        return s.strip()

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
            name = self.normalize_string(track['name'])
            artist = self.normalize_string(track['artists'][0]['name'])
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
                try:
                    self.sp.current_user_saved_tracks_delete(tracks=batch)
                except spotipy.exceptions.SpotifyException as e:
                    logging.error(f"Error removing tracks: {e}")
                    time.sleep(2)
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
