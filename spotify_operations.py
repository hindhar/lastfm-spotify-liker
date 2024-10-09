# spotify_operations.py

import os
import sys
import re
import logging
import time
from datetime import datetime, timezone
import sqlite3
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from rapidfuzz import fuzz, process
from utils import normalize_string

load_dotenv()

class SpotifyOperations:
    def __init__(self, db_file='spotify_liked_songs.db'):
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read user-library-modify"))
        self.db_file = db_file
        self.create_table()

    def create_table(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        
        # Create the liked_songs table if it doesn't exist
        c.execute('''CREATE TABLE IF NOT EXISTS liked_songs
                     (id TEXT PRIMARY KEY, name TEXT, artist TEXT, album TEXT, album_id TEXT, added_at TEXT)''')
        
        # Create metadata table if it doesn't exist
        c.execute('''CREATE TABLE IF NOT EXISTS metadata
                     (key TEXT PRIMARY KEY, value TEXT)''')
        
        # Create search_cache table if it doesn't exist
        c.execute('''CREATE TABLE IF NOT EXISTS search_cache
                     (name TEXT, artist TEXT, track_id TEXT, PRIMARY KEY (name, artist))''')
        
        conn.commit()
        conn.close()

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

        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()

        for item in all_tracks:
            track = item['track']
            name = normalize_string(track['name'])
            artist = normalize_string(track['artists'][0]['name'])
            album = normalize_string(track['album']['name'])
            album_id = track['album']['id']
            c.execute("INSERT OR REPLACE INTO liked_songs VALUES (?, ?, ?, ?, ?, ?)",
                      (track['id'], name, artist, album, album_id, item['added_at']))

        conn.commit()
        conn.close()

        return len(all_tracks)

    def get_liked_songs_set(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT name, artist FROM liked_songs")
        liked_songs = c.fetchall()
        conn.close()
        # Use normalized strings for comparison
        return set((name, artist) for name, artist in liked_songs)

    def like_tracks(self, track_ids):
        batch_size = 50  # Spotify allows up to 50 tracks to be liked at once
        for i in range(0, len(track_ids), batch_size):
            batch = track_ids[i:i+batch_size]
            try:
                self.sp.current_user_saved_tracks_add(tracks=batch)
                logging.info(f"Liked {len(batch)} tracks")
            except spotipy.exceptions.SpotifyException as e:
                logging.error(f"Error liking tracks: {e}")
                time.sleep(2)  # Wait before retrying
            time.sleep(0.1)  # Add a small delay to avoid rate limiting

    def search_track(self, name, artist):
        # Inputs are expected to be normalized
        # Remove extra descriptors from the track name for query purposes
        name_query = re.sub(r' - .*$', '', name)  # Remove content after ' - '
        name_query = name_query.strip()

        # Try initial search with both track and artist
        query = f"track:{name_query} artist:{artist}"
        track_id = self._search_and_match(query, name, artist)
        if track_id:
            self.cache_track_id(name, artist, track_id)
            return track_id

        # Try searching by track name only
        query = f"track:{name_query}"
        track_id = self._search_and_match(query, name, artist)
        if track_id:
            self.cache_track_id(name, artist, track_id)
            return track_id

        # Cache negative result
        self.cache_track_id(name, artist, None)
        return None

    def _search_and_match(self, query, name, artist):
        try:
            results = self.sp.search(q=query, type='track', limit=10)
            if results['tracks']['items']:
                best_match = None
                highest_score = 0
                for item in results['tracks']['items']:
                    spotify_name = normalize_string(item['name'])
                    spotify_artist = normalize_string(item['artists'][0]['name'])
                    name_score = fuzz.token_sort_ratio(name, spotify_name)
                    artist_score = fuzz.token_sort_ratio(artist, spotify_artist)
                    total_score = (name_score + artist_score) / 2
                    if total_score > highest_score:
                        highest_score = total_score
                        best_match = item['id']
                if highest_score > 80:  # Threshold can be adjusted
                    return best_match
            return None
        except spotipy.exceptions.SpotifyException as e:
            logging.error(f"Spotify API error: {e}")
            time.sleep(2)  # Wait before retrying
            return None
        finally:
            time.sleep(0.1)  # Small delay to respect rate limits

    def get_cached_track_id(self, name, artist):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT track_id FROM search_cache WHERE name = ? AND artist = ?", (name, artist))
        result = c.fetchone()
        conn.close()
        if result:
            return result[0]  # This could be None if previously not found
        return None

    def cache_track_id(self, name, artist, track_id):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO search_cache (name, artist, track_id) VALUES (?, ?, ?)",
                  (name, artist, track_id))
        conn.commit()
        conn.close()

    def find_tracks_to_like(self, lastfm_tracks, min_play_count=5):
        spotify_liked = self.get_liked_songs_set()
        tracks_to_like = []

        for track in lastfm_tracks:
            if track[2] >= min_play_count:
                name = track[1]  # Already normalized when stored in the database
                artist = track[0]  # Already normalized when stored in the database

                # Check if the track is already liked
                if (name, artist) in spotify_liked:
                    continue  # Skip already liked tracks

                # Proceed to search and like the track
                track_id = self.search_track(name, artist)
                if track_id:
                    tracks_to_like.append(track_id)
                    logging.info(f"Will like: {artist} - {name}")
                else:
                    logging.info(f"Couldn't find track on Spotify: {artist} - {name}")
            else:
                break

        return tracks_to_like

    def get_last_update_time(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT value FROM metadata WHERE key = 'last_update'")
        result = c.fetchone()
        conn.close()
        if result and result[0]:
            return datetime.fromisoformat(result[0]).astimezone(timezone.utc)
        return None

    def set_last_update_time(self, update_time):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                  ('last_update', update_time.isoformat()))
        conn.commit()
        conn.close()

    def fetch_new_liked_songs(self):
        last_update = self.get_last_update_time()
        offset = 0
        limit = 50
        new_tracks = []

        while True:
            results = self.sp.current_user_saved_tracks(limit=limit, offset=offset)
            if not results['items']:
                break

            for item in results['items']:
                added_at = datetime.strptime(item['added_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if last_update and added_at <= last_update:
                    continue  # Skip tracks we've already processed
                new_tracks.append(item)

            offset += limit

        return new_tracks

    def update_liked_songs(self):
        last_update = self.get_last_update_time()
        if last_update is None:
            logging.info("No last update time found, fetching all liked songs from Spotify.")
            total_tracks = self.fetch_all_liked_songs()
            logging.info(f"Fetched {total_tracks} liked songs from Spotify.")
            self.set_last_update_time(datetime.now(timezone.utc))
            return total_tracks
        else:
            new_tracks = self.fetch_new_liked_songs()
            if not new_tracks:
                logging.info("No new liked songs to update.")
                return 0

            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()

            for item in new_tracks:
                track = item['track']
                name = normalize_string(track['name'])
                artist = normalize_string(track['artists'][0]['name'])
                album = normalize_string(track['album']['name'])
                album_id = track['album']['id']
                c.execute("INSERT OR REPLACE INTO liked_songs VALUES (?, ?, ?, ?, ?, ?)",
                          (track['id'], name, artist, album, album_id, item['added_at']))

            conn.commit()
            conn.close()

            latest_added_at = max(
                datetime.strptime(item['added_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                for item in new_tracks
            )
            self.set_last_update_time(latest_added_at)
            logging.info(f"Updated last update time to: {latest_added_at.isoformat()}")
            return len(new_tracks)
