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
from src.utils import normalize_string
from src.database import Database

# Load environment variables
load_dotenv()

# Use environment variable for database file path
SPOTIFY_DB_FILE = os.getenv('SPOTIFY_DB_FILE', 'db/spotify_liked_songs.db')
LASTFM_DB_FILE = os.getenv('LASTFM_DB_FILE', 'db/lastfm_history.db')

class SpotifyOperations:
    """Handles operations related to Spotify, including searching and liking tracks."""

    def __init__(self, db_file=SPOTIFY_DB_FILE):
        """Initialize the Spotify operations and create necessary database tables."""
        self.sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read user-library-modify"))
        self.db_file = db_file
        self.create_table()

    def create_table(self):
        """Create necessary tables in the database if they don't exist."""
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
        
        # Create unfound_tracks table if it doesn't exist
        c.execute('''CREATE TABLE IF NOT EXISTS unfound_tracks
                     (artist TEXT, name TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                      PRIMARY KEY (artist, name))''')
        
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
        
        # Check for duplicates
        liked_set = set()
        duplicates = set()
        for name, artist in liked_songs:
            if (name, artist) in liked_set:
                duplicates.add((name, artist))
            else:
                liked_set.add((name, artist))
        
        logging.info(f"Retrieved {len(liked_set)} unique liked songs from local database")
        if duplicates:
            logging.warning(f"Found {len(duplicates)} duplicate entries in liked_songs table")
            for name, artist in list(duplicates)[:5]:  # Log first 5 duplicates
                logging.warning(f"Duplicate: {artist} - {name}")
        
        return liked_set

    def remove_duplicates(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("""
            DELETE FROM liked_songs
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM liked_songs
                GROUP BY name, artist
            )
        """)
        removed = c.rowcount
        conn.commit()
        conn.close()
        logging.info(f"Removed {removed} duplicate entries from liked_songs table")

    def like_tracks(self, track_ids):
        batch_size = 50  # Spotify allows up to 50 tracks to be liked at once
        for i in range(0, len(track_ids), batch_size):
            batch = track_ids[i:i+batch_size]
            try:
                self.sp.current_user_saved_tracks_add(tracks=batch)
                logging.info(f"Liked {len(batch)} tracks")
                # After liking tracks, add them to the local database
                self._save_newly_liked_tracks(batch)
                logging.info(f"Saved {len(batch)} newly liked tracks to local database")
            except spotipy.exceptions.SpotifyException as e:
                logging.error(f"Error liking tracks: {e}")
                time.sleep(2)  # Wait before retrying
            time.sleep(0.1)  # Add a small delay to avoid rate limiting

    def _save_newly_liked_tracks(self, track_ids):
        # Fetch details of the newly liked tracks in batches
        batch_size = 50
        tracks = []
        for i in range(0, len(track_ids), batch_size):
            batch = track_ids[i:i+batch_size]
            response = self.sp.tracks(batch)
            for track in response['tracks']:
                tracks.append({
                    'added_at': datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
                    'track': track
                })
            time.sleep(0.1)  # To respect rate limits

        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        for item in tracks:
            track = item['track']
            name = normalize_string(track['name'])
            artist = normalize_string(track['artists'][0]['name'])
            album = normalize_string(track['album']['name'])
            album_id = track['album']['id']
            
            # Check if the track already exists
            c.execute("SELECT id FROM liked_songs WHERE name = ? AND artist = ?", (name, artist))
            existing = c.fetchone()
            if existing:
                logging.info(f"Track already in database: {artist} - {name}")
            else:
                c.execute("INSERT INTO liked_songs VALUES (?, ?, ?, ?, ?, ?)",
                          (track['id'], name, artist, album, album_id, item['added_at']))
                logging.info(f"Saving newly liked track: {artist} - {name}")
        
        conn.commit()
        conn.close()

    def search_track(self, name, artist):
        """Search for a track on Spotify and return its ID if found."""
        # Check cache first
        cached_id = self.get_cached_track_id(name, artist)
        if cached_id is not None:
            return cached_id

        # Define query variations
        queries = [
            f"track:{name} artist:{artist}",
            f"track:{name}",
            f"{name} {artist}"
        ]

        for query in queries:
            track_id = self._search_and_match(query, name, artist)
            if track_id:
                self.cache_track_id(name, artist, track_id)
                return track_id

        # Cache negative result
        self.cache_track_id(name, artist, None)
        return None

    def _search_and_match(self, query, name, artist):
        """Perform a search query and match results against the given name and artist."""
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
            return result[0] if result[0] != 'NOT_FOUND' else None
        return None

    def cache_track_id(self, name, artist, track_id):
        if track_id is None:
            track_id = 'NOT_FOUND'
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO search_cache (name, artist, track_id) VALUES (?, ?, ?)",
                  (name, artist, track_id))
        conn.commit()
        conn.close()

    def find_tracks_to_like(self, lastfm_tracks, min_play_count=5):
        spotify_liked = self.get_liked_songs_set()
        tracks_to_like = []
        processed_tracks = []

        for track in lastfm_tracks:
            if track[2] >= min_play_count:
                name, artist, play_count = track[1], track[0], track[2]
                processed_tracks.append((artist, name, play_count))

                logging.info(f"Checking track: {artist} - {name} (Play count: {play_count})")

                if not self.is_track_liked(name, artist, spotify_liked):
                    logging.info(f"Track not liked on Spotify: {artist} - {name}")
                    track_id = self.search_track(name, artist)
                    if track_id:
                        if not self.is_track_in_database(track_id):
                            tracks_to_like.append(track_id)
                            logging.info(f"Will like: {artist} - {name}")
                        else:
                            logging.info(f"Track already in database, skipping: {artist} - {name}")
                    else:
                        logging.info(f"Couldn't find track on Spotify: {artist} - {name}")
                        self.add_unfound_track(artist, name)
                else:
                    logging.info(f"Track already liked, skipping: {artist} - {name}")
            else:
                logging.info(f"Skipping track with low play count: {track[0]} - {track[1]} (Play count: {track[2]})")
                break

        # Mark processed tracks in the Last.fm database
        lastfm_db = Database(db_file=LASTFM_DB_FILE)
        lastfm_db.mark_tracks_as_processed(processed_tracks)

        logging.info(f"Found {len(processed_tracks)} tracks with {min_play_count}+ plays on Last.fm")
        logging.info(f"Of these, {len(tracks_to_like)} are new tracks to like on Spotify")
        return tracks_to_like

    def is_track_liked(self, name, artist, spotify_liked):
        normalized_name = normalize_string(name)
        normalized_artist = normalize_string(artist)
        return any(
            fuzz.ratio(normalized_name, liked_name) > 90 and
            fuzz.ratio(normalized_artist, liked_artist) > 90
            for liked_name, liked_artist in spotify_liked
        )

    def is_track_in_database(self, track_id):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT id FROM liked_songs WHERE id = ?", (track_id,))
        result = c.fetchone()
        conn.close()
        return result is not None

    def add_unfound_track(self, artist, name):
        """Add a track that couldn't be found on Spotify to the unfound_tracks table."""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO unfound_tracks (artist, name) VALUES (?, ?)", (artist, name))
        conn.commit()
        conn.close()

    def get_last_update_time(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT value FROM metadata WHERE key = 'last_update'")
        result = c.fetchone()
        conn.close()
        if result and result[0]:
            # Ensure the datetime is timezone-aware and in UTC
            return datetime.fromisoformat(result[0]).astimezone(timezone.utc)
        return None

    def set_last_update_time(self, update_time):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        # Ensure update_time is in ISO format with timezone info
        c.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
                  ('last_update', update_time.astimezone(timezone.utc).isoformat()))
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
                    # Since the list is in descending order, we can stop here
                    return new_tracks
                new_tracks.append(item)
            offset += limit
        return new_tracks

    def update_liked_songs(self, force_full_fetch=False):
        last_update = self.get_last_update_time()
        if last_update is None or force_full_fetch:
            logging.info("Fetching all liked songs from Spotify.")
            total_tracks = self.fetch_all_liked_songs()
            logging.info(f"Fetched {total_tracks} liked songs from Spotify.")
            self.set_last_update_time(datetime.now(timezone.utc))
            
            # Add logging to verify database update
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM liked_songs")
            total_liked_songs = c.fetchone()[0]
            conn.close()
            logging.info(f"Total liked songs in local database: {total_liked_songs}")
            
            return total_tracks
        else:
            new_tracks = self.fetch_new_liked_songs()
            if not new_tracks:
                logging.info("No new liked songs to update.")
                return 0
            self._save_tracks_to_db(new_tracks)
            latest_added_at = max(
                datetime.strptime(item['added_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                for item in new_tracks
            )
            self.set_last_update_time(latest_added_at)
            logging.info(f"Updated last update time to: {latest_added_at.isoformat()}")
            
            # Add logging to verify database update
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM liked_songs")
            total_liked_songs = c.fetchone()[0]
            conn.close()
            logging.info(f"Total liked songs in local database after update: {total_liked_songs}")
            
            return len(new_tracks)

    def _save_tracks_to_db(self, tracks):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        for item in tracks:
            track = item['track']
            name = normalize_string(track['name'])
            artist = normalize_string(track['artists'][0]['name'])
            album = normalize_string(track['album']['name'])
            album_id = track['album']['id']
            c.execute("INSERT OR REPLACE INTO liked_songs VALUES (?, ?, ?, ?, ?, ?)",
                      (track['id'], name, artist, album, album_id, item['added_at']))
        conn.commit()
        conn.close()

    def verify_local_database(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM liked_songs")
        count = c.fetchone()[0]
        logging.info(f"Local database contains {count} liked songs")
        c.execute("SELECT name, artist FROM liked_songs LIMIT 5")
        sample = c.fetchall()
        logging.info(f"Sample of liked songs in database: {sample}")
        conn.close()

    def log_unfound_tracks(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT artist, name FROM unfound_tracks")
        unfound = c.fetchall()
        conn.close()
        logging.info(f"Unfound tracks in database: {len(unfound)}")
        for artist, name in unfound[:5]:  # Log first 5 for brevity
            logging.info(f"Unfound: {artist} - {name}")

    def get_all_album_ids(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT DISTINCT album_id FROM liked_songs")
        album_ids = [row[0] for row in c.fetchall()]
        conn.close()
        return album_ids

    def get_album_ids_since(self, since_datetime):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT DISTINCT album_id FROM liked_songs WHERE added_at > ?",
                  (since_datetime.isoformat(),))
        album_ids = [row[0] for row in c.fetchall()]
        conn.close()
        return album_ids