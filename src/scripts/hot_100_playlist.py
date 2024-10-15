#!/usr/bin/env python3
# hot_100_playlist.py

import os
import sys
import re
import sqlite3
import logging
import random
import requests
import time  # <-- Add this import
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from rapidfuzz import fuzz
from typing import List, Dict, Tuple, Optional

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from src.utils import normalize_string, get_user_input_with_timeout

# Load environment variables
load_dotenv()

# Last.fm API credentials
LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
LASTFM_USER = os.getenv('LASTFM_USER')

# Spotify API credentials
SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI')

# Database file paths
LASTFM_100_DAYS_DB = 'db/lastfm_100_days.db'

# Playlist settings
DEFAULT_PLAYLIST_NAME = "Bobby's Hot 💯"
PLAYLIST_ID_FILE = 'playlist_id.txt'
DEFAULT_TIME_RANGE_DAYS = 100
DEFAULT_TRACK_LIMIT = 100

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/hot_100_playlist.log',
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

class LastFM100DaysDB:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.create_table()

    def create_table(self) -> None:
        with sqlite3.connect(self.db_file) as conn:
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS tracks
                         (id INTEGER PRIMARY KEY AUTOINCREMENT,
                          artist TEXT,
                          name TEXT,
                          album TEXT,
                          play_count INTEGER,
                          last_played TEXT,
                          UNIQUE(artist, name, album))''')
            conn.commit()
        logging.info("Initialized tracks table in Last.fm 100 Days database.")

    def update_tracks(self, tracks: List[Dict]) -> None:
        with sqlite3.connect(self.db_file) as conn:
            c = conn.cursor()
            # Clear existing data
            c.execute("DELETE FROM tracks")
            for track in tracks:
                c.execute('''INSERT OR REPLACE INTO tracks 
                             (artist, name, album, play_count, last_played)
                             VALUES (?, ?, ?, ?, ?)''',
                          (track['artist'], track['name'], track['album'], 
                           track['play_count'], track['last_played'].isoformat()))
            conn.commit()
        logging.info(f"Updated database with {len(tracks)} tracks.")

    def remove_old_tracks(self, cut_off_date: datetime) -> None:
        with sqlite3.connect(self.db_file) as conn:
            c = conn.cursor()
            cut_off_date_str = cut_off_date.isoformat()
            c.execute("DELETE FROM tracks WHERE last_played < ?", (cut_off_date_str,))
            removed = c.rowcount
            conn.commit()
        logging.info(f"Removed {removed} tracks older than {cut_off_date_str}.")

    def get_all_tracks(self) -> List[Tuple]:
        with sqlite3.connect(self.db_file) as conn:
            c = conn.cursor()
            c.execute('''SELECT artist, name, album, play_count FROM tracks
                         ORDER BY play_count DESC, last_played DESC''')
            all_tracks = c.fetchall()
        logging.info(f"Retrieved {len(all_tracks)} tracks from the database.")
        return all_tracks

def get_lastfm_tracks(from_date: datetime, to_date: datetime) -> List[Dict]:
    url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'user.getrecenttracks',
        'user': LASTFM_USER,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': 200,
        'from': int(from_date.timestamp()),
        'to': int(to_date.timestamp())
    }

    all_tracks = []
    page = 1

    logging.info("Starting to fetch tracks from Last.fm...")

    while True:
        params['page'] = page
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if 'error' in data:
                logging.error(f"Error fetching Last.fm tracks: {data['message']}")
                break

            tracks = data['recenttracks']['track']
            if not isinstance(tracks, list):
                tracks = [tracks]
            all_tracks.extend(tracks)

            total_pages = int(data['recenttracks']['@attr']['totalPages'])
            logging.info(f"Fetched page {page} of {total_pages} ({len(tracks)} tracks)")

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.2)  # Sleep to respect API rate limits

        except requests.RequestException as e:
            logging.error(f"Network error when fetching Last.fm tracks: {e}")
            time.sleep(5)
            continue  # Retry on network errors
        except Exception as e:
            logging.error(f"Unexpected error in get_lastfm_tracks: {e}", exc_info=True)
            break

    logging.info(f"Fetched a total of {len(all_tracks)} tracks from Last.fm")
    return all_tracks

def process_lastfm_tracks(tracks: List[Dict]) -> List[Dict]:
    processed_tracks = {}
    now = datetime.now(timezone.utc)

    logging.info("Processing Last.fm tracks...")

    for track in tracks:
        try:
            if 'date' not in track:
                continue  # Skip currently playing track

            artist = track['artist']['#text']
            name = track['name']
            album = track['album']['#text'] if track['album']['#text'] else 'Unknown Album'
            date = datetime.fromtimestamp(int(track['date']['uts']), tz=timezone.utc)

            if (now - date).days > DEFAULT_TIME_RANGE_DAYS:
                continue

            key = (artist.lower(), name.lower(), album.lower())
            if key in processed_tracks:
                processed_tracks[key]['play_count'] += 1
                processed_tracks[key]['last_played'] = max(processed_tracks[key]['last_played'], date)
            else:
                processed_tracks[key] = {
                    'artist': artist,
                    'name': name,
                    'album': album,
                    'play_count': 1,
                    'last_played': date
                }
        except Exception as e:
            logging.error(f"Error processing track: {track}", exc_info=True)
            continue

    logging.info(f"Processed {len(processed_tracks)} unique tracks.")
    return list(processed_tracks.values())

def get_or_create_playlist(sp: spotipy.Spotify, name: str) -> str:
    playlist_id_file = PLAYLIST_ID_FILE

    # Check if playlist ID is stored in file
    if os.path.exists(playlist_id_file):
        with open(playlist_id_file, 'r') as f:
            playlist_id = f.read().strip()
        # Verify that the playlist still exists and is accessible
        try:
            playlist = sp.playlist(playlist_id)
            if playlist['name'] == name:
                logging.info(f"Found existing playlist: {playlist['name']}")
                return playlist_id
            else:
                logging.info(f"Updating playlist name to: {name}")
                sp.user_playlist_change_details(sp.me()['id'], playlist_id, name=name)
                return playlist_id
        except spotipy.exceptions.SpotifyException as e:
            logging.warning(f"Playlist ID not valid or playlist not found. Creating new playlist.")

    # Playlist ID not found or invalid, create a new playlist
    logging.info(f"Creating new playlist: {name}")
    user_id = sp.me()['id']
    playlist = sp.user_playlist_create(user_id, name, public=False)
    # Store the new playlist ID
    with open(playlist_id_file, 'w') as f:
        f.write(playlist['id'])
    return playlist['id']

def update_playlist(sp: spotipy.Spotify, playlist_id: str, track_ids: List[str]) -> None:
    logging.info(f"Updating playlist {playlist_id} with {len(track_ids)} tracks")

    # Randomize the order of tracks
    random.shuffle(track_ids)

    # Clear the playlist first
    sp.playlist_replace_items(playlist_id, [])
    # Spotify API allows up to 100 tracks per request
    for i in range(0, len(track_ids), 100):
        batch = track_ids[i:i+100]
        sp.playlist_add_items(playlist_id, batch)
        logging.info(f"Added {len(batch)} tracks to playlist")
        time.sleep(0.2)  # Sleep to respect API rate limits

    logging.info("Playlist update completed with randomized order")

def main():
    try:
        # Prompt the user for playlist name and time range
        playlist_name = DEFAULT_PLAYLIST_NAME
        time_range_days = DEFAULT_TIME_RANGE_DAYS
        track_limit = DEFAULT_TRACK_LIMIT

        # Initialize Spotify client
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="playlist-modify-private,playlist-modify-public"))

        # Initialize Last.fm database
        lastfm_db = LastFM100DaysDB(LASTFM_100_DAYS_DB)

        # Calculate date range
        end_date = datetime.utcnow().replace(tzinfo=timezone.utc)
        start_date = end_date - timedelta(days=time_range_days)

        # Fetch Last.fm tracks
        lastfm_tracks = get_lastfm_tracks(start_date, end_date)
        processed_tracks = process_lastfm_tracks(lastfm_tracks)

        # Update database
        lastfm_db.update_tracks(processed_tracks)
        lastfm_db.remove_old_tracks(start_date)

        # Get all tracks sorted by play count descending
        all_tracks = lastfm_db.get_all_tracks()

        # Build a dictionary to keep track of the highest play count for each track
        track_dict = {}
        for artist, name, album, play_count in all_tracks:
            key = (normalize_string(artist), normalize_string(name))
            if key not in track_dict or play_count > track_dict[key]['play_count']:
                track_dict[key] = {
                    'artist': artist,
                    'name': name,
                    'album': album,
                    'play_count': play_count
                }

        # Get the top tracks based on play count
        sorted_tracks = sorted(track_dict.values(), key=lambda x: x['play_count'], reverse=True)

        # Build the list of tracks to add to the playlist
        spotify_track_ids = []
        track_count = 0
        for track_info in sorted_tracks:
            if track_count >= track_limit:
                break

            artist = track_info['artist']
            name = track_info['name']
            album = track_info['album']
            play_count = track_info['play_count']

            logging.info(f"Searching for track: {artist} - {name} (Album: {album}, Play count: {play_count})")

            # Search for the track on Spotify using fuzzy matching
            query = f"track:{name} artist:{artist}"
            try:
                results = sp.search(q=query, type='track', limit=10)
                best_match_id = None
                highest_score = 0
                for item in results['tracks']['items']:
                    spotify_artist = item['artists'][0]['name']
                    spotify_name = item['name']
                    artist_score = fuzz.token_sort_ratio(normalize_string(artist), normalize_string(spotify_artist))
                    name_score = fuzz.token_sort_ratio(normalize_string(name), normalize_string(spotify_name))
                    total_score = (artist_score + name_score) / 2
                    if total_score > highest_score:
                        highest_score = total_score
                        best_match_id = item['id']
                if highest_score > 80 and best_match_id:
                    spotify_track_ids.append(best_match_id)
                    track_count += 1
                    logging.info(f"Found match with score {highest_score}: {spotify_artist} - {spotify_name}")
                else:
                    logging.warning(f"No suitable match found for: {artist} - {name}")
            except Exception as e:
                logging.error(f"Error searching for track on Spotify: {e}", exc_info=True)
                continue

        # Ensure we only have the desired number of tracks
        spotify_track_ids = spotify_track_ids[:track_limit]

        # Get or create playlist
        playlist_id = get_or_create_playlist(sp, playlist_name)

        # Update playlist with randomized order
        update_playlist(sp, playlist_id, spotify_track_ids)

        logging.info(f"Playlist '{playlist_name}' updated with {len(spotify_track_ids)} tracks in random order.")

        # Get the playlist link
        playlist_info = sp.playlist(playlist_id)
        playlist_link = playlist_info['external_urls']['spotify']

        print(f"\nPlaylist updated successfully!")
        print(f"You can access your playlist '{playlist_name}' here: {playlist_link}")
        logging.info(f"Playlist link: {playlist_link}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
