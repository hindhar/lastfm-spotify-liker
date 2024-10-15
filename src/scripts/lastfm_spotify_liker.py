#!/usr/bin/env python3
# last_fm_spotify_liker.py

import os
import sys
import logging
import time
import sqlite3
import threading
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from typing import Optional, List, Dict, Any

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from src.database import Database
from src.spotify_operations import SpotifyOperations
from src.utils import normalize_string, get_user_input_with_timeout

# Load environment variables
load_dotenv()

# Last.fm API credentials
LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
LASTFM_USER = os.getenv('LASTFM_USER')
MIN_PLAY_COUNT = int(os.getenv('MIN_PLAY_COUNT', 5))

# Database file paths
LASTFM_DB_FILE = os.getenv('LASTFM_DB_FILE', 'db/lastfm_history.db')
SPOTIFY_DB_FILE = os.getenv('SPOTIFY_DB_FILE', 'db/spotify_liked_songs.db')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/lastfm_spotify_liker.log',
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def get_new_lastfm_tracks(db: Database, from_timestamp: Optional[int] = None) -> int:
    """
    Fetch new tracks from Last.fm and update the local database.

    Args:
        db (Database): The database instance to update.
        from_timestamp (int, optional): Unix timestamp to fetch tracks from.

    Returns:
        int: Number of tracks fetched and processed.
    """
    url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'user.getrecenttracks',
        'user': LASTFM_USER,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': 200,
    }
    if from_timestamp:
        params['from'] = from_timestamp
        logging.info(f"Fetching tracks from timestamp: {from_timestamp} ({datetime.fromtimestamp(from_timestamp, tz=timezone.utc).isoformat()})")
    else:
        logging.info("Fetching all tracks without a 'from' timestamp.")

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

            total_pages = int(data['recenttracks']['@attr'].get('totalPages', 1))
            logging.info(f"Fetched page {page} of {total_pages} ({len(tracks)} tracks)")

            if page >= total_pages:
                break
            page += 1

        except requests.RequestException as e:
            logging.error(f"Network error when fetching Last.fm tracks: {e}")
            time.sleep(5)
            continue  # Retry on network errors
        except Exception as e:
            logging.error(f"Unexpected error in get_new_lastfm_tracks: {e}", exc_info=True)
            break

    processed_count = 0
    for track in all_tracks:
        try:
            if 'date' in track:
                track_date = datetime.fromtimestamp(int(track['date']['uts']), tz=timezone.utc)
            else:
                track_date = datetime.utcnow().replace(tzinfo=timezone.utc)  # Use accurate UTC time

            artist_name = track['artist']['#text'] if isinstance(track['artist'], dict) else track['artist']
            album_name = track['album']['#text'] if isinstance(track.get('album'), dict) else track.get('album', '')
            track_name = track['name']

            db.add_or_update_track({
                'artist': artist_name,
                'name': track_name,
                'album': album_name,
                'date': track_date,
                'mbid': track.get('mbid', '')
            })
            processed_count += 1
        except Exception as e:
            logging.error(f"Error processing track: {track}", exc_info=True)
            continue

    logging.info(f"Processed {processed_count} tracks from Last.fm")
    return processed_count

def main():
    try:
        lastfm_db = Database(db_file=LASTFM_DB_FILE)
        spotify_ops = SpotifyOperations(db_file=SPOTIFY_DB_FILE)

        # Ensure the processed column exists
        lastfm_db.add_processed_column()

        spotify_ops.verify_local_database()  # Verify database at the start

        # Check for and remove duplicates
        spotify_ops.remove_duplicates()

        # Check if databases exist
        if not os.path.exists(LASTFM_DB_FILE) or not os.path.exists(SPOTIFY_DB_FILE):
            logging.info("Database files not found. Performing full update without prompting.")
            force_full_fetch = True
            last_update = None
        else:
            # Prompt the user for full or incremental update
            prompt = "Do you want to perform a full update or an incremental update? (Enter 'full' or 'incremental' within 10 seconds, default is 'incremental'): "
            user_choice = get_user_input_with_timeout(prompt, timeout=10)

            if user_choice.lower() == 'full':
                force_full_fetch = True
                logging.info("User selected full update.")
                last_update = None
            else:
                force_full_fetch = False
                last_update = lastfm_db.get_last_update_time()
                if last_update:
                    # Ensure last_update is timezone-aware and in UTC
                    if last_update.tzinfo is None:
                        last_update = last_update.replace(tzinfo=timezone.utc)
                    logging.info(f"Proceeding with incremental update since {last_update.isoformat()}")
                else:
                    logging.info("No previous update time found. Performing full update.")
                    force_full_fetch = True

        # Update Last.fm tracks
        if force_full_fetch:
            logging.info("Fetching all Last.fm tracks (full update)")
            new_tracks_count = get_new_lastfm_tracks(lastfm_db)
        else:
            from_timestamp = int(last_update.timestamp())
            logging.info(f"Fetching Last.fm tracks since {last_update.isoformat()} (timestamp: {from_timestamp})")
            new_tracks_count = get_new_lastfm_tracks(lastfm_db, from_timestamp)
        logging.info(f"Added or updated {new_tracks_count} tracks from Last.fm")

        # Update Spotify liked songs
        logging.info("Updating Spotify liked songs...")
        spotify_ops.update_liked_songs()
        logging.info("Spotify liked songs updated.")

        # Get frequently played tracks from Last.fm
        frequently_played = lastfm_db.get_frequently_played_tracks(MIN_PLAY_COUNT)
        logging.info(f"Found {len(frequently_played)} tracks played more than {MIN_PLAY_COUNT} times on Last.fm")

        # Find tracks to be liked
        tracks_to_like = spotify_ops.find_tracks_to_like(frequently_played, min_play_count=MIN_PLAY_COUNT)

        if tracks_to_like:
            logging.info(f"Found {len(tracks_to_like)} new tracks to like on Spotify:")
            for track_id in tracks_to_like:
                try:
                    logging.info(f"Fetching track info for track ID: {track_id}")
                    # Set a timeout for the API call
                    track_info = spotify_ops.sp.track(track_id)
                    logging.info(f"  - {track_info['artists'][0]['name']} - {track_info['name']}")
                except Exception as e:
                    logging.error(f"Error fetching track info for track ID {track_id}: {e}", exc_info=True)
                    continue

            try:
                # Like the tracks on Spotify
                spotify_ops.like_tracks(tracks_to_like)
                logging.info(f"Finished liking {len(tracks_to_like)} tracks on Spotify")
            except Exception as e:
                logging.error(f"Error liking tracks on Spotify: {e}", exc_info=True)
        else:
            logging.info("No new tracks to like on Spotify")

        # Update Spotify liked songs again to ensure local database is current
        spotify_ops.update_liked_songs()

        # Check for and remove duplicates again
        spotify_ops.remove_duplicates()

        spotify_ops.verify_local_database()  # Verify database at the end

    except sqlite3.OperationalError as e:
        logging.error(f"Database error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()