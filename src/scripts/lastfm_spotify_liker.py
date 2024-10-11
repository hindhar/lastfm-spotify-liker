import os
import sys

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

import logging
import time
import sqlite3
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

from src.database import Database
from src.spotify_operations import SpotifyOperations
from src.utils import normalize_string

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('logs/lastfm_spotify_liker.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Load environment variables
load_dotenv()

# Last.fm API credentials
LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
LASTFM_USER = os.getenv('LASTFM_USER')
MIN_PLAY_COUNT = int(os.getenv('MIN_PLAY_COUNT', 5))

# Database file paths
LASTFM_DB_FILE = os.getenv('LASTFM_DB_FILE', 'db/lastfm_history.db')
SPOTIFY_DB_FILE = os.getenv('SPOTIFY_DB_FILE', 'db/spotify_liked_songs.db')

def get_new_lastfm_tracks(db, from_timestamp=None):
    url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'user.getrecenttracks',
        'user': LASTFM_USER,
        'api_key': LASTFM_API_KEY,
        'format': 'json',
        'limit': 200,
        'from': from_timestamp
    }

    all_tracks = []
    page = 1
    total_pages = 1

    while True:
        params['page'] = page
        try:
            response = requests.get(url, params=params)
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
            logging.info(f"Fetched page {page} of {total_pages}")

            if page >= total_pages:
                break
            page += 1

        except requests.RequestException as e:
            logging.error(f"Network error when fetching Last.fm tracks: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error in get_new_lastfm_tracks: {e}")
            logging.exception(e)
            break

    for track in all_tracks:
        try:
            if 'date' in track:
                track['date'] = datetime.fromtimestamp(int(track['date']['uts'])).replace(tzinfo=timezone.utc)
            else:
                track['date'] = datetime.now(timezone.utc)  # For 'now playing' track

            # Safely extract artist name
            artist_name = track['artist']['#text'] if isinstance(track['artist'], dict) else track['artist']

            # Safely extract album name
            album_name = track['album']['#text'] if isinstance(track['album'], dict) else track['album']

            # Safely extract track name
            track_name = track['name']['#text'] if isinstance(track['name'], dict) else track['name']

            db.add_or_update_track({
                'artist': artist_name,
                'name': track_name,
                'album': album_name,
                'date': track['date'],
                'mbid': track.get('mbid', '')
            })
        except Exception as e:
            logging.error(f"Error processing track: {track}")
            logging.exception(e)
            continue

    return len(all_tracks)

def main():
    try:
        lastfm_db = Database(db_file=LASTFM_DB_FILE)
        spotify_ops = SpotifyOperations(db_file=SPOTIFY_DB_FILE)
        
        # Ensure the processed column exists
        lastfm_db.add_processed_column()
        
        spotify_ops.verify_local_database()  # Verify database at the start
        
        # Check for and remove duplicates
        spotify_ops.remove_duplicates()
        
        # Update Last.fm tracks
        last_update = lastfm_db.get_last_update_time()
        if last_update:
            logging.info(f"Fetching Last.fm tracks since {last_update}")
            new_tracks_count = get_new_lastfm_tracks(lastfm_db, int(last_update.timestamp()))
        else:
            logging.info("Fetching all Last.fm tracks (first run)")
            new_tracks_count = get_new_lastfm_tracks(lastfm_db)

        logging.info(f"Added or updated {new_tracks_count} tracks from Last.fm")

        # Update Spotify liked songs
        logging.info("Updating Spotify liked songs...")
        new_liked_songs_count = spotify_ops.update_liked_songs()
        logging.info(f"Added {new_liked_songs_count} new liked songs to the Spotify database")

        # Get frequently played tracks from Last.fm
        frequently_played = lastfm_db.get_frequently_played_tracks(MIN_PLAY_COUNT)
        logging.info(f"Found {len(frequently_played)} tracks played more than {MIN_PLAY_COUNT} times on Last.fm")

        # Find tracks to be liked
        tracks_to_like = spotify_ops.find_tracks_to_like(frequently_played, min_play_count=MIN_PLAY_COUNT)
        
        if tracks_to_like:
            logging.info(f"Found {len(tracks_to_like)} new tracks to like on Spotify:")
            for track_id in tracks_to_like:
                track_info = spotify_ops.sp.track(track_id)
                logging.info(f"  - {track_info['artists'][0]['name']} - {track_info['name']}")
            
            # Like the tracks on Spotify
            spotify_ops.like_tracks(tracks_to_like)
            logging.info(f"Finished liking {len(tracks_to_like)} tracks on Spotify")
        else:
            logging.info("No new tracks to like on Spotify")

        # Update Spotify liked songs again to ensure local database is current
        spotify_ops.update_liked_songs()
        
        # Check for and remove duplicates again
        spotify_ops.remove_duplicates()
        
        spotify_ops.verify_local_database()  # Verify database at the end

    except sqlite3.OperationalError as e:
        logging.error(f"Database error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise

if __name__ == "__main__":
    main()
