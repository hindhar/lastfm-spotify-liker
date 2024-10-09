import os
import sys
import logging
import time
from datetime import datetime
import requests
from dotenv import load_dotenv
from database import Database
from spotify_operations import SpotifyOperations

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Last.fm API credentials
LASTFM_API_KEY = os.getenv('LASTFM_API_KEY')
LASTFM_USER = os.getenv('LASTFM_USER')

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

    while page <= total_pages:
        params['page'] = page
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'error' in data:
                logging.error(f"Error fetching Last.fm tracks: {data['message']}")
                break

            tracks = data['recenttracks']['track']
            all_tracks.extend(tracks)

            total_pages = int(data['recenttracks']['@attr']['totalPages'])
            page += 1

            logging.info(f"Fetched page {page-1} of {total_pages}")

        except requests.RequestException as e:
            logging.error(f"Network error when fetching Last.fm tracks: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error in get_new_lastfm_tracks: {e}")
            logging.exception(e)
            break

    for track in all_tracks:
        if 'date' in track:
            track['date'] = datetime.fromtimestamp(int(track['date']['uts']))
        else:
            track['date'] = datetime.now()  # For 'now playing' track

        db.add_or_update_track({
            'artist': track['artist']['#text'],
            'name': track['name'],
            'album': track['album']['#text'],
            'date': track['date'],
            'mbid': track.get('mbid', '')
        })

    return len(all_tracks)

def main():
    try:
        lastfm_db = Database()
        spotify_ops = SpotifyOperations()

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
        logging.info(f"Spotify liked songs database updated.")

        # Get frequently played tracks from Last.fm
        frequently_played = lastfm_db.get_frequently_played_tracks(5)
        logging.info(f"Found {len(frequently_played)} tracks played more than 5 times on Last.fm")

        # Find tracks to be liked
        tracks_to_like = spotify_ops.find_tracks_to_like(frequently_played, min_play_count=5)
        logging.info(f"Found {len(tracks_to_like)} tracks to like on Spotify")

        # Like the tracks on Spotify
        if tracks_to_like:
            spotify_ops.like_tracks(tracks_to_like)
            logging.info(f"Finished liking tracks on Spotify")
        else:
            logging.info("No new tracks to like on Spotify")

    except KeyboardInterrupt:
        logging.info("Program interrupted by user. Exiting gracefully.")
        sys.exit(0)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
