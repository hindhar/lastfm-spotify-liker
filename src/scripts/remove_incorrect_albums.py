#!/usr/bin/env python3
# remove_albums_after_specific_album.py

import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import time
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Load environment variables
load_dotenv()

# Ensure the 'logs' directory exists
logs_dir = 'logs'
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join(logs_dir, 'remove_albums_after_specific_album.log'),
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def main():
    try:
        # Initialize Spotify client with necessary scopes
        sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                scope="user-library-read user-library-modify"
            )
        )

        # Album ID of "In Waves" by Jamie XX
        reference_album_id = '57MSBg5pBQZH5bfLVDmeuP'

        # Fetch the 'added_at' timestamp for the reference album
        logging.info(f"Fetching 'added_at' timestamp for album ID {reference_album_id}")
        found_reference = False
        offset = 0
        limit = 50
        reference_added_at = None

        while True:
            results = sp.current_user_saved_albums(limit=limit, offset=offset)
            items = results['items']
            if not items:
                break
            for item in items:
                album = item['album']
                if album['id'] == reference_album_id:
                    reference_added_at = datetime.strptime(item['added_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    found_reference = True
                    logging.info(f"Found reference album. 'added_at' timestamp: {reference_added_at.isoformat()}")
                    break  # Exit the loop since we found the reference album
            if found_reference:
                break
            offset += limit
            time.sleep(0.1)  # Respect rate limits

        if not found_reference:
            logging.error(f"Reference album with ID {reference_album_id} not found in your saved albums.")
            return

        # Now, fetch all saved albums and identify those added after the reference album
        albums_to_remove = []
        offset = 0

        while True:
            results = sp.current_user_saved_albums(limit=limit, offset=offset)
            items = results['items']
            if not items:
                break
            for item in items:
                added_at = datetime.strptime(item['added_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                if added_at > reference_added_at:
                    album_id = item['album']['id']
                    album_name = item['album']['name']
                    artist_name = item['album']['artists'][0]['name']
                    albums_to_remove.append({
                        'id': album_id,
                        'name': album_name,
                        'artist': artist_name,
                        'added_at': added_at
                    })
            offset += limit
            time.sleep(0.1)  # Respect rate limits

        if not albums_to_remove:
            logging.info("No albums found to remove.")
            return

        logging.info(f"Found {len(albums_to_remove)} albums to remove.")

        # Remove albums from Spotify library in batches of 20
        batch_size = 20
        album_ids = [album['id'] for album in albums_to_remove]
        for i in range(0, len(album_ids), batch_size):
            batch = album_ids[i:i+batch_size]
            sp.current_user_saved_albums_delete(batch)
            logging.info(f"Removed {len(batch)} albums from Spotify library.")

        logging.info(f"Successfully removed {len(album_ids)} albums added after the reference album.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
