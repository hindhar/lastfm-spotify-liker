#!/usr/bin/env python3
# remove_duplicate_albums.py

import os
import sys
import logging
import sqlite3
import time
from dotenv import load_dotenv
import random

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

from src.spotify_operations import SpotifyOperations
# Removed import of normalize_string since special characters should not be removed

# Load environment variables
load_dotenv()

# Ensure the 'logs' directory exists
logs_dir = os.path.join(project_root, 'logs')
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Set up logging to match your project's configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join(logs_dir, 'remove_duplicate_albums.log'),
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Database file path
DB_FILE = os.path.join(project_root, 'db', 'removed_albums.db')

def get_spotify_client():
    spotify_ops = SpotifyOperations()
    return spotify_ops.sp

def create_database():
    """Create the database and table if they don't exist."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS removed_albums (
            album_id TEXT PRIMARY KEY,
            album_name TEXT,
            artist_name TEXT
        )
    ''')
    conn.commit()
    conn.close()

def insert_removed_album(album_id, album_name, artist_name):
    """Insert a removed album into the database."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT OR IGNORE INTO removed_albums (album_id, album_name, artist_name)
        VALUES (?, ?, ?)
    ''', (album_id, album_name, artist_name))
    conn.commit()
    conn.close()

def get_albums_to_remove(sp):
    albums_to_remove = []
    offset = 0
    batch_size = 50
    album_dict = {}

    logging.info("Fetching saved albums from Spotify...")
    while True:
        results = sp.current_user_saved_albums(limit=batch_size, offset=offset)
        if not results['items']:
            break

        for item in results['items']:
            album = item['album']
            artist = album['artists'][0]['name']
            album_name = album['name']
            album_id = album['id']

            # Using exact names including special characters
            key = f"{artist}:{album_name}"

            if key in album_dict:
                album_dict[key].append((album_id, album_name, artist))
            else:
                album_dict[key] = [(album_id, album_name, artist)]

        offset += batch_size
        time.sleep(0.1)  # Respect rate limits

    # Identify duplicates and decide which albums to remove
    for albums in album_dict.values():
        if len(albums) >= 3:
            # If there are three or more versions of an album
            albums_to_keep, albums_to_remove_duplicates = choose_album_to_keep(sp, albums)
            albums_to_remove.extend(albums_to_remove_duplicates)
        elif len(albums) > 1:
            # If there are exactly two versions, remove duplicates keeping one
            albums_to_keep = [albums[0]]  # Keep the first one
            albums_to_remove_duplicates = albums[1:]  # Remove the rest
            albums_to_remove.extend(albums_to_remove_duplicates)

    # Remove duplicates from the removal list (in case of overlaps)
    albums_to_remove = list({album[0]: album for album in albums_to_remove}.values())

    return albums_to_remove

def choose_album_to_keep(sp, albums):
    # Count liked songs in each album
    album_liked_songs = {}
    min_liked_songs = None

    for album in albums:
        album_id, album_name, artist = album
        liked_songs_count = count_liked_songs(sp, album_id)
        album_liked_songs[album_id] = liked_songs_count

    # Find album(s) with the fewest liked songs
    min_liked_songs = min(album_liked_songs.values())
    albums_with_min_likes = [album for album in albums if album_liked_songs[album[0]] == min_liked_songs]

    # Randomly select one album with the fewest liked songs to remove
    album_to_remove = random.choice(albums_with_min_likes)

    # Albums to keep: All others
    albums_to_keep = [album for album in albums if album != album_to_remove]
    albums_to_remove = [album_to_remove]

    return albums_to_keep, albums_to_remove

def count_liked_songs(sp, album_id):
    tracks = sp.album_tracks(album_id)['items']
    liked_count = 0
    track_ids = [track['id'] for track in tracks]

    # Check in batches of 50
    for i in range(0, len(track_ids), 50):
        batch = track_ids[i:i+50]
        liked_results = sp.current_user_saved_tracks_contains(batch)
        liked_count += sum(liked_results)

    return liked_count

def remove_albums(sp, albums_to_remove):
    batch_size = 20  # Spotify API allows up to 20 IDs per request
    total_removed = 0
    for i in range(0, len(albums_to_remove), batch_size):
        batch = albums_to_remove[i:i+batch_size]
        album_ids = [album[0] for album in batch]
        sp.current_user_saved_albums_delete(album_ids)
        for album in batch:
            album_id, album_name, artist = album
            logging.info(f"Removed: {artist} - {album_name}")
            # Insert removed album into database
            insert_removed_album(album_id, album_name, artist)
        total_removed += len(batch)
        time.sleep(0.1)  # Respect rate limits

    logging.info(f"Total albums removed: {total_removed}")

def main():
    sp = get_spotify_client()
    create_database()

    logging.info("Searching for albums to remove...")
    albums_to_remove = get_albums_to_remove(sp)

    if albums_to_remove:
        logging.info(f"Found {len(albums_to_remove)} albums to remove.")
        # Optional: List albums to be removed
        for album in albums_to_remove:
            album_id, album_name, artist = album
            logging.info(f"To be removed: {artist} - {album_name}")
        confirm = input("Do you want to remove these albums? (y/n): ")
        if confirm.lower() == 'y':
            remove_albums(sp, albums_to_remove)
            logging.info("Album removal process completed.")
        else:
            logging.info("Operation cancelled by the user.")
    else:
        logging.info("No albums to remove found in your library.")

if __name__ == "__main__":
    main()
