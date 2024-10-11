# remove_duplicate_albums.py

import os
import sys

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import random
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='logs/remove_duplicate_albums.log', filemode='a')

# Load environment variables from .env file
load_dotenv()

# Set up Spotify client
SPOTIPY_CLIENT_ID = os.getenv('SPOTIPY_CLIENT_ID')
SPOTIPY_CLIENT_SECRET = os.getenv('SPOTIPY_CLIENT_SECRET')
SPOTIPY_REDIRECT_URI = os.getenv('SPOTIPY_REDIRECT_URI')

print(f"SPOTIPY_CLIENT_ID: {SPOTIPY_CLIENT_ID}")
print(f"SPOTIPY_CLIENT_SECRET: {SPOTIPY_CLIENT_SECRET}")
print(f"SPOTIPY_REDIRECT_URI: {SPOTIPY_REDIRECT_URI}")

if not SPOTIPY_CLIENT_ID or not SPOTIPY_CLIENT_SECRET or not SPOTIPY_REDIRECT_URI:
    raise ValueError("Spotify credentials not found in .env file.")

def get_spotify_client():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(scope="user-library-read user-library-modify"))

def get_albums_to_remove(sp):
    albums_to_remove = []
    offset = 0
    batch_size = 50
    album_dict = {}

    while True:
        results = sp.current_user_saved_albums(limit=batch_size, offset=offset)
        if not results['items']:
            break
        
        for item in results['items']:
            album = item['album']
            artist = album['artists'][0]['name']
            album_name = album['name']
            album_id = album['id']

            if 'various artists' in artist.lower():
                albums_to_remove.append((album_id, album_name, artist))
            else:
                key = f"{artist.lower()}:{album_name.lower()}"
                if key in album_dict:
                    album_dict[key].append((album_id, album_name, artist))
                else:
                    album_dict[key] = [(album_id, album_name, artist)]
        
        offset += batch_size

    # Check for duplicates
    for albums in album_dict.values():
        if len(albums) > 1:
            albums_to_keep = choose_albums_to_keep(sp, albums)
            albums_to_remove.extend([album for album in albums if album not in albums_to_keep])

    return albums_to_remove

def choose_albums_to_keep(sp, albums):
    deluxe = None
    remastered = None
    normal = None
    max_liked_songs = -1
    albums_with_max = []

    for album in albums:
        album_id, album_name, _ = album
        if 'deluxe' in album_name.lower():
            deluxe = album
        elif 'remaster' in album_name.lower():
            remastered = album
        else:
            normal = album

        liked_songs = count_liked_songs(sp, album_id)
        if liked_songs > max_liked_songs:
            max_liked_songs = liked_songs
            albums_with_max = [album]
        elif liked_songs == max_liked_songs:
            albums_with_max.append(album)

    to_keep = []
    if deluxe:
        to_keep.append(deluxe)
    if remastered and remastered not in to_keep:
        to_keep.append(remastered)
    if normal and normal not in to_keep:
        to_keep.append(normal)

    if not to_keep:
        to_keep = [random.choice(albums_with_max)]

    return to_keep

def count_liked_songs(sp, album_id):
    tracks = sp.album_tracks(album_id)['items']
    liked_count = 0
    for track in tracks:
        if sp.current_user_saved_tracks_contains([track['id']])[0]:
            liked_count += 1
    return liked_count

def remove_albums(sp, albums_to_remove):
    for i in range(0, len(albums_to_remove), 50):
        batch = albums_to_remove[i:i+50]
        sp.current_user_saved_albums_delete([album[0] for album in batch])
        for album in batch:
            print(f"Removed: {album[2]} - {album[1]}")

def main():
    sp = get_spotify_client()
    
    print("Searching for albums to remove...")
    albums_to_remove = get_albums_to_remove(sp)
    
    if albums_to_remove:
        print(f"Found {len(albums_to_remove)} albums to remove:")
        for album in albums_to_remove:
            print(f"  {album[2]} - {album[1]}")
        confirm = input("Do you want to remove these albums? (y/n): ")
        if confirm.lower() == 'y':
            remove_albums(sp, albums_to_remove)
            print(f"Total albums removed: {len(albums_to_remove)}")
        else:
            print("Operation cancelled.")
    else:
        print("No albums to remove found in your library.")

if __name__ == "__main__":
    main()