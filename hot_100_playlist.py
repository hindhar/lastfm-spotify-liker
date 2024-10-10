import os
import sys
import logging
import sqlite3
from datetime import datetime, timedelta
import random
import requests
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from utils import normalize_string

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
LASTFM_100_DAYS_DB = 'lastfm_100_days.db'
PLAYLIST_NAME = "Bobby's Hot 💯"

class LastFM100DaysDB:
    def __init__(self, db_file):
        self.db_file = db_file
        self.create_table()

    def create_table(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS tracks
                     (id INTEGER PRIMARY KEY, artist TEXT, name TEXT, 
                      play_count INTEGER, last_played DATE)''')
        conn.commit()
        conn.close()

    def update_tracks(self, tracks):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        for track in tracks:
            c.execute('''INSERT OR REPLACE INTO tracks (artist, name, play_count, last_played)
                         VALUES (?, ?, ?, ?)''',
                      (track['artist'], track['name'], track['play_count'], track['last_played']))
        conn.commit()
        conn.close()

    def remove_old_tracks(self, cut_off_date):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("DELETE FROM tracks WHERE last_played < ?", (cut_off_date,))
        conn.commit()
        conn.close()

    def get_top_tracks(self, limit=100):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''SELECT artist, name, play_count FROM tracks
                     ORDER BY play_count DESC LIMIT ?''', (limit,))
        top_tracks = c.fetchall()
        conn.close()
        return top_tracks

def get_lastfm_tracks(from_date, to_date):
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

    while True:
        params['page'] = page
        response = requests.get(url, params=params)
        data = response.json()

        if 'error' in data:
            logging.error(f"Error fetching Last.fm tracks: {data['message']}")
            break

        tracks = data['recenttracks']['track']
        all_tracks.extend(tracks)

        if page >= int(data['recenttracks']['@attr']['totalPages']):
            break
        page += 1

    return all_tracks

def process_lastfm_tracks(tracks):
    processed_tracks = {}
    for track in tracks:
        artist = track['artist']['#text']
        name = track['name']
        date = datetime.fromtimestamp(int(track['date']['uts']))
        
        key = (artist, name)
        if key in processed_tracks:
            processed_tracks[key]['play_count'] += 1
            processed_tracks[key]['last_played'] = max(processed_tracks[key]['last_played'], date)
        else:
            processed_tracks[key] = {
                'artist': artist,
                'name': name,
                'play_count': 1,
                'last_played': date
            }

    return list(processed_tracks.values())

def get_or_create_playlist(sp, name):
    playlists = sp.current_user_playlists()
    for playlist in playlists['items']:
        if playlist['name'] == name:
            return playlist['id']
    
    user_id = sp.me()['id']
    playlist = sp.user_playlist_create(user_id, name, public=False)
    return playlist['id']

def update_playlist(sp, playlist_id, track_ids):
    sp.playlist_replace_items(playlist_id, track_ids)

def main():
    # Initialize Spotify client
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="playlist-modify-private"))

    # Initialize Last.fm 100 days database
    lastfm_db = LastFM100DaysDB(LASTFM_100_DAYS_DB)

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=100)

    # Fetch Last.fm tracks
    lastfm_tracks = get_lastfm_tracks(start_date, end_date)
    processed_tracks = process_lastfm_tracks(lastfm_tracks)

    # Update database
    lastfm_db.update_tracks(processed_tracks)
    lastfm_db.remove_old_tracks(start_date)

    # Get top 100 tracks
    top_tracks = lastfm_db.get_top_tracks(100)

    # Shuffle the tracks
    random.shuffle(top_tracks)

    # Search for tracks on Spotify and get their IDs
    spotify_track_ids = []
    for artist, name, _ in top_tracks:
        results = sp.search(q=f"track:{name} artist:{artist}", type='track', limit=1)
        if results['tracks']['items']:
            spotify_track_ids.append(results['tracks']['items'][0]['id'])

    # Get or create playlist
    playlist_id = get_or_create_playlist(sp, PLAYLIST_NAME)

    # Update playlist
    update_playlist(sp, playlist_id, spotify_track_ids)

    logging.info(f"Playlist '{PLAYLIST_NAME}' updated with {len(spotify_track_ids)} tracks.")

if __name__ == "__main__":
    main()