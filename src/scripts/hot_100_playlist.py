import os
import sys
import re
import sqlite3
import logging
import random
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import unicodedata

# Modify the sys.path to include the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='logs/hot_100_playlist.log', filemode='a')

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
PLAYLIST_NAME = "Bobby's Hot 💯"

class LastFM100DaysDB:
    def __init__(self, db_file):
        self.db_file = db_file
        self.create_table()

    def create_table(self):
        conn = sqlite3.connect(self.db_file)
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
        conn.close()

    def update_tracks(self, tracks):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        for track in tracks:
            c.execute('''SELECT play_count, last_played FROM tracks
                         WHERE artist = ? AND name = ? AND album = ?''',
                      (track['artist'], track['name'], track['album']))
            result = c.fetchone()
            if result:
                existing_play_count, existing_last_played = result
                # Convert existing_last_played from string to datetime
                existing_last_played = datetime.fromisoformat(existing_last_played)
                new_play_count = existing_play_count + track['play_count']
                new_last_played = max(existing_last_played, track['last_played'])
                # Convert new_last_played to ISO string for storage
                new_last_played_str = new_last_played.isoformat()
                c.execute('''UPDATE tracks
                             SET play_count = ?, last_played = ?
                             WHERE artist = ? AND name = ? AND album = ?''',
                          (new_play_count, new_last_played_str, track['artist'], track['name'], track['album']))
            else:
                # Convert last_played to ISO string for storage
                last_played_str = track['last_played'].isoformat()
                c.execute('''INSERT INTO tracks (artist, name, album, play_count, last_played)
                             VALUES (?, ?, ?, ?, ?)''',
                          (track['artist'], track['name'], track['album'], track['play_count'], last_played_str))
        conn.commit()
        conn.close()

    def remove_old_tracks(self, cut_off_date):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        # Convert cut_off_date to ISO string for comparison
        cut_off_date_str = cut_off_date.isoformat()
        c.execute("DELETE FROM tracks WHERE last_played < ?", (cut_off_date_str,))
        conn.commit()
        conn.close()

    def get_all_tracks(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''SELECT artist, name, album, play_count FROM tracks
                     ORDER BY play_count DESC, last_played DESC''')
        all_tracks = c.fetchall()
        conn.close()
        return all_tracks

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

        total_pages = int(data['recenttracks']['@attr']['totalPages'])
        if page >= total_pages:
            break
        page += 1

    return all_tracks

def process_lastfm_tracks(tracks):
    processed_tracks = {}
    for track in tracks:
        if 'date' not in track:
            # Skip currently playing track
            continue

        artist = track['artist']['#text']
        name = track['name']
        album = track['album']['#text'] if 'album' in track and track['album']['#text'] else 'Unknown Album'
        date = datetime.fromtimestamp(int(track['date']['uts']))

        key = (artist, name, album)
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

    return list(processed_tracks.values())

def normalize_string(s):
    # Convert to lowercase
    s = s.lower()
    # Remove content inside parentheses
    s = re.sub(r'\(.*?\)', '', s)
    # Remove extra spaces
    s = ' '.join(s.strip().split())
    # Remove punctuation except ampersand
    s = re.sub(r'[^\w\s&]', '', s)
    return s

def get_or_create_playlist(sp, name):
    def normalize(s):
        # Normalize Unicode characters and convert to lowercase
        return unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII').lower()

    normalized_name = normalize(name)

    playlists = []
    limit = 50
    offset = 0
    user_id = sp.me()['id']

    while True:
        response = sp.user_playlists(user_id, limit=limit, offset=offset)
        playlists.extend(response['items'])
        if response['next']:
            offset += limit
        else:
            break

    for playlist in playlists:
        playlist_name = playlist['name']
        normalized_playlist_name = normalize(playlist_name)
        if normalized_playlist_name == normalized_name:
            logging.info(f"Found existing playlist: {playlist_name}")
            return playlist['id']

    logging.info(f"Playlist '{name}' not found. Creating new playlist.")
    playlist = sp.user_playlist_create(user_id, name, public=False)
    return playlist['id']

def update_playlist(sp, playlist_id, track_ids):
    logging.info(f"Updating playlist {playlist_id} with {len(track_ids)} tracks")

    # Randomize the order of tracks
    random.shuffle(track_ids)

    # Clear the playlist first
    sp.playlist_replace_items(playlist_id, [])
    # Spotify API allows up to 100 tracks per request
    for i in range(0, len(track_ids), 100):
        sp.playlist_add_items(playlist_id, track_ids[i:i+100])
    logging.info("Playlist update completed with randomized order")

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

    # Get all tracks sorted by play count descending
    all_tracks = lastfm_db.get_all_tracks()

    # Build a dictionary to keep track of the album with the highest play count for each (artist, name)
    track_album_playcount = {}
    for artist, name, album, play_count in all_tracks:
        norm_artist = normalize_string(artist)
        norm_name = normalize_string(name)
        key = (norm_artist, norm_name)
        if key not in track_album_playcount:
            track_album_playcount[key] = {
                'artist': artist,
                'name': name,
                'album': album,
                'play_count': play_count
            }
        else:
            if play_count > track_album_playcount[key]['play_count']:
                track_album_playcount[key] = {
                    'artist': artist,
                    'name': name,
                    'album': album,
                    'play_count': play_count
                }

    # Now, get the top 100 tracks based on play count
    sorted_tracks = sorted(track_album_playcount.values(), key=lambda x: x['play_count'], reverse=True)

    # Build the list of tracks to add to the playlist
    spotify_track_ids = []
    track_count = 0
    for track_info in sorted_tracks:
        if track_count >= 100:
            break

        artist = track_info['artist']
        name = track_info['name']
        album = track_info['album']
        play_count = track_info['play_count']

        logging.info(f"Searching for track: {artist} - {name} (Album: {album}, Play count: {play_count})")

        # Try to find the track on Spotify
        found = False
        queries = [
            f'track:"{name}" artist:"{artist}" album:"{album}"',
            f'track:"{name}" artist:"{artist}"',
            f'track:"{name}"',
        ]
        for query in queries:
            results = sp.search(q=query, type='track', limit=5)
            if results['tracks']['items']:
                # Try to find the best match
                for item in results['tracks']['items']:
                    spotify_track = item['name']
                    spotify_artist = item['artists'][0]['name']
                    spotify_album = item['album']['name']
                    # Normalize the names
                    norm_spotify_artist = normalize_string(spotify_artist)
                    norm_spotify_name = normalize_string(spotify_track)
                    norm_spotify_album = normalize_string(spotify_album)
                    norm_artist = normalize_string(artist)
                    norm_name = normalize_string(name)
                    norm_album = normalize_string(album)
                    if norm_spotify_artist == norm_artist and norm_spotify_name == norm_name and norm_spotify_album == norm_album:
                        spotify_track_ids.append(item['id'])
                        found = True
                        track_count += 1
                        logging.info(f"Found exact match: {spotify_artist} - {spotify_track} ({spotify_album})")
                        break
                if found:
                    break
        if not found:
            logging.warning(f"Could not find exact match on Spotify: {artist} - {name} ({album})")
            # Try to find the best available version
            for query in queries:
                results = sp.search(q=query, type='track', limit=5)
                if results['tracks']['items']:
                    item = results['tracks']['items'][0]
                    spotify_track_ids.append(item['id'])
                    track_count += 1
                    logging.info(f"Added closest match: {item['artists'][0]['name']} - {item['name']} ({item['album']['name']})")
                    found = True
                    break

    # Ensure we only have 100 tracks
    spotify_track_ids = spotify_track_ids[:100]

    # Get or create playlist
    playlist_id = get_or_create_playlist(sp, PLAYLIST_NAME)

    # Update playlist with randomized order
    update_playlist(sp, playlist_id, spotify_track_ids)

    logging.info(f"Playlist '{PLAYLIST_NAME}' updated with {len(spotify_track_ids)} tracks in random order.")

    # Get the playlist link
    playlist_info = sp.playlist(playlist_id)
    playlist_link = playlist_info['external_urls']['spotify']

    print(f"\nPlaylist updated successfully!")
    print(f"You can access your playlist '{PLAYLIST_NAME}' here: {playlist_link}")
    logging.info(f"Playlist link: {playlist_link}")

if __name__ == "__main__":
    main()
