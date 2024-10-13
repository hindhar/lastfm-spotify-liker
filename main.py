import os
import sys
import subprocess
import logging
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/main.log',
    filemode='a'
)

def run_script(script_name):
    script_path = os.path.join(project_root, 'src', 'scripts', script_name)
    logging.info(f"Running {script_name}")
    try:
        subprocess.run([sys.executable, script_path], check=True)
        logging.info(f"{script_name} completed successfully")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}: {e}")

def main():
    logging.info("Starting main process")
    
    # Run lastfm_spotify_liker.py
    run_script('lastfm_spotify_liker.py')
    
    # Run album_saver.py
    run_script('album_saver.py')
    
    # Run hot_100_playlist.py
    run_script('hot_100_playlist.py')
    
    logging.info("Main process completed")

if __name__ == "__main__":
    main()