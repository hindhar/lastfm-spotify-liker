# utils.py

import re
import threading
import sys
import time
from datetime import datetime, timezone

def normalize_string(s: str) -> str:
    """Normalize a string for consistent comparison."""
    s = s.lower().strip()
    # Remove content in parentheses or brackets
    s = re.sub(r'\s*[\(\[\{].*?[\)\]\}]', '', s)
    # Remove version-specific keywords and their accompanying years if any
    s = re.sub(r'\b(remastered|live|acoustic|mono|stereo|version|edit|feat\.?|featuring|from|remix)\b(\s+\d{4})?', '', s)
    # Remove extra punctuation (but keep numbers)
    s = re.sub(r'[^a-zA-Z0-9\s]', '', s)
    # Remove extra whitespace
    s = re.sub(r'\s+', ' ', s)
    return s.strip()

def get_user_input_with_timeout(prompt: str, timeout: int = 10) -> str:
    """Get user input with a timeout."""
    print(prompt, end='', flush=True)
    user_input = [None]

    def input_thread():
        user_input[0] = sys.stdin.readline().strip()

    thread = threading.Thread(target=input_thread)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        print("\nNo input received. Proceeding with default option.")
        return ''
    return user_input[0]

def get_current_utc_time():
    return datetime.utcnow().replace(tzinfo=timezone.utc)
