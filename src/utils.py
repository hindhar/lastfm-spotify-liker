# utils.py
import re

def normalize_string(s):
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
