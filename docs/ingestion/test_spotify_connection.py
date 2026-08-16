import json
import os
from pathlib import Path

import spotipy
from dotenv import load_dotenv
from spotipy.cache_handler import CacheFileHandler
from spotipy.oauth2 import SpotifyPKCE


load_dotenv()

client_id = os.environ["SPOTIPY_CLIENT_ID"]
redirect_uri = os.environ["SPOTIPY_REDIRECT_URI"]

cache_handler = CacheFileHandler(
    cache_path=".spotify_cache"
)

auth_manager = SpotifyPKCE(
    client_id=client_id,
    redirect_uri=redirect_uri,
    scope="user-read-recently-played",
    cache_handler=cache_handler,
)

spotify = spotipy.Spotify(
    auth_manager=auth_manager
)

results = spotify.current_user_recently_played(
    limit=20
)

raw_directory = Path("data/raw")
raw_directory.mkdir(parents=True, exist_ok=True)

output_path = raw_directory / "recently_played_sample.json"

with output_path.open("w", encoding="utf-8") as file:
    json.dump(
        results,
        file,
        indent=2,
        ensure_ascii=False,
    )

print(f"Retrieved {len(results['items'])} listening events.")
print(f"Raw response saved to: {output_path}")
