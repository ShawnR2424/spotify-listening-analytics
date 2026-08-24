import json
import os
from uuid import uuid4

import spotipy
from databricks import sql
from dotenv import load_dotenv
from spotipy.cache_handler import CacheFileHandler
from spotipy.oauth2 import SpotifyPKCE


SOURCE_ENDPOINT = "/me/player/recently-played"
BRONZE_TABLE = (
    "spotify_analytics.bronze.spotify_recently_played_raw"
)


load_dotenv()


def get_spotify_client():
    auth_manager = SpotifyPKCE(
        client_id=os.environ["SPOTIPY_CLIENT_ID"],
        redirect_uri=os.environ["SPOTIPY_REDIRECT_URI"],
        scope="user-read-recently-played",
        cache_handler=CacheFileHandler(
            cache_path=".spotify_cache"
        ),
    )

    return spotipy.Spotify(
        auth_manager=auth_manager
    )


def get_databricks_connection():
    return sql.connect(
        server_hostname=os.environ[
            "DATABRICKS_SERVER_HOSTNAME"
        ],
        http_path=os.environ[
            "DATABRICKS_HTTP_PATH"
        ],
        auth_type="databricks-oauth",
    )


def get_latest_processed_timestamp(connection):
    """
    Return the newest listening event already present
    in the Silver layer as Unix milliseconds.
    """

    query = """
        SELECT unix_millis(MAX(played_at))
        FROM spotify_analytics.silver.listening_events
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()

    if result is None:
        return None

    return result[0]


def insert_bronze_payload(
    connection,
    payload,
):
    batch_id = str(uuid4())

    query = f"""
        INSERT INTO {BRONZE_TABLE}
        (
            batch_id,
            ingested_at,
            source_endpoint,
            payload
        )
        VALUES (
            ?,
            current_timestamp(),
            ?,
            ?
        )
    """

    with connection.cursor() as cursor:
        cursor.execute(
            query,
            [
                batch_id,
                SOURCE_ENDPOINT,
                json.dumps(payload),
            ],
        )

    return batch_id


def main():
    spotify = get_spotify_client()

    with get_databricks_connection() as connection:

        latest_timestamp = (
            get_latest_processed_timestamp(
                connection
            )
        )

        print(
            "Latest processed Spotify timestamp:",
            latest_timestamp,
        )

        results = spotify.current_user_recently_played(
            limit=50,
            after=latest_timestamp,
        )

        total_received = 0
        batches_written = 0

        while results:

            item_count = len(
                results.get("items", [])
            )

            if item_count == 0:
                break

            batch_id = insert_bronze_payload(
                connection,
                results,
            )

            total_received += item_count
            batches_written += 1

            print(
                f"Wrote Bronze batch {batch_id} "
                f"with {item_count} events."
            )

            if not results.get("next"):
                break

            results = spotify.next(results)

    print()
    print(
        f"Ingestion complete: "
        f"{total_received} events received "
        f"across {batches_written} batch(es)."
    )


if __name__ == "__main__":
    main()