import os
from google.cloud import bigquery

def check_and_create_dataset_table(dataset_id, table_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<credentials_path>"
    bq_client = bigquery.Client()
    
    dataset_ref = bq_client.dataset(dataset_id)
    
    try:
        bq_client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)
        print(f"Dataset {dataset_id} create.")
        
    table_ref = dataset_ref.table(table_id)
    table_ref.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="release_date"
    )
    
    schema = [
        bigquery.SchemaField("track", "STRING"),
        bigquery.SchemaField("album_name", "STRING"),
        bigquery.SchemaField("artist", "STRING"),
        bigquery.SchemaField("release_date", "DATETIME"),
        bigquery.SchemaField("isrc", "STRING"),
        bigquery.SchemaField("all_time_rank", "INTEGER"),
        bigquery.SchemaField("track_score", "FLOAT64"),
        bigquery.SchemaField("spotify_streams", "INTEGER"),
        bigquery.SchemaField("spotify_playlist_count", "INTEGER"),
        bigquery.SchemaField("spotify_playlist_reach", "INTEGER"),
        bigquery.SchemaField("spotify_popularity", "INTEGER"),
        bigquery.SchemaField("youtube_views", "INTEGER"),
        bigquery.SchemaField("youtube_likes", "INTEGER"),
        bigquery.SchemaField("tiktok_posts", "INTEGER"),
        bigquery.SchemaField("tiktok_likes", "INTEGER"),
        bigquery.SchemaField("tiktok_views", "INTEGER"),
        bigquery.SchemaField("youtube_playlist_reach", "INTEGER"),
        bigquery.SchemaField("apple_music_playlist_count", "INTEGER"),
        bigquery.SchemaField("airplay_spins", "INTEGER"),
        bigquery.SchemaField("siriusxm_spins", "INTEGER"),
        bigquery.SchemaField("deezer_playlist_count", "INTEGER"),
        bigquery.SchemaField("deezer_playlist_reach", "INTEGER"),
        bigquery.SchemaField("amazon_playlist_count", "INTEGER"),
        bigquery.SchemaField("pandora_streams", "INTEGER"),
        bigquery.SchemaField("pandora_track_stations", "INTEGER"),
        bigquery.SchemaField("soundcloud_streams", "INTEGER"),
        bigquery.SchemaField("shazam_counts", "INTEGER"),
        bigquery.SchemaField("explicit_track", "BOOLEAN")
        ]
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)
        print(f"Table {table_id} created.")
