import pandas as pd
import os
from io import StringIO
from google.cloud import storage

#function change value
def change_value(df,col,old,new):
    if old == "NaN":
        df[col].fillna(new, inplace=True)
    else:
        df[col] = df[col].map(lambda x: x.replace(old, new) if isinstance(x, str) else x)
    return df

def transform_data():
    raw_bucket = "raw-data-spotify"
    output_bucket = "transform-data-spotify"
    raw_file_name = "most-streamed-spotify-songs-2024.csv"
    output_file_name = "clean-data-spotify.parquet"
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials/spotify-project-airflow-keys.json"
    
    client = storage.Client()
    _raw_bucket = client.get_bucket(f"{raw_bucket}")
    _output_bucket = client.get_bucket(f"{output_bucket}")
    
    #Read file
    blob = _raw_bucket.blob(f"{raw_file_name}")
    data = blob.download_as_string()
    try:
        df = pd.read_csv(StringIO(data.decode("ISO-8859-1")))
        print(f"Read File: {raw_file_name} in Bucket: {raw_bucket} Success")
    except Exception as e:
        print(e)
    
    #Transform and Cleansing Data
    #Change Columne names
    new_name_cols = {col: col.lower().replace(' ', '_') for col in df.columns}
    df_change_col_name = df.rename(columns = new_name_cols)
    
    #Cleansing Value
    df_clean_value = df_change_col_name
    col_names = df_clean_value.columns
    for col_name in col_names:
        df_clean_value = change_value(df_clean_value, col_name, "/", "-")
        df_clean_value = change_value(df_clean_value, col_name, ",", "") 

    df_clean_value['release_date'] = pd.to_datetime(df_clean_value['release_date'], format='%m-%d-%Y').dt.strftime('%Y-%m-%d')
    
    #Handle Missing Value
    df_clean_missing = change_value(df_clean_value, "artist", "NaN", "Missing Value")
    
    missing_col_names = df_clean_missing.columns
    for col_name in missing_col_names:
        df_clean_missing = change_value(df_clean_missing, col_name, "NaN", 0)
    
    df_drop_missing = df_clean_missing.drop("tidal_popularity", axis=1)
    
    #Change Data Type
    new_type = {"release_date":"datetime64[ns]", "all_time_rank":"int64", "spotify_streams":"int64", "spotify_playlist_count":"int64", "spotify_playlist_reach":"int64", "spotify_popularity":"int64", "youtube_views":"int64", "youtube_likes":"int64", "tiktok_posts":"int64", "tiktok_likes":"int64", "tiktok_views":"int64", "youtube_playlist_reach":"int64", "apple_music_playlist_count":"int64", "airplay_spins":"int64", "siriusxm_spins":"int64", "deezer_playlist_count":"int64", "deezer_playlist_reach":"int64", "amazon_playlist_count":"int64", "pandora_streams":"int64", "pandora_track_stations":"int64", "soundcloud_streams":"int64", "shazam_counts":"int64", "explicit_track":"bool"}

    df_change_type = df_drop_missing
    for col, dtype in new_type.items():
        df_change_type[col] = df_change_type[col].astype(dtype)

    #Output
    try:
        df_change_type.to_parquet(f"gs://{output_bucket}/{output_file_name}", index=False)
        print(f"Upload File: {output_file_name} to Bucket: {output_bucket}")
    except Exception as e:
        print(e) 
        