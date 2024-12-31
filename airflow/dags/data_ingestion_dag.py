from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from utils.spotify_utils import get_spotify_client, fetch_trending_tracks, fetch_artist_data, upload_to_gcs
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_spotify_data(**context):
    """
    Fetch Spotify data and upload to GCS.
    """
    # Initialize Spotify client
    sp_client = get_spotify_client()
    
    # Fetch trending tracks
    tracks = fetch_trending_tracks(sp_client)
    
    # Extract unique artist IDs
    artist_ids = set()
    for track in tracks:
        for artist in track['artists']:
            artist_ids.add(artist['id'])
    
    # Fetch artist data
    artists = fetch_artist_data(sp_client, list(artist_ids))
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    bucket_name = os.getenv('GCS_RAW_BUCKET')
    
    # Upload to GCS
    tracks_path = f'spotify/raw/tracks/tracks_{timestamp}.json'
    artists_path = f'spotify/raw/artists/artists_{timestamp}.json'
    
    tracks_uri = upload_to_gcs(tracks, bucket_name, tracks_path)
    artists_uri = upload_to_gcs(artists, bucket_name, artists_path)
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='tracks_file', value=tracks_uri)
    context['task_instance'].xcom_push(key='artists_file', value=artists_uri)

with DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Data ingestion pipeline for Prysm',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['prysm', 'data_ingestion'],
) as dag:
    
    spotify_task = PythonOperator(
        task_id='fetch_spotify_data',
        python_callable=fetch_spotify_data,
        provide_context=True,
    )
    
    # Add task dependencies
    spotify_task 