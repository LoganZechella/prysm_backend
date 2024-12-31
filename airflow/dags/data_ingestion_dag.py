from datetime import datetime, timedelta
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models import Variable
from utils.spotify_utils import get_spotify_client, fetch_trending_tracks, fetch_artist_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_google_data(**context):
    # TODO: Implement Google API data fetching
    pass

def fetch_spotify_data(**context):
    """
    Fetch trending tracks and artist data from Spotify and upload to GCS.
    """
    # Initialize GCS hook
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = Variable.get('GCS_BUCKET_NAME')
    
    # Get Spotify client
    sp_client = get_spotify_client()
    
    # Fetch trending tracks
    tracks = fetch_trending_tracks(sp_client)
    
    # Extract unique artist IDs from tracks
    artist_ids = set()
    for track in tracks:
        for artist in track['artists']:
            if artist.get('id'):
                artist_ids.add(artist['id'])
    
    # Fetch artist data
    artists = fetch_artist_data(sp_client, list(artist_ids))
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    tracks_filename = f'spotify/raw/tracks_{timestamp}.json'
    artists_filename = f'spotify/raw/artists_{timestamp}.json'
    
    # Upload to GCS
    gcs_hook.upload_file_obj(
        bucket_name=bucket_name,
        object_name=tracks_filename,
        data=json.dumps(tracks).encode('utf-8'),
        mime_type='application/json'
    )
    
    gcs_hook.upload_file_obj(
        bucket_name=bucket_name,
        object_name=artists_filename,
        data=json.dumps(artists).encode('utf-8'),
        mime_type='application/json'
    )
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='tracks_file', value=tracks_filename)
    context['task_instance'].xcom_push(key='artists_file', value=artists_filename)

def fetch_linkedin_data(**context):
    # TODO: Implement LinkedIn API data fetching
    pass

def process_data(**context):
    # TODO: Implement data processing logic
    pass

with DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Data ingestion pipeline for recommendation engine',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['prysm', 'data_ingestion'],
) as dag:

    # Data fetching tasks
    fetch_google = PythonOperator(
        task_id='fetch_google_data',
        python_callable=fetch_google_data,
    )

    fetch_spotify = PythonOperator(
        task_id='fetch_spotify_data',
        python_callable=fetch_spotify_data,
    )

    fetch_linkedin = PythonOperator(
        task_id='fetch_linkedin_data',
        python_callable=fetch_linkedin_data,
    )

    # Data processing task
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    # Define task dependencies
    [fetch_google, fetch_spotify, fetch_linkedin] >> process_data_task 