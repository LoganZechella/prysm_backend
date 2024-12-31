from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from utils.spotify_utils import get_spotify_client, fetch_trending_tracks, fetch_artist_data, upload_to_gcs as upload_spotify_to_gcs
from utils.google_trends_utils import (
    get_google_trends_client,
    fetch_trending_topics,
    fetch_topic_interest,
    fetch_related_topics,
    upload_to_gcs as upload_trends_to_gcs
)
from utils.linkedin_utils import (
    get_linkedin_client,
    fetch_profile_info,
    fetch_connections,
    fetch_recent_activity,
    upload_to_gcs as upload_linkedin_to_gcs
)
from utils.data_processing_utils import (
    read_from_gcs,
    process_spotify_data,
    process_trends_data,
    process_linkedin_data,
    upload_processed_data
)
from utils.user_profile_utils import aggregate_user_data, store_aggregated_data
import os
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables
load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_required_env_var(var_name: str) -> str:
    """Get a required environment variable or raise an error."""
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Required environment variable {var_name} is not set")
    return value

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
    
    # Get bucket name
    bucket_name = get_required_env_var('GCS_RAW_BUCKET')
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    # Upload to GCS
    tracks_path = f'spotify/raw/tracks/tracks_{timestamp}.json'
    artists_path = f'spotify/raw/artists/artists_{timestamp}.json'
    
    tracks_uri = upload_spotify_to_gcs(tracks, bucket_name, tracks_path)
    artists_uri = upload_spotify_to_gcs(artists, bucket_name, artists_path)
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='tracks_file', value=tracks_uri)
    context['task_instance'].xcom_push(key='artists_file', value=artists_uri)

def fetch_trends_data(**context):
    """
    Fetch Google Trends data and upload to GCS.
    """
    # Initialize Google Trends client
    trends_client = get_google_trends_client()
    
    # Fetch trending topics
    trending_topics = fetch_trending_topics(trends_client)
    
    # Get topics list for detailed analysis
    topics = [topic['topic'] for topic in trending_topics]
    
    # Fetch interest over time
    interest_data = fetch_topic_interest(trends_client, topics)
    
    # Fetch related topics
    related_data = fetch_related_topics(trends_client, topics)
    
    # Get bucket name
    bucket_name = get_required_env_var('GCS_RAW_BUCKET')
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    # Upload to GCS
    trending_path = f'trends/raw/trending/trending_{timestamp}.json'
    interest_path = f'trends/raw/interest/interest_{timestamp}.json'
    related_path = f'trends/raw/related/related_{timestamp}.json'
    
    trending_uri = upload_trends_to_gcs(trending_topics, bucket_name, trending_path)
    interest_uri = upload_trends_to_gcs(interest_data, bucket_name, interest_path)
    related_uri = upload_trends_to_gcs(related_data, bucket_name, related_path)
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='trending_file', value=trending_uri)
    context['task_instance'].xcom_push(key='interest_file', value=interest_uri)
    context['task_instance'].xcom_push(key='related_file', value=related_uri)

def fetch_linkedin_data(**context):
    """
    Fetch LinkedIn data and upload to GCS.
    """
    # Initialize LinkedIn client
    li_client = get_linkedin_client()
    
    # Get bucket name
    bucket_name = get_required_env_var('GCS_RAW_BUCKET')
    
    try:
        # Fetch user profile data
        profile_info = fetch_profile_info(li_client)
        connections = fetch_connections(li_client)
        activities = fetch_recent_activity(li_client)
        
        # Generate filenames with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Upload to GCS
        profile_path = f'linkedin/raw/profile/profile_{timestamp}.json'
        connections_path = f'linkedin/raw/connections/connections_{timestamp}.json'
        activities_path = f'linkedin/raw/activities/activities_{timestamp}.json'
        
        profile_uri = upload_linkedin_to_gcs(profile_info, bucket_name, profile_path)
        connections_uri = upload_linkedin_to_gcs(connections, bucket_name, connections_path)
        activities_uri = upload_linkedin_to_gcs(activities, bucket_name, activities_path)
        
        # Push file paths to XCom for downstream tasks
        context['task_instance'].xcom_push(key='profile_file', value=profile_uri)
        context['task_instance'].xcom_push(key='connections_file', value=connections_uri)
        context['task_instance'].xcom_push(key='activities_file', value=activities_uri)
        
    except Exception as e:
        print(f"Error fetching LinkedIn data: {str(e)}")
        raise

def process_spotify_raw_data(**context):
    """
    Process raw Spotify data and upload processed data to GCS.
    """
    # Get file paths from XCom
    ti = context['task_instance']
    tracks_uri = ti.xcom_pull(task_ids='fetch_spotify_data', key='tracks_file')
    artists_uri = ti.xcom_pull(task_ids='fetch_spotify_data', key='artists_file')
    
    # Get bucket names
    raw_bucket = get_required_env_var('GCS_RAW_BUCKET')
    processed_bucket = get_required_env_var('GCS_PROCESSED_BUCKET')
    
    # Extract blob paths
    tracks_path = tracks_uri.replace(f'gs://{raw_bucket}/', '')
    artists_path = artists_uri.replace(f'gs://{raw_bucket}/', '')
    
    # Read raw data
    tracks_data = read_from_gcs(raw_bucket, tracks_path)
    artists_data = read_from_gcs(raw_bucket, artists_path)
    
    # Process data
    processed_tracks, processed_artists = process_spotify_data(tracks_data, artists_data)
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    # Upload processed data
    proc_tracks_path = f'spotify/processed/tracks/tracks_{timestamp}.json'
    proc_artists_path = f'spotify/processed/artists/artists_{timestamp}.json'
    
    tracks_uri = upload_processed_data(processed_tracks, processed_bucket, proc_tracks_path)
    artists_uri = upload_processed_data(processed_artists, processed_bucket, proc_artists_path)
    
    # Push processed file paths to XCom
    context['task_instance'].xcom_push(key='processed_tracks_file', value=tracks_uri)
    context['task_instance'].xcom_push(key='processed_artists_file', value=artists_uri)

def process_trends_raw_data(**context):
    """
    Process raw Google Trends data and upload processed data to GCS.
    """
    # Get file paths from XCom
    ti = context['task_instance']
    trending_uri = ti.xcom_pull(task_ids='fetch_trends_data', key='trending_file')
    interest_uri = ti.xcom_pull(task_ids='fetch_trends_data', key='interest_file')
    related_uri = ti.xcom_pull(task_ids='fetch_trends_data', key='related_file')
    
    # Get bucket names
    raw_bucket = get_required_env_var('GCS_RAW_BUCKET')
    processed_bucket = get_required_env_var('GCS_PROCESSED_BUCKET')
    
    # Extract blob paths
    trending_path = trending_uri.replace(f'gs://{raw_bucket}/', '')
    interest_path = interest_uri.replace(f'gs://{raw_bucket}/', '')
    related_path = related_uri.replace(f'gs://{raw_bucket}/', '')
    
    # Read raw data
    trending_data = read_from_gcs(raw_bucket, trending_path)
    interest_data = read_from_gcs(raw_bucket, interest_path)
    related_data = read_from_gcs(raw_bucket, related_path)
    
    # Process data
    processed_trending, processed_interest, processed_related = process_trends_data(
        trending_data, interest_data, related_data
    )
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    # Upload processed data
    proc_trending_path = f'trends/processed/trending/trending_{timestamp}.json'
    proc_interest_path = f'trends/processed/interest/interest_{timestamp}.json'
    proc_related_path = f'trends/processed/related/related_{timestamp}.json'
    
    trending_uri = upload_processed_data(processed_trending, processed_bucket, proc_trending_path)
    interest_uri = upload_processed_data(processed_interest, processed_bucket, proc_interest_path)
    related_uri = upload_processed_data(processed_related, processed_bucket, proc_related_path)
    
    # Push processed file paths to XCom
    context['task_instance'].xcom_push(key='processed_trending_file', value=trending_uri)
    context['task_instance'].xcom_push(key='processed_interest_file', value=interest_uri)
    context['task_instance'].xcom_push(key='processed_related_file', value=related_uri)

def process_linkedin_raw_data(**context):
    """
    Process raw LinkedIn data and upload processed data to GCS.
    """
    # Get file paths from XCom
    ti = context['task_instance']
    profile_uri = ti.xcom_pull(task_ids='fetch_linkedin_data', key='profile_file')
    connections_uri = ti.xcom_pull(task_ids='fetch_linkedin_data', key='connections_file')
    activities_uri = ti.xcom_pull(task_ids='fetch_linkedin_data', key='activities_file')
    
    # Get bucket names
    raw_bucket = get_required_env_var('GCS_RAW_BUCKET')
    processed_bucket = get_required_env_var('GCS_PROCESSED_BUCKET')
    
    # Extract blob paths
    profile_path = profile_uri.replace(f'gs://{raw_bucket}/', '')
    connections_path = connections_uri.replace(f'gs://{raw_bucket}/', '')
    activities_path = activities_uri.replace(f'gs://{raw_bucket}/', '')
    
    # Read raw data
    profile_data = read_from_gcs(raw_bucket, profile_path)
    connections_data = read_from_gcs(raw_bucket, connections_path)
    activities_data = read_from_gcs(raw_bucket, activities_path)
    
    # Process data
    processed_profile, processed_connections, processed_activities = process_linkedin_data(
        profile_data,
        connections_data,
        activities_data
    )
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    
    # Upload processed data
    proc_profile_path = f'linkedin/processed/profile/profile_{timestamp}.json'
    proc_connections_path = f'linkedin/processed/connections/connections_{timestamp}.json'
    proc_activities_path = f'linkedin/processed/activities/activities_{timestamp}.json'
    
    profile_uri = upload_processed_data(processed_profile, processed_bucket, proc_profile_path)
    connections_uri = upload_processed_data(processed_connections, processed_bucket, proc_connections_path)
    activities_uri = upload_processed_data(processed_activities, processed_bucket, proc_activities_path)
    
    # Push processed file paths to XCom
    context['task_instance'].xcom_push(key='processed_profile_file', value=profile_uri)
    context['task_instance'].xcom_push(key='processed_connections_file', value=connections_uri)
    context['task_instance'].xcom_push(key='processed_activities_file', value=activities_uri)

def aggregate_user_data_task(**context):
    """
    Aggregate processed data for each user and store the results.
    """
    # Get the processed bucket name
    processed_bucket = get_required_env_var('GCS_PROCESSED_BUCKET')
    
    # Get list of user profiles
    storage_client = storage.Client()
    bucket = storage_client.bucket(processed_bucket)
    
    # Process each user profile
    for blob in bucket.list_blobs(prefix='user_profiles/'):
        if not blob.name.endswith('.json'):
            continue
            
        # Get user ID from filename
        user_id = blob.name.split('/')[-1].replace('.json', '')
        
        try:
            # Aggregate data for this user
            aggregated_data = aggregate_user_data(
                user_id=user_id,
                processed_bucket=processed_bucket
            )
            
            # Store aggregated data
            uri = store_aggregated_data(aggregated_data, processed_bucket)
            
            # Log the result
            print(f"Aggregated data for user {user_id} stored at: {uri}")
        except Exception as e:
            print(f"Error processing user {user_id}: {str(e)}")
            continue

with DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Data ingestion pipeline for Prysm',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['prysm', 'data_ingestion'],
) as dag:
    
    # Data fetching tasks
    spotify_task = PythonOperator(
        task_id='fetch_spotify_data',
        python_callable=fetch_spotify_data,
    )
    
    trends_task = PythonOperator(
        task_id='fetch_trends_data',
        python_callable=fetch_trends_data,
    )
    
    linkedin_task = PythonOperator(
        task_id='fetch_linkedin_data',
        python_callable=fetch_linkedin_data,
    )
    
    # Data processing tasks
    process_spotify_task = PythonOperator(
        task_id='process_spotify_data',
        python_callable=process_spotify_raw_data,
    )
    
    process_trends_task = PythonOperator(
        task_id='process_trends_data',
        python_callable=process_trends_raw_data,
    )
    
    process_linkedin_task = PythonOperator(
        task_id='process_linkedin_data',
        python_callable=process_linkedin_raw_data,
    )
    
    # Aggregate user data task
    aggregate_data = PythonOperator(
        task_id='aggregate_user_data',
        python_callable=aggregate_user_data_task
    )
    
    # Set up task dependencies
    spotify_task.set_downstream(process_spotify_task)
    trends_task.set_downstream(process_trends_task)
    linkedin_task.set_downstream(process_linkedin_task)
    
    process_spotify_task.set_downstream(aggregate_data)
    process_trends_task.set_downstream(aggregate_data)
    process_linkedin_task.set_downstream(aggregate_data) 