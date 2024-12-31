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
    fetch_company_info,
    fetch_company_updates,
    fetch_job_postings,
    upload_to_gcs as upload_linkedin_to_gcs
)
from utils.data_processing_utils import (
    read_from_gcs,
    process_spotify_data,
    process_trends_data,
    process_linkedin_data,
    upload_processed_data
)
import os
from dotenv import load_dotenv

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
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    bucket_name = os.getenv('GCS_RAW_BUCKET')
    
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
    
    # Get target companies from environment variables
    companies = os.getenv('LINKEDIN_TARGET_COMPANIES', '').split(',')
    
    all_company_info = []
    all_company_updates = []
    all_job_postings = []
    
    for company_id in companies:
        # Fetch company data
        company_info = fetch_company_info(li_client, company_id)
        company_updates = fetch_company_updates(li_client, company_id)
        job_postings = fetch_job_postings(li_client, company_id)
        
        all_company_info.append(company_info)
        all_company_updates.extend(company_updates)
        all_job_postings.extend(job_postings)
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    bucket_name = os.getenv('GCS_RAW_BUCKET')
    
    # Upload to GCS
    companies_path = f'linkedin/raw/companies/companies_{timestamp}.json'
    updates_path = f'linkedin/raw/updates/updates_{timestamp}.json'
    jobs_path = f'linkedin/raw/jobs/jobs_{timestamp}.json'
    
    companies_uri = upload_linkedin_to_gcs(all_company_info, bucket_name, companies_path)
    updates_uri = upload_linkedin_to_gcs(all_company_updates, bucket_name, updates_path)
    jobs_uri = upload_linkedin_to_gcs(all_job_postings, bucket_name, jobs_path)
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='companies_file', value=companies_uri)
    context['task_instance'].xcom_push(key='updates_file', value=updates_uri)
    context['task_instance'].xcom_push(key='jobs_file', value=jobs_uri)

def process_spotify_raw_data(**context):
    """
    Process raw Spotify data and upload processed data to GCS.
    """
    # Get file paths from XCom
    ti = context['task_instance']
    tracks_uri = ti.xcom_pull(task_ids='fetch_spotify_data', key='tracks_file')
    artists_uri = ti.xcom_pull(task_ids='fetch_spotify_data', key='artists_file')
    
    # Extract bucket and blob paths
    raw_bucket = os.getenv('GCS_RAW_BUCKET')
    tracks_path = tracks_uri.replace(f'gs://{raw_bucket}/', '')
    artists_path = artists_uri.replace(f'gs://{raw_bucket}/', '')
    
    # Read raw data
    tracks_data = read_from_gcs(raw_bucket, tracks_path)
    artists_data = read_from_gcs(raw_bucket, artists_path)
    
    # Process data
    processed_tracks, processed_artists = process_spotify_data(tracks_data, artists_data)
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    processed_bucket = os.getenv('GCS_PROCESSED_BUCKET')
    
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
    
    # Extract bucket and blob paths
    raw_bucket = os.getenv('GCS_RAW_BUCKET')
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
    processed_bucket = os.getenv('GCS_PROCESSED_BUCKET')
    
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
    companies_uri = ti.xcom_pull(task_ids='fetch_linkedin_data', key='companies_file')
    updates_uri = ti.xcom_pull(task_ids='fetch_linkedin_data', key='updates_file')
    jobs_uri = ti.xcom_pull(task_ids='fetch_linkedin_data', key='jobs_file')
    
    # Extract bucket and blob paths
    raw_bucket = os.getenv('GCS_RAW_BUCKET')
    companies_path = companies_uri.replace(f'gs://{raw_bucket}/', '')
    updates_path = updates_uri.replace(f'gs://{raw_bucket}/', '')
    jobs_path = jobs_uri.replace(f'gs://{raw_bucket}/', '')
    
    # Read raw data
    companies_data = read_from_gcs(raw_bucket, companies_path)
    updates_data = read_from_gcs(raw_bucket, updates_path)
    jobs_data = read_from_gcs(raw_bucket, jobs_path)
    
    # Process data
    processed_companies, processed_updates, processed_jobs = process_linkedin_data(
        companies_data[0],  # Process first company for now
        updates_data,
        jobs_data
    )
    
    # Generate filenames with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    processed_bucket = os.getenv('GCS_PROCESSED_BUCKET')
    
    # Upload processed data
    proc_companies_path = f'linkedin/processed/companies/companies_{timestamp}.json'
    proc_updates_path = f'linkedin/processed/updates/updates_{timestamp}.json'
    proc_jobs_path = f'linkedin/processed/jobs/jobs_{timestamp}.json'
    
    companies_uri = upload_processed_data(processed_companies, processed_bucket, proc_companies_path)
    updates_uri = upload_processed_data(processed_updates, processed_bucket, proc_updates_path)
    jobs_uri = upload_processed_data(processed_jobs, processed_bucket, proc_jobs_path)
    
    # Push processed file paths to XCom
    context['task_instance'].xcom_push(key='processed_companies_file', value=companies_uri)
    context['task_instance'].xcom_push(key='processed_updates_file', value=updates_uri)
    context['task_instance'].xcom_push(key='processed_jobs_file', value=jobs_uri)

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
    
    # Set up task dependencies
    spotify_task >> process_spotify_task
    trends_task >> process_trends_task
    linkedin_task >> process_linkedin_task 