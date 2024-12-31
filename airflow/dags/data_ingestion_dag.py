from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from utils.spotify_utils import get_spotify_client, fetch_trending_tracks, upload_to_gcs
from utils.google_trends_utils import (
    get_google_trends_client,
    fetch_trending_topics,
    fetch_topic_interest,
    fetch_related_topics
)
from utils.linkedin_utils import (
    get_linkedin_client,
    fetch_profile_info,
    fetch_connections,
    fetch_recent_activity
)
from utils.data_processing_utils import (
    read_from_gcs,
    process_spotify_data,
    process_trends_data,
    process_linkedin_data,
    upload_processed_data
)
from utils.data_quality_utils import (
    validate_spotify_data,
    validate_trends_data,
    validate_linkedin_data,
    check_data_freshness,
    validate_processed_data
)
from utils.data_transformation_utils import (
    transform_spotify_data,
    transform_trends_data,
    transform_linkedin_data,
    aggregate_data,
    save_to_bigquery
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['admin@example.com'],  # Replace with actual email address
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_spotify_data(**context):
    # Initialize Spotify client
    client = get_spotify_client()
    
    # Fetch trending tracks
    tracks_data = fetch_trending_tracks(client)
    
    # Extract unique artist IDs
    artist_ids = list(set(track['artists'][0]['id'] for track in tracks_data))
    
    # Fetch artist data
    artist_data = [client.artist(artist_id) for artist_id in artist_ids]
    
    # Generate filenames with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    tracks_filename = f'spotify_tracks_{timestamp}.json'
    artists_filename = f'spotify_artists_{timestamp}.json'
    
    # Upload to GCS
    tracks_path = upload_to_gcs(tracks_data, 'prysm-raw-data', f'spotify/tracks/{tracks_filename}')
    artists_path = upload_to_gcs(artist_data, 'prysm-raw-data', f'spotify/artists/{artists_filename}')
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='tracks_path', value=tracks_path)
    context['task_instance'].xcom_push(key='artists_path', value=artists_path)

def fetch_trends_data(**context):
    # Initialize Google Trends client
    client = get_google_trends_client()
    
    # Fetch trending topics
    topics_data = fetch_trending_topics(client)
    
    # Extract topic names for detailed analysis
    topics = [topic['topic'] for topic in topics_data]
    
    # Fetch interest over time and related topics
    interest_data = fetch_topic_interest(client, topics)
    related_data = fetch_related_topics(client, topics)
    
    # Generate filenames with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    topics_filename = f'trends_topics_{timestamp}.json'
    interest_filename = f'trends_interest_{timestamp}.json'
    related_filename = f'trends_related_{timestamp}.json'
    
    # Upload to GCS
    topics_path = upload_to_gcs(topics_data, 'prysm-raw-data', f'google_trends/topics/{topics_filename}')
    interest_path = upload_to_gcs(interest_data, 'prysm-raw-data', f'google_trends/interest/{interest_filename}')
    related_path = upload_to_gcs(related_data, 'prysm-raw-data', f'google_trends/related/{related_filename}')
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='topics_path', value=topics_path)
    context['task_instance'].xcom_push(key='interest_path', value=interest_path)
    context['task_instance'].xcom_push(key='related_path', value=related_path)

def fetch_linkedin_data(**context):
    # Initialize LinkedIn client
    client = get_linkedin_client()
    
    # Fetch profile info, connections, and recent activity
    profile_data = fetch_profile_info(client)
    connections_data = fetch_connections(client)
    activity_data = fetch_recent_activity(client)
    
    # Generate filenames with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    profile_filename = f'linkedin_profile_{timestamp}.json'
    connections_filename = f'linkedin_connections_{timestamp}.json'
    activity_filename = f'linkedin_activity_{timestamp}.json'
    
    # Upload to GCS
    profile_path = upload_to_gcs(profile_data, 'prysm-raw-data', f'linkedin/profile/{profile_filename}')
    connections_path = upload_to_gcs(connections_data, 'prysm-raw-data', f'linkedin/connections/{connections_filename}')
    activity_path = upload_to_gcs(activity_data, 'prysm-raw-data', f'linkedin/activity/{activity_filename}')
    
    # Push file paths to XCom for downstream tasks
    context['task_instance'].xcom_push(key='profile_path', value=profile_path)
    context['task_instance'].xcom_push(key='connections_path', value=connections_path)
    context['task_instance'].xcom_push(key='activity_path', value=activity_path)

def process_spotify_task(**context):
    # Get file paths from XCom
    tracks_path = context['task_instance'].xcom_pull(task_ids='fetch_spotify_data', key='tracks_path')
    artists_path = context['task_instance'].xcom_pull(task_ids='fetch_spotify_data', key='artists_path')
    
    # Read data from GCS
    tracks_data = read_from_gcs('prysm-raw-data', tracks_path.replace('gs://prysm-raw-data/', ''))
    artists_data = read_from_gcs('prysm-raw-data', artists_path.replace('gs://prysm-raw-data/', ''))
    
    # Process data
    processed_tracks, processed_artists = process_spotify_data(tracks_data, artists_data)
    
    # Generate filenames with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    tracks_filename = f'processed_tracks_{timestamp}.json'
    artists_filename = f'processed_artists_{timestamp}.json'
    
    # Upload processed data
    processed_tracks_path = upload_processed_data(processed_tracks, 'prysm-processed-data', f'spotify/tracks/{tracks_filename}')
    processed_artists_path = upload_processed_data(processed_artists, 'prysm-processed-data', f'spotify/artists/{artists_filename}')
    
    # Push processed file paths to XCom
    context['task_instance'].xcom_push(key='processed_tracks_path', value=processed_tracks_path)
    context['task_instance'].xcom_push(key='processed_artists_path', value=processed_artists_path)

def process_trends_task(**context):
    # Get file paths from XCom
    topics_path = context['task_instance'].xcom_pull(task_ids='fetch_trends_data', key='topics_path')
    interest_path = context['task_instance'].xcom_pull(task_ids='fetch_trends_data', key='interest_path')
    related_path = context['task_instance'].xcom_pull(task_ids='fetch_trends_data', key='related_path')
    
    # Read data from GCS
    topics_data = read_from_gcs('prysm-raw-data', topics_path.replace('gs://prysm-raw-data/', ''))
    interest_data = read_from_gcs('prysm-raw-data', interest_path.replace('gs://prysm-raw-data/', ''))
    related_data = read_from_gcs('prysm-raw-data', related_path.replace('gs://prysm-raw-data/', ''))
    
    # Process data
    processed_topics, processed_interest, processed_related = process_trends_data(topics_data, interest_data, related_data)
    
    # Generate filenames with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    topics_filename = f'processed_topics_{timestamp}.json'
    interest_filename = f'processed_interest_{timestamp}.json'
    related_filename = f'processed_related_{timestamp}.json'
    
    # Upload processed data
    processed_topics_path = upload_processed_data(processed_topics, 'prysm-processed-data', f'google_trends/topics/{topics_filename}')
    processed_interest_path = upload_processed_data(processed_interest, 'prysm-processed-data', f'google_trends/interest/{interest_filename}')
    processed_related_path = upload_processed_data(processed_related, 'prysm-processed-data', f'google_trends/related/{related_filename}')
    
    # Push processed file paths to XCom
    context['task_instance'].xcom_push(key='processed_topics_path', value=processed_topics_path)
    context['task_instance'].xcom_push(key='processed_interest_path', value=processed_interest_path)
    context['task_instance'].xcom_push(key='processed_related_path', value=processed_related_path)

def process_linkedin_task(**context):
    # Get file paths from XCom
    profile_path = context['task_instance'].xcom_pull(task_ids='fetch_linkedin_data', key='profile_path')
    connections_path = context['task_instance'].xcom_pull(task_ids='fetch_linkedin_data', key='connections_path')
    activity_path = context['task_instance'].xcom_pull(task_ids='fetch_linkedin_data', key='activity_path')
    
    # Read data from GCS
    profile_data = read_from_gcs('prysm-raw-data', profile_path.replace('gs://prysm-raw-data/', ''))
    connections_data = read_from_gcs('prysm-raw-data', connections_path.replace('gs://prysm-raw-data/', ''))
    activity_data = read_from_gcs('prysm-raw-data', activity_path.replace('gs://prysm-raw-data/', ''))
    
    # Process data
    processed_profile, processed_connections, processed_activities = process_linkedin_data(profile_data, connections_data, activity_data)
    
    # Generate filenames with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    profile_filename = f'processed_profile_{timestamp}.json'
    connections_filename = f'processed_connections_{timestamp}.json'
    activities_filename = f'processed_activities_{timestamp}.json'
    
    # Upload processed data
    processed_profile_path = upload_processed_data(processed_profile, 'prysm-processed-data', f'linkedin/profile/{profile_filename}')
    processed_connections_path = upload_processed_data(processed_connections, 'prysm-processed-data', f'linkedin/connections/{connections_filename}')
    processed_activities_path = upload_processed_data(processed_activities, 'prysm-processed-data', f'linkedin/activities/{activities_filename}')
    
    # Push processed file paths to XCom
    context['task_instance'].xcom_push(key='processed_profile_path', value=processed_profile_path)
    context['task_instance'].xcom_push(key='processed_connections_path', value=processed_connections_path)
    context['task_instance'].xcom_push(key='processed_activities_path', value=processed_activities_path)

def check_spotify_data_quality(**context):
    """Check quality of Spotify data and determine next task."""
    # Get file paths from XCom
    tracks_path = context['task_instance'].xcom_pull(task_ids='fetch_spotify_data', key='tracks_path')
    artists_path = context['task_instance'].xcom_pull(task_ids='fetch_spotify_data', key='artists_path')
    
    # Read data from GCS
    tracks_data = read_from_gcs('prysm-raw-data', tracks_path.replace('gs://prysm-raw-data/', ''))
    artists_data = read_from_gcs('prysm-raw-data', artists_path.replace('gs://prysm-raw-data/', ''))
    
    # Validate data
    is_valid, errors = validate_spotify_data(tracks_data, artists_data)
    
    # Check data freshness
    is_fresh, freshness_error = check_data_freshness(tracks_data[0].get('timestamp', ''))
    
    if not is_valid or not is_fresh:
        error_message = "Data quality check failed:\n"
        if not is_valid:
            error_message += "\n".join(errors)
        if not is_fresh and freshness_error:
            error_message += f"\n{freshness_error}"
        
        context['task_instance'].xcom_push(key='spotify_quality_errors', value=error_message)
        return 'spotify_quality_failed'
    
    return 'process_spotify_data'

def check_trends_data_quality(**context):
    """Check quality of Google Trends data and determine next task."""
    # Get file paths from XCom
    topics_path = context['task_instance'].xcom_pull(task_ids='fetch_trends_data', key='topics_path')
    interest_path = context['task_instance'].xcom_pull(task_ids='fetch_trends_data', key='interest_path')
    related_path = context['task_instance'].xcom_pull(task_ids='fetch_trends_data', key='related_path')
    
    # Read data from GCS
    topics_data = read_from_gcs('prysm-raw-data', topics_path.replace('gs://prysm-raw-data/', ''))
    interest_data = read_from_gcs('prysm-raw-data', interest_path.replace('gs://prysm-raw-data/', ''))
    related_data = read_from_gcs('prysm-raw-data', related_path.replace('gs://prysm-raw-data/', ''))
    
    # Validate data
    is_valid, errors = validate_trends_data(topics_data, interest_data, related_data)
    
    # Check data freshness
    is_fresh, freshness_error = check_data_freshness(topics_data[0].get('timestamp', ''))
    
    if not is_valid or not is_fresh:
        error_message = "Data quality check failed:\n"
        if not is_valid:
            error_message += "\n".join(errors)
        if not is_fresh and freshness_error:
            error_message += f"\n{freshness_error}"
        
        context['task_instance'].xcom_push(key='trends_quality_errors', value=error_message)
        return 'trends_quality_failed'
    
    return 'process_trends_data'

def check_linkedin_data_quality(**context):
    """Check quality of LinkedIn data and determine next task."""
    # Get file paths from XCom
    profile_path = context['task_instance'].xcom_pull(task_ids='fetch_linkedin_data', key='profile_path')
    connections_path = context['task_instance'].xcom_pull(task_ids='fetch_linkedin_data', key='connections_path')
    activity_path = context['task_instance'].xcom_pull(task_ids='fetch_linkedin_data', key='activity_path')
    
    # Read data from GCS
    profile_data = read_from_gcs('prysm-raw-data', profile_path.replace('gs://prysm-raw-data/', ''))
    connections_data = read_from_gcs('prysm-raw-data', connections_path.replace('gs://prysm-raw-data/', ''))
    activity_data = read_from_gcs('prysm-raw-data', activity_path.replace('gs://prysm-raw-data/', ''))
    
    # Validate data
    is_valid, errors = validate_linkedin_data(profile_data, connections_data, activity_data)
    
    # Check data freshness
    is_fresh, freshness_error = check_data_freshness(profile_data.get('timestamp', ''))
    
    if not is_valid or not is_fresh:
        error_message = "Data quality check failed:\n"
        if not is_valid:
            error_message += "\n".join(errors)
        if not is_fresh and freshness_error:
            error_message += f"\n{freshness_error}"
        
        context['task_instance'].xcom_push(key='linkedin_quality_errors', value=error_message)
        return 'linkedin_quality_failed'
    
    return 'process_linkedin_data'

def check_processed_data_quality(**context):
    """Check quality of processed data and determine next task."""
    # Get file paths from XCom
    processed_tracks_path = context['task_instance'].xcom_pull(task_ids='process_spotify_data', key='processed_tracks_path')
    processed_artists_path = context['task_instance'].xcom_pull(task_ids='process_spotify_data', key='processed_artists_path')
    processed_topics_path = context['task_instance'].xcom_pull(task_ids='process_trends_data', key='processed_topics_path')
    processed_profile_path = context['task_instance'].xcom_pull(task_ids='process_linkedin_data', key='processed_profile_path')
    
    # Read processed data from GCS
    tracks_data = read_from_gcs('prysm-processed-data', processed_tracks_path.replace('gs://prysm-processed-data/', ''))
    artists_data = read_from_gcs('prysm-processed-data', processed_artists_path.replace('gs://prysm-processed-data/', ''))
    topics_data = read_from_gcs('prysm-processed-data', processed_topics_path.replace('gs://prysm-processed-data/', ''))
    profile_data = read_from_gcs('prysm-processed-data', processed_profile_path.replace('gs://prysm-processed-data/', ''))
    
    # Define schemas for processed data
    track_schema = {
        'id': str,
        'name': str,
        'popularity': int,
        'artist_id': str,
        'artist_name': str,
        'album': str,
        'release_date': str,
        'duration_ms': int,
        'explicit': bool,
        'timestamp': str
    }
    
    artist_schema = {
        'id': str,
        'name': str,
        'genres': list,
        'popularity': int,
        'followers': int,
        'timestamp': str
    }
    
    topic_schema = {
        'topic': str,
        'interest_over_time': dict,
        'timestamp': str
    }
    
    profile_schema = {
        'id': str,
        'first_name': str,
        'last_name': str,
        'headline': str,
        'industry': str,
        'timestamp': str
    }
    
    # Validate processed data
    errors = []
    
    # Check tracks
    for track in tracks_data:
        is_valid, track_errors = validate_processed_data(track, track_schema)
        if not is_valid:
            errors.extend([f"Track {track.get('id', 'Unknown')}: {error}" for error in track_errors])
    
    # Check artists
    for artist in artists_data:
        is_valid, artist_errors = validate_processed_data(artist, artist_schema)
        if not is_valid:
            errors.extend([f"Artist {artist.get('id', 'Unknown')}: {error}" for error in artist_errors])
    
    # Check topics
    for topic in topics_data:
        is_valid, topic_errors = validate_processed_data(topic, topic_schema)
        if not is_valid:
            errors.extend([f"Topic {topic.get('topic', 'Unknown')}: {error}" for error in topic_errors])
    
    # Check profile
    is_valid, profile_errors = validate_processed_data(profile_data, profile_schema)
    if not is_valid:
        errors.extend([f"Profile: {error}" for error in profile_errors])
    
    if errors:
        error_message = "Processed data quality check failed:\n" + "\n".join(errors)
        context['task_instance'].xcom_push(key='processed_quality_errors', value=error_message)
        return 'processed_quality_failed'
    
    return 'data_quality_success'

def handle_quality_failure(context):
    """Handle quality check failures by sending notifications."""
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context['execution_date']
    
    # Get error messages from XCom
    error_key = f"{task_id.replace('check_', '').replace('_quality', '')}_quality_errors"
    error_message = task_instance.xcom_pull(key=error_key)
    
    # Log the error
    print(f"Data quality check failed for {task_id}:")
    print(error_message)
    
    # Prepare email content
    subject = f"Data Quality Check Failed - {task_id}"
    html_content = f"""
    <h3>Data Quality Check Failed</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <h4>Error Details:</h4>
    <pre>{error_message}</pre>
    """
    
    # Send email notification
    from airflow.utils.email import send_email
    send_email(
        to=dag.default_args['email'],
        subject=subject,
        html_content=html_content
    )

def transform_and_aggregate_data(**context):
    """Transform and aggregate data from all sources."""
    # Get processed data paths from XCom
    processed_tracks_path = context['task_instance'].xcom_pull(task_ids='process_spotify_data', key='processed_tracks_path')
    processed_artists_path = context['task_instance'].xcom_pull(task_ids='process_spotify_data', key='processed_artists_path')
    processed_topics_path = context['task_instance'].xcom_pull(task_ids='process_trends_data', key='processed_topics_path')
    processed_interest_path = context['task_instance'].xcom_pull(task_ids='process_trends_data', key='processed_interest_path')
    processed_related_path = context['task_instance'].xcom_pull(task_ids='process_trends_data', key='processed_related_path')
    processed_profile_path = context['task_instance'].xcom_pull(task_ids='process_linkedin_data', key='processed_profile_path')
    processed_connections_path = context['task_instance'].xcom_pull(task_ids='process_linkedin_data', key='processed_connections_path')
    processed_activities_path = context['task_instance'].xcom_pull(task_ids='process_linkedin_data', key='processed_activities_path')
    
    # Read processed data from GCS
    tracks_data = read_from_gcs('prysm-processed-data', processed_tracks_path.replace('gs://prysm-processed-data/', ''))
    artists_data = read_from_gcs('prysm-processed-data', processed_artists_path.replace('gs://prysm-processed-data/', ''))
    topics_data = read_from_gcs('prysm-processed-data', processed_topics_path.replace('gs://prysm-processed-data/', ''))
    interest_data = read_from_gcs('prysm-processed-data', processed_interest_path.replace('gs://prysm-processed-data/', ''))
    related_data = read_from_gcs('prysm-processed-data', processed_related_path.replace('gs://prysm-processed-data/', ''))
    profile_data = read_from_gcs('prysm-processed-data', processed_profile_path.replace('gs://prysm-processed-data/', ''))
    connections_data = read_from_gcs('prysm-processed-data', processed_connections_path.replace('gs://prysm-processed-data/', ''))
    activities_data = read_from_gcs('prysm-processed-data', processed_activities_path.replace('gs://prysm-processed-data/', ''))
    
    # Transform data
    spotify_df = transform_spotify_data(tracks_data, artists_data)
    trends_df = transform_trends_data(topics_data, interest_data, related_data)
    linkedin_df = transform_linkedin_data(profile_data, connections_data, activities_data)
    
    # Aggregate data
    aggregated_data = aggregate_data(spotify_df, trends_df, linkedin_df)
    
    # Save to BigQuery
    project_id = 'prysm-backend'  # Replace with your project ID
    dataset_id = 'prysm_recommendations'
    table_id = 'aggregated_insights'
    
    save_to_bigquery(aggregated_data, project_id, dataset_id, table_id)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    blob_path = f'aggregated/insights_{timestamp}.json'
    
    # Save aggregated data to GCS
    aggregated_path = upload_to_gcs(aggregated_data, 'prysm-processed-data', blob_path)
    
    # Push aggregated data path to XCom
    context['task_instance'].xcom_push(key='aggregated_data_path', value=aggregated_path)

with DAG(
    'data_ingestion_dag',
    default_args=default_args,
    description='Data ingestion DAG for Spotify, Google Trends, and LinkedIn data',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    
    # Data fetching tasks
    spotify_task = PythonOperator(
        task_id='fetch_spotify_data',
        python_callable=fetch_spotify_data,
        provide_context=True,
    )
    
    trends_task = PythonOperator(
        task_id='fetch_trends_data',
        python_callable=fetch_trends_data,
        provide_context=True,
    )
    
    linkedin_task = PythonOperator(
        task_id='fetch_linkedin_data',
        python_callable=fetch_linkedin_data,
        provide_context=True,
    )
    
    # Data quality check tasks
    check_spotify = BranchPythonOperator(
        task_id='check_spotify_quality',
        python_callable=check_spotify_data_quality,
        provide_context=True,
    )
    
    check_trends = BranchPythonOperator(
        task_id='check_trends_quality',
        python_callable=check_trends_data_quality,
        provide_context=True,
    )
    
    check_linkedin = BranchPythonOperator(
        task_id='check_linkedin_quality',
        python_callable=check_linkedin_data_quality,
        provide_context=True,
    )
    
    check_processed = BranchPythonOperator(
        task_id='check_processed_quality',
        python_callable=check_processed_data_quality,
        provide_context=True,
    )
    
    # Data processing tasks
    process_spotify = PythonOperator(
        task_id='process_spotify_data',
        python_callable=process_spotify_task,
        provide_context=True,
    )
    
    process_trends = PythonOperator(
        task_id='process_trends_data',
        python_callable=process_trends_task,
        provide_context=True,
    )
    
    process_linkedin = PythonOperator(
        task_id='process_linkedin_data',
        python_callable=process_linkedin_task,
        provide_context=True,
    )
    
    # Quality check failure tasks
    spotify_quality_failed = PythonOperator(
        task_id='spotify_quality_failed',
        python_callable=handle_quality_failure,
        provide_context=True,
    )
    
    trends_quality_failed = PythonOperator(
        task_id='trends_quality_failed',
        python_callable=handle_quality_failure,
        provide_context=True,
    )
    
    linkedin_quality_failed = PythonOperator(
        task_id='linkedin_quality_failed',
        python_callable=handle_quality_failure,
        provide_context=True,
    )
    
    processed_quality_failed = PythonOperator(
        task_id='processed_quality_failed',
        python_callable=handle_quality_failure,
        provide_context=True,
    )
    
    # Success task
    data_quality_success = EmptyOperator(
        task_id='data_quality_success',
        trigger_rule='all_success'  # Only trigger if all upstream tasks succeed
    )
    
    # Add new task to DAG
    transform_task = PythonOperator(
        task_id='transform_and_aggregate_data',
        python_callable=transform_and_aggregate_data,
        provide_context=True,
    )
    
    # Set task dependencies
    spotify_task.set_downstream(check_spotify)
    check_spotify.set_downstream([process_spotify, spotify_quality_failed])
    process_spotify.set_downstream(trends_task)
    trends_task.set_downstream(check_trends)
    check_trends.set_downstream([process_trends, trends_quality_failed])
    process_trends.set_downstream(linkedin_task)
    linkedin_task.set_downstream(check_linkedin)
    check_linkedin.set_downstream([process_linkedin, linkedin_quality_failed])
    process_linkedin.set_downstream(check_processed)
    check_processed.set_downstream([data_quality_success, processed_quality_failed])
    data_quality_success.set_downstream(transform_task) 