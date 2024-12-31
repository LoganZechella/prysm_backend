from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Import utility modules using relative imports
from utils.spotify_utils import fetch_spotify_data
from utils.event_collection_utils import (
    EventbriteClient,
    transform_eventbrite_event,
    upload_to_gcs
)
from utils.data_quality_utils import (
    validate_spotify_data,
    validate_trends_data,
    validate_event_data,
    validate_processed_data,
    check_data_freshness,
    check_data_completeness
)
from utils.data_aggregation_utils import (
    aggregate_event_data,
    aggregate_trends_data,
    generate_event_insights,
    generate_trends_insights,
    save_insights_to_bigquery
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_eventbrite_data(**context):
    """Fetch event data from Eventbrite API."""
    client = EventbriteClient()
    
    # Set search parameters
    location = "San Francisco, CA"  # Can be parameterized based on user preferences
    categories = ["103"]  # Music category ID
    start_date = datetime.utcnow().isoformat() + 'Z'
    end_date = (datetime.utcnow() + timedelta(days=30)).isoformat() + 'Z'
    
    # Search for events
    events = []
    page = 1
    while True:
        response = client.search_events(
            location=location,
            categories=categories,
            start_date=start_date,
            end_date=end_date,
            page=page
        )
        
        if not response.get('events'):
            break
            
        # Transform each event to our schema
        for event in response['events']:
            transformed_event = transform_eventbrite_event(event)
            events.append(transformed_event)
        
        # Check if there are more pages
        pagination = response.get('pagination', {})
        if page >= pagination.get('page_count', 1):
            break
        page += 1
    
    # Generate filename with timestamp
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    filename = f'eventbrite/raw/events_{timestamp}.json'
    
    # Upload to GCS
    bucket_name = 'prysm-raw-data'
    events_path = upload_to_gcs({'events': events}, bucket_name, filename)
    
    # Push file path to XCom for downstream tasks
    context['task_instance'].xcom_push(key='events_path', value=events_path)
    
    return events_path

def handle_quality_failure(task_id, **context):
    """Handle quality check failures."""
    # Get error messages from XCom
    error_messages = context['task_instance'].xcom_pull(task_ids=task_id)
    
    # Log the error messages
    for error in error_messages:
        print(f"Quality check failed: {error}")
    
    # In a real implementation, you might want to:
    # 1. Send notifications (email, Slack, etc.)
    # 2. Create incident tickets
    # 3. Trigger recovery workflows
    # 4. Update monitoring dashboards

with DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Pipeline for ingesting data from various sources',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['prysm', 'data_ingestion'],
) as dag:
    
    # Start task
    start = EmptyOperator(task_id='start')
    
    # Data collection tasks
    with TaskGroup('data_collection') as data_collection:
        spotify_task = PythonOperator(
            task_id='fetch_spotify_data',
            python_callable=fetch_spotify_data
        )
        
        eventbrite_task = PythonOperator(
            task_id='fetch_eventbrite_data',
            python_callable=fetch_eventbrite_data
        )
    
    # Data quality checks
    with TaskGroup('data_quality_checks') as quality_checks:
        spotify_quality = PythonOperator(
            task_id='check_spotify_quality',
            python_callable=validate_spotify_data
        )
        
        event_quality = PythonOperator(
            task_id='check_event_quality',
            python_callable=validate_event_data
        )
        
        trends_quality = PythonOperator(
            task_id='check_trends_quality',
            python_callable=validate_trends_data
        )
        
        processed_quality = PythonOperator(
            task_id='check_processed_quality',
            python_callable=validate_processed_data
        )
    
    # Data quality check failure handlers
    spotify_quality_failed = PythonOperator(
        task_id='spotify_quality_failed',
        python_callable=handle_quality_failure,
        op_kwargs={'task_id': 'check_spotify_quality'},
        trigger_rule='one_failed'
    )
    
    event_quality_failed = PythonOperator(
        task_id='event_quality_failed',
        python_callable=handle_quality_failure,
        op_kwargs={'task_id': 'check_event_quality'},
        trigger_rule='one_failed'
    )
    
    trends_quality_failed = PythonOperator(
        task_id='trends_quality_failed',
        python_callable=handle_quality_failure,
        op_kwargs={'task_id': 'check_trends_quality'},
        trigger_rule='one_failed'
    )
    
    processed_quality_failed = PythonOperator(
        task_id='processed_quality_failed',
        python_callable=handle_quality_failure,
        op_kwargs={'task_id': 'check_processed_quality'},
        trigger_rule='one_failed'
    )
    
    # Success task
    data_quality_success = EmptyOperator(
        task_id='data_quality_success',
        trigger_rule='all_success'
    )

    # Data aggregation and insights generation
    with TaskGroup('data_aggregation') as aggregation:
        # Aggregate event data
        event_aggregation = PythonOperator(
            task_id='aggregate_event_data',
            python_callable=aggregate_event_data,
            op_kwargs={
                'processed_bucket': 'prysm-processed-data',
                'start_date': '{{ macros.ds_add(ds, -7) }}',  # Last 7 days
                'end_date': '{{ ds }}'
            }
        )
        
        # Aggregate trends data
        trends_aggregation = PythonOperator(
            task_id='aggregate_trends_data',
            python_callable=aggregate_trends_data,
            op_kwargs={
                'processed_bucket': 'prysm-processed-data',
                'start_date': '{{ macros.ds_add(ds, -7) }}',  # Last 7 days
                'end_date': '{{ ds }}'
            }
        )
        
        # Generate event insights
        event_insights = PythonOperator(
            task_id='generate_event_insights',
            python_callable=generate_event_insights,
            op_kwargs={
                'events_df': '{{ task_instance.xcom_pull(task_ids="aggregate_event_data") }}'
            }
        )
        
        # Generate trends insights
        trends_insights = PythonOperator(
            task_id='generate_trends_insights',
            python_callable=generate_trends_insights,
            op_kwargs={
                'trends_data': '{{ task_instance.xcom_pull(task_ids="aggregate_trends_data") }}'
            }
        )
        
        # Save insights to BigQuery
        save_event_insights = PythonOperator(
            task_id='save_event_insights',
            python_callable=save_insights_to_bigquery,
            op_kwargs={
                'insights': '{{ task_instance.xcom_pull(task_ids="generate_event_insights") }}',
                'project_id': 'prysm-dev',
                'dataset_id': 'insights',
                'table_id': 'event_insights'
            }
        )
        
        save_trends_insights = PythonOperator(
            task_id='save_trends_insights',
            python_callable=save_insights_to_bigquery,
            op_kwargs={
                'insights': '{{ task_instance.xcom_pull(task_ids="generate_trends_insights") }}',
                'project_id': 'prysm-dev',
                'dataset_id': 'insights',
                'table_id': 'trends_insights'
            }
        )
        
        # Set up task dependencies within the aggregation group
        event_aggregation >> event_insights >> save_event_insights
        trends_aggregation >> trends_insights >> save_trends_insights
    
    # End task
    end = EmptyOperator(task_id='end')
    
    # Set up task dependencies
    start >> [spotify_task, eventbrite_task]
    spotify_task >> spotify_quality
    eventbrite_task >> event_quality
    [spotify_quality, event_quality] >> trends_quality
    trends_quality >> processed_quality
    [processed_quality] >> data_quality_success
    
    # Quality check failure paths
    spotify_quality >> [spotify_quality_failed, data_quality_success]
    event_quality >> [event_quality_failed, data_quality_success]
    trends_quality >> [trends_quality_failed, data_quality_success]
    processed_quality >> [processed_quality_failed, data_quality_success]
    
    # Connect data quality to aggregation
    data_quality_success >> aggregation
    
    # Final task dependencies
    [aggregation, spotify_quality_failed, event_quality_failed, 
     trends_quality_failed, processed_quality_failed] >> end 