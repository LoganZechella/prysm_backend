import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from google.cloud import storage
from google.cloud import bigquery

def aggregate_event_data(
    processed_bucket: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> pd.DataFrame:
    """
    Aggregate processed event data from all sources.
    
    Args:
        processed_bucket: Name of the GCS bucket containing processed data
        start_date: Optional start date for filtering (ISO format)
        end_date: Optional end date for filtering (ISO format)
        
    Returns:
        DataFrame containing aggregated event data
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(processed_bucket)
    
    # Helper function to filter data by date range
    def is_in_date_range(timestamp: str) -> bool:
        if not (start_date or end_date):
            return True
        dt = datetime.fromisoformat(timestamp)
        if start_date and dt < datetime.fromisoformat(start_date):
            return False
        if end_date and dt > datetime.fromisoformat(end_date):
            return False
        return True
    
    # Collect all event data
    events = []
    
    # Process Eventbrite data
    for blob in bucket.list_blobs(prefix='eventbrite/processed'):
        data = json.loads(blob.download_as_string())
        for event in data['events']:
            if is_in_date_range(event['start_datetime']):
                event['source'] = 'eventbrite'
                events.append(event)
    
    # Convert to DataFrame
    events_df = pd.DataFrame(events)
    
    # Add derived features
    if not events_df.empty:
        events_df['datetime'] = pd.to_datetime(events_df['start_datetime'])
        events_df['day_of_week'] = events_df['datetime'].dt.day_name()
        events_df['hour_of_day'] = events_df['datetime'].dt.hour
        events_df['is_weekend'] = events_df['datetime'].dt.dayofweek.isin([5, 6])
    
    return events_df

def aggregate_trends_data(
    processed_bucket: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Aggregate processed Google Trends data.
    
    Args:
        processed_bucket: Name of the GCS bucket containing processed data
        start_date: Optional start date for filtering (ISO format)
        end_date: Optional end date for filtering (ISO format)
        
    Returns:
        Dictionary containing DataFrames for topics and interest data
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(processed_bucket)
    
    topics = []
    interest_records = []
    
    # Process Google Trends data
    for blob in bucket.list_blobs(prefix='trends/processed'):
        data = json.loads(blob.download_as_string())
        
        if 'topics' in blob.name:
            topics.extend(data)
        elif 'interest' in blob.name:
            for topic, values in data['data'].items():
                for date, value in values.items():
                    interest_records.append({
                        'topic': topic,
                        'date': date,
                        'interest_value': value
                    })
    
    # Convert to DataFrames
    topics_df = pd.DataFrame(topics)
    interest_df = pd.DataFrame(interest_records)
    
    # Filter by date range if specified
    if start_date or end_date:
        if not interest_df.empty:
            interest_df['date'] = pd.to_datetime(interest_df['date'])
            if start_date:
                interest_df = interest_df[
                    interest_df['date'] >= pd.to_datetime(start_date)
                ]
            if end_date:
                interest_df = interest_df[
                    interest_df['date'] <= pd.to_datetime(end_date)
                ]
    
    return {
        'topics': topics_df,
        'interest': interest_df
    }

def generate_event_insights(events_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate insights from aggregated event data.
    
    Args:
        events_df: DataFrame containing event data
        
    Returns:
        Dictionary containing various insights
    """
    if events_df.empty:
        return {
            'total_events': 0,
            'insights': {}
        }
    
    insights = {
        'total_events': len(events_df),
        'events_by_day': events_df['day_of_week'].value_counts().to_dict(),
        'events_by_hour': events_df['hour_of_day'].value_counts().to_dict(),
        'weekend_vs_weekday': {
            'weekend': events_df['is_weekend'].sum(),
            'weekday': (~events_df['is_weekend']).sum()
        },
        'price_distribution': {
            'free': len(events_df[events_df['price_info.min_price'] == 0]),
            'paid': len(events_df[events_df['price_info.min_price'] > 0])
        },
        'popular_categories': events_df['categories'].explode().value_counts().head(10).to_dict(),
        'popular_venues': events_df['location.venue_name'].value_counts().head(10).to_dict()
    }
    
    return insights

def generate_trends_insights(trends_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Generate insights from aggregated Google Trends data.
    
    Args:
        trends_data: Dictionary containing topics and interest DataFrames
        
    Returns:
        Dictionary containing various insights
    """
    topics_df = trends_data['topics']
    interest_df = trends_data['interest']
    
    if topics_df.empty or interest_df.empty:
        return {
            'total_topics': 0,
            'insights': {}
        }
    
    # Calculate average interest by topic
    avg_interest = interest_df.groupby('topic')['interest_value'].mean()
    
    insights = {
        'total_topics': len(topics_df),
        'top_topics_by_interest': avg_interest.nlargest(10).to_dict(),
        'interest_distribution': {
            'high': len(avg_interest[avg_interest >= 75]),
            'medium': len(avg_interest[(avg_interest >= 25) & (avg_interest < 75)]),
            'low': len(avg_interest[avg_interest < 25])
        }
    }
    
    return insights

def save_insights_to_bigquery(
    insights: Dict[str, Any],
    project_id: str,
    dataset_id: str,
    table_id: str
) -> None:
    """
    Save generated insights to BigQuery.
    
    Args:
        insights: Dictionary containing insights
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
    """
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Add timestamp to insights
    insights['timestamp'] = datetime.utcnow().isoformat()
    
    # Convert insights to newline-delimited JSON
    rows = [insights]
    
    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config
    )
    job.result()  # Wait for the job to complete 