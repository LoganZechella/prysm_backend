import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any
from google.cloud import storage
from google.cloud import bigquery

def transform_spotify_data(tracks_data: List[Dict], artists_data: List[Dict]) -> pd.DataFrame:
    """
    Transform Spotify data into a format suitable for analysis.
    
    Args:
        tracks_data: List of processed track dictionaries
        artists_data: List of processed artist dictionaries
        
    Returns:
        DataFrame with transformed data
    """
    # Convert to DataFrames
    tracks_df = pd.DataFrame(tracks_data)
    artists_df = pd.DataFrame(artists_data)
    
    # Merge tracks and artists data
    merged_df = tracks_df.merge(
        artists_df,
        left_on='artist_id',
        right_on='id',
        suffixes=('_track', '_artist')
    )
    
    # Calculate additional features
    merged_df['popularity_score'] = (
        merged_df['popularity_track'] * 0.6 + 
        merged_df['popularity_artist'] * 0.4
    )
    
    # Add timestamp
    merged_df['processed_at'] = datetime.now().isoformat()
    
    return merged_df

def transform_trends_data(
    topics_data: List[Dict],
    interest_data: Dict[str, Dict],
    related_data: Dict[str, List]
) -> pd.DataFrame:
    """
    Transform Google Trends data into a format suitable for analysis.
    
    Args:
        topics_data: List of processed topic dictionaries
        interest_data: Dictionary of processed interest data
        related_data: Dictionary of processed related topics
        
    Returns:
        DataFrame with transformed data
    """
    # Convert topics to DataFrame
    topics_df = pd.DataFrame(topics_data)
    
    # Convert interest data to long format
    interest_records = []
    for topic, data in interest_data.items():
        for date, value in data.items():
            interest_records.append({
                'topic': topic,
                'date': date,
                'interest_value': value
            })
    interest_df = pd.DataFrame(interest_records)
    
    # Convert related topics to long format
    related_records = []
    for topic, related in related_data.items():
        for related_topic in related:
            related_records.append({
                'main_topic': topic,
                'related_topic': related_topic
            })
    related_df = pd.DataFrame(related_records)
    
    # Merge all data
    merged_df = topics_df.merge(
        interest_df,
        on='topic'
    ).merge(
        related_df,
        left_on='topic',
        right_on='main_topic',
        how='left'
    )
    
    # Add timestamp
    merged_df['processed_at'] = datetime.now().isoformat()
    
    return merged_df

def transform_linkedin_data(
    profile_data: Dict,
    connections_data: List[Dict],
    activities_data: List[Dict]
) -> pd.DataFrame:
    """
    Transform LinkedIn data into a format suitable for analysis.
    
    Args:
        profile_data: Processed profile dictionary
        connections_data: List of processed connection dictionaries
        activities_data: List of processed activity dictionaries
        
    Returns:
        DataFrame with transformed data
    """
    # Convert to DataFrames
    connections_df = pd.DataFrame(connections_data)
    activities_df = pd.DataFrame(activities_data)
    
    # Calculate network metrics
    industry_counts = connections_df['industry'].value_counts().to_dict()
    activity_counts = activities_df['type'].value_counts().to_dict()
    
    # Create profile metrics
    profile_metrics = {
        'user_id': profile_data['id'],
        'industry': profile_data['industry'],
        'total_connections': len(connections_data),
        'industry_distribution': industry_counts,
        'activity_distribution': activity_counts,
        'processed_at': datetime.now().isoformat()
    }
    
    return pd.DataFrame([profile_metrics])

def aggregate_data(
    spotify_df: pd.DataFrame,
    trends_df: pd.DataFrame,
    linkedin_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Aggregate transformed data from all sources.
    
    Args:
        spotify_df: Transformed Spotify data
        trends_df: Transformed Google Trends data
        linkedin_df: Transformed LinkedIn data
        
    Returns:
        Dictionary containing aggregated insights
    """
    aggregated_data = {
        'music_insights': {
            'top_artists': spotify_df.groupby('artist_name')['popularity_score'].mean().nlargest(10).to_dict(),
            'genre_distribution': spotify_df['genres'].explode().value_counts().to_dict(),
            'avg_track_popularity': spotify_df['popularity_track'].mean()
        },
        'trend_insights': {
            'top_topics': trends_df.groupby('topic')['interest_value'].mean().nlargest(10).to_dict(),
            'trending_topics': trends_df[trends_df['date'] == trends_df['date'].max()]['topic'].tolist(),
            'related_topics_network': trends_df.groupby('topic')['related_topic'].apply(list).to_dict()
        },
        'industry_insights': {
            'industry_distribution': linkedin_df['industry_distribution'].iloc[0],
            'activity_patterns': linkedin_df['activity_distribution'].iloc[0]
        },
        'timestamp': datetime.now().isoformat()
    }
    
    return aggregated_data

def save_to_bigquery(
    data: Dict[str, Any],
    project_id: str,
    dataset_id: str,
    table_id: str
) -> None:
    """
    Save aggregated data to BigQuery.
    
    Args:
        data: Dictionary containing aggregated data
        project_id: Google Cloud project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
    """
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Convert nested dictionaries to strings
    flattened_data = {
        k: json.dumps(v) if isinstance(v, (dict, list)) else v
        for k, v in data.items()
    }
    
    # Create table if not exists
    schema = [
        bigquery.SchemaField("music_insights", "STRING"),
        bigquery.SchemaField("trend_insights", "STRING"),
        bigquery.SchemaField("industry_insights", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP")
    ]
    
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    
    # Insert data
    rows_to_insert = [flattened_data]
    errors = client.insert_rows_json(table, rows_to_insert)
    
    if errors:
        raise Exception(f"Error inserting rows into BigQuery: {errors}") 