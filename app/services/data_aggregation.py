from typing import Dict, Any, List
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

def aggregate_trends_data(trends_data: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate trends data by category and date.
    
    Args:
        trends_data: Raw trends data
        
    Returns:
        Aggregated DataFrame with trends by category
    """
    # Group by date and category, sum searches
    aggregated = trends_data.groupby(['date', 'category'])['searches'].sum().reset_index()
    return aggregated

def generate_event_insights(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate insights from event data.
    
    Args:
        events: List of event dictionaries
        
    Returns:
        Dictionary containing various insights
    """
    insights = {
        'total_events': len(events),
        'categories': {},
        'locations': {},
        'price_ranges': {
            'free': 0,
            'low': 0,
            'medium': 0,
            'high': 0
        }
    }
    
    for event in events:
        # Aggregate categories
        for category in event.get('categories', []):
            insights['categories'][category] = insights['categories'].get(category, 0) + 1
            
        # Aggregate locations
        location = event.get('location', {})
        city = location.get('city', 'Unknown')
        insights['locations'][city] = insights['locations'].get(city, 0) + 1
        
        # Aggregate price ranges
        price_info = event.get('price_info', {})
        max_price = price_info.get('max_price', 0)
        
        if max_price == 0:
            insights['price_ranges']['free'] += 1
        elif max_price <= 20:
            insights['price_ranges']['low'] += 1
        elif max_price <= 50:
            insights['price_ranges']['medium'] += 1
        else:
            insights['price_ranges']['high'] += 1
            
    return insights

def generate_trends_insights(trends_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate insights from trends data.
    
    Args:
        trends_data: DataFrame with trends data
        
    Returns:
        Dictionary containing trends insights
    """
    insights = {
        'trending_categories': {},
        'growth_rates': {},
        'seasonal_patterns': {}
    }
    
    # Calculate trending categories
    latest_date = trends_data['date'].max()
    latest_trends = trends_data[trends_data['date'] == latest_date]
    
    for _, row in latest_trends.iterrows():
        insights['trending_categories'][row['category']] = row['searches']
        
    # Calculate growth rates
    for category in trends_data['category'].unique():
        category_data = trends_data[trends_data['category'] == category]
        if len(category_data) >= 2:
            first_value = category_data.iloc[0]['searches']
            last_value = category_data.iloc[-1]['searches']
            growth = (last_value - first_value) / first_value if first_value > 0 else 0
            insights['growth_rates'][category] = growth
            
    # Identify seasonal patterns
    for category in trends_data['category'].unique():
        category_data = trends_data[trends_data['category'] == category]
        insights['seasonal_patterns'][category] = {
            'weekday_avg': category_data['searches'].mean(),
            'weekend_avg': category_data[category_data['date'].dt.dayofweek.isin([5, 6])]['searches'].mean()
        }
        
    return insights

def save_insights_to_bigquery(insights: Dict[str, Any]) -> None:
    """
    Save insights to BigQuery.
    
    Args:
        insights: Dictionary of insights to save
    """
    client = bigquery.Client()
    table_id = "your-project.your-dataset.insights"
    
    # Convert insights to rows
    rows = []
    timestamp = datetime.utcnow().isoformat()
    
    # Flatten insights into rows
    for category, count in insights.get('categories', {}).items():
        rows.append({
            'timestamp': timestamp,
            'insight_type': 'category',
            'key': category,
            'value': count
        })
        
    for location, count in insights.get('locations', {}).items():
        rows.append({
            'timestamp': timestamp,
            'insight_type': 'location',
            'key': location,
            'value': count
        })
        
    for price_range, count in insights.get('price_ranges', {}).items():
        rows.append({
            'timestamp': timestamp,
            'insight_type': 'price_range',
            'key': price_range,
            'value': count
        })
    
    # Insert rows
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise Exception(f"Failed to insert rows: {errors}") 