import pytest
import pandas as pd
from datetime import datetime, timedelta
from app.services.data_aggregation import (
    aggregate_trends_data,
    generate_event_insights,
    generate_trends_insights,
    save_insights_to_bigquery
)
from app.schemas.event import Event
from app.schemas.category import CategoryHierarchy

def test_aggregate_trends_data(sample_trends_data):
    """Test aggregating trends data."""
    aggregated = aggregate_trends_data(sample_trends_data)
    assert isinstance(aggregated, pd.DataFrame)
    assert "date" in aggregated.columns
    assert "category" in aggregated.columns
    assert "searches" in aggregated.columns

def test_generate_event_insights(sample_event_data):
    """Test generating event insights."""
    insights = generate_event_insights(sample_event_data)
    assert "total_events" in insights
    assert "categories" in insights
    assert "locations" in insights
    assert "price_ranges" in insights

def test_generate_trends_insights(sample_trends_data):
    """Test generating trends insights."""
    insights = generate_trends_insights(sample_trends_data)
    assert "trending_categories" in insights
    assert "growth_rates" in insights
    assert "seasonal_patterns" in insights

@pytest.mark.skip(reason="Requires BigQuery credentials")
def test_save_insights_to_bigquery():
    """Test saving insights to BigQuery."""
    insights = {
        "total_events": 100,
        "categories": {"music": 50, "sports": 30, "arts": 20},
        "locations": {"New York": 40, "Los Angeles": 30, "Chicago": 30}
    }
    
    # This should be mocked in a real test
    save_insights_to_bigquery(insights) 