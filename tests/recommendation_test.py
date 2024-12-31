import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime
from recommender.recommendation_engine import RecommendationEngine

@pytest.fixture
def mock_bigquery_client():
    with patch('google.cloud.bigquery.Client') as mock_client:
        yield mock_client

@pytest.fixture
def sample_insights():
    return {
        'music_insights': {
            'top_artists': {
                'Artist 1': 0.8,
                'Artist 2': 0.7
            },
            'genre_distribution': {
                'pop': 10,
                'rock': 8
            },
            'avg_track_popularity': 75.5
        },
        'trend_insights': {
            'top_topics': {
                'Topic 1': 100,
                'Topic 2': 80
            },
            'trending_topics': ['Topic 1', 'Topic 2'],
            'related_topics_network': {
                'Topic 1': ['Related 1', 'Related 2']
            }
        },
        'industry_insights': {
            'industry_distribution': {
                'Technology': 15,
                'Music': 10
            },
            'activity_patterns': {
                'POST': 5,
                'SHARE': 3
            }
        },
        'timestamp': datetime.now().isoformat()
    }

@pytest.fixture
def recommendation_engine(mock_bigquery_client):
    return RecommendationEngine('test-project', 'test-dataset')

def test_get_latest_insights(recommendation_engine, mock_bigquery_client, sample_insights):
    # Mock BigQuery response
    mock_query = Mock()
    mock_query.to_dataframe.return_value = pd.DataFrame([{
        'music_insights': str(sample_insights['music_insights']),
        'trend_insights': str(sample_insights['trend_insights']),
        'industry_insights': str(sample_insights['industry_insights']),
        'timestamp': sample_insights['timestamp']
    }])
    mock_bigquery_client.return_value.query.return_value = mock_query
    
    insights = recommendation_engine.get_latest_insights('test-user')
    
    assert insights is not None
    assert 'music_insights' in insights
    assert 'trend_insights' in insights
    assert 'industry_insights' in insights
    assert 'timestamp' in insights

def test_generate_music_recommendations(recommendation_engine, mock_bigquery_client, sample_insights):
    # Mock BigQuery response
    mock_query = Mock()
    mock_query.to_dataframe.return_value = pd.DataFrame([{
        'track_id': 'track1',
        'artist_id': 'artist1',
        'name': 'Test Track',
        'artist_name': 'Test Artist',
        'popularity_score': 80,
        'genres': ['pop', 'rock'],
        'release_date': '2024-01-01'
    }])
    mock_bigquery_client.return_value.query.return_value = mock_query
    
    recommendations = recommendation_engine.generate_music_recommendations(sample_insights)
    
    assert len(recommendations) > 0
    assert all(isinstance(rec, dict) for rec in recommendations)
    assert all('track_id' in rec for rec in recommendations)

def test_generate_trend_recommendations(recommendation_engine, mock_bigquery_client, sample_insights):
    # Mock BigQuery response
    mock_query = Mock()
    mock_query.to_dataframe.return_value = pd.DataFrame([{
        'topic': 'Test Topic',
        'interest_value': 100,
        'related_topics': ['Related 1', 'Related 2']
    }])
    mock_bigquery_client.return_value.query.return_value = mock_query
    
    recommendations = recommendation_engine.generate_trend_recommendations(sample_insights)
    
    assert len(recommendations) > 0
    assert all(isinstance(rec, dict) for rec in recommendations)
    assert all('topic' in rec for rec in recommendations)

def test_generate_network_recommendations(recommendation_engine, mock_bigquery_client, sample_insights):
    # Mock BigQuery response
    mock_query = Mock()
    mock_query.to_dataframe.return_value = pd.DataFrame([{
        'id': 'user1',
        'first_name': 'Test',
        'last_name': 'User',
        'headline': 'Test Headline',
        'industry': 'Technology',
        'company_name': 'Test Company',
        'position': 'Test Position'
    }])
    mock_bigquery_client.return_value.query.return_value = mock_query
    
    recommendations = recommendation_engine.generate_network_recommendations(sample_insights)
    
    assert len(recommendations) > 0
    assert all(isinstance(rec, dict) for rec in recommendations)
    assert all('id' in rec for rec in recommendations)

def test_get_recommendations(recommendation_engine, mock_bigquery_client, sample_insights):
    # Mock get_latest_insights
    with patch.object(recommendation_engine, 'get_latest_insights') as mock_insights:
        mock_insights.return_value = sample_insights
        
        # Mock recommendation generators
        with patch.object(recommendation_engine, 'generate_music_recommendations') as mock_music:
            with patch.object(recommendation_engine, 'generate_trend_recommendations') as mock_trends:
                with patch.object(recommendation_engine, 'generate_network_recommendations') as mock_network:
                    mock_music.return_value = [{'track_id': 'track1'}]
                    mock_trends.return_value = [{'topic': 'topic1'}]
                    mock_network.return_value = [{'id': 'user1'}]
                    
                    recommendations = recommendation_engine.get_recommendations('test-user')
                    
                    assert 'music' in recommendations
                    assert 'trends' in recommendations
                    assert 'network' in recommendations
                    assert len(recommendations['music']) > 0
                    assert len(recommendations['trends']) > 0
                    assert len(recommendations['network']) > 0

def test_record_feedback(recommendation_engine, mock_bigquery_client):
    # Mock insert_rows_json
    mock_bigquery_client.return_value.insert_rows_json.return_value = []
    
    recommendation_engine.record_feedback(
        'test-user',
        'rec1',
        'like',
        'Great recommendation!'
    )
    
    assert mock_bigquery_client.return_value.insert_rows_json.called

def test_update_user_preferences(recommendation_engine, mock_bigquery_client):
    # Mock feedback query
    mock_query = Mock()
    mock_query.to_dataframe.return_value = pd.DataFrame([{
        'user_id': 'test-user',
        'recommendation_id': 'rec1',
        'feedback_type': 'like',
        'timestamp': datetime.now().isoformat()
    }])
    mock_bigquery_client.return_value.query.return_value = mock_query
    
    # Mock insert_rows_json
    mock_bigquery_client.return_value.insert_rows_json.return_value = []
    
    recommendation_engine.update_user_preferences('test-user')
    
    assert mock_bigquery_client.return_value.query.called
    assert mock_bigquery_client.return_value.insert_rows_json.called 