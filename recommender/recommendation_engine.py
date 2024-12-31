import json
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
from google.cloud import bigquery
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics.pairwise import cosine_similarity

class RecommendationEngine:
    def __init__(self, project_id: str, dataset_id: str):
        """
        Initialize the recommendation engine.
        
        Args:
            project_id: Google Cloud project ID
            dataset_id: BigQuery dataset ID
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client()
    
    def get_latest_insights(self, user_id: str) -> Dict[str, Any]:
        """
        Fetch the latest aggregated insights for a user.
        
        Args:
            user_id: User ID to fetch insights for
            
        Returns:
            Dictionary containing latest insights
        """
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.aggregated_insights`
        WHERE user_id = @user_id
        ORDER BY timestamp DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
            ]
        )
        
        df = self.client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            return {}
        
        # Parse JSON strings back to dictionaries
        row = df.iloc[0]
        return {
            'music_insights': json.loads(row['music_insights']),
            'trend_insights': json.loads(row['trend_insights']),
            'industry_insights': json.loads(row['industry_insights']),
            'timestamp': row['timestamp'].isoformat()
        }
    
    def generate_music_recommendations(
        self,
        user_insights: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Generate music recommendations based on user insights.
        
        Args:
            user_insights: Dictionary containing user insights
            limit: Maximum number of recommendations to return
            
        Returns:
            List of recommended tracks/artists
        """
        music_insights = user_insights.get('music_insights', {})
        trend_insights = user_insights.get('trend_insights', {})
        
        # Get user's preferred genres and artists
        preferred_genres = list(music_insights.get('genre_distribution', {}).keys())[:5]
        top_artists = list(music_insights.get('top_artists', {}).keys())[:5]
        
        # Query similar tracks
        query = f"""
        WITH user_preferences AS (
            SELECT DISTINCT track_id, artist_id, name, artist_name, popularity_score,
                   genres, release_date
            FROM `{self.project_id}.{self.dataset_id}.processed_tracks`
            WHERE genres IN UNNEST(@genres)
               OR artist_name IN UNNEST(@artists)
        )
        SELECT *
        FROM user_preferences
        ORDER BY popularity_score DESC
        LIMIT @limit
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("genres", "STRING", preferred_genres),
                bigquery.ArrayQueryParameter("artists", "STRING", top_artists),
                bigquery.ScalarQueryParameter("limit", "INTEGER", limit)
            ]
        )
        
        recommendations = self.client.query(query, job_config=job_config).to_dataframe()
        
        return recommendations.to_dict('records')
    
    def generate_trend_recommendations(
        self,
        user_insights: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Generate trending topic recommendations based on user insights.
        
        Args:
            user_insights: Dictionary containing user insights
            limit: Maximum number of recommendations to return
            
        Returns:
            List of recommended trending topics
        """
        trend_insights = user_insights.get('trend_insights', {})
        industry_insights = user_insights.get('industry_insights', {})
        
        # Get user's industry and top topics
        user_industry = industry_insights.get('industry', '')
        top_topics = list(trend_insights.get('top_topics', {}).keys())[:5]
        
        # Query related trending topics
        query = f"""
        WITH user_trends AS (
            SELECT topic, interest_value, related_topics,
                   ROW_NUMBER() OVER (PARTITION BY topic ORDER BY date DESC) as rn
            FROM `{self.project_id}.{self.dataset_id}.processed_trends`
            WHERE topic IN UNNEST(@topics)
               OR LOWER(topic) LIKE CONCAT('%', LOWER(@industry), '%')
        )
        SELECT topic, interest_value, related_topics
        FROM user_trends
        WHERE rn = 1
        ORDER BY interest_value DESC
        LIMIT @limit
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("topics", "STRING", top_topics),
                bigquery.ScalarQueryParameter("industry", "STRING", user_industry),
                bigquery.ScalarQueryParameter("limit", "INTEGER", limit)
            ]
        )
        
        recommendations = self.client.query(query, job_config=job_config).to_dataframe()
        
        return recommendations.to_dict('records')
    
    def generate_network_recommendations(
        self,
        user_insights: Dict[str, Any],
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Generate network recommendations based on user insights.
        
        Args:
            user_insights: Dictionary containing user insights
            limit: Maximum number of recommendations to return
            
        Returns:
            List of recommended connections/companies
        """
        industry_insights = user_insights.get('industry_insights', {})
        
        # Get user's industry distribution
        industry_dist = industry_insights.get('industry_distribution', {})
        top_industries = list(industry_dist.keys())[:5]
        
        # Query potential connections
        query = f"""
        WITH potential_connections AS (
            SELECT DISTINCT c.id, c.first_name, c.last_name, c.headline,
                   c.industry, c.company_name, c.position
            FROM `{self.project_id}.{self.dataset_id}.processed_connections` c
            WHERE c.industry IN UNNEST(@industries)
              AND c.id NOT IN (
                SELECT connection_id
                FROM `{self.project_id}.{self.dataset_id}.user_connections`
                WHERE user_id = @user_id
              )
        )
        SELECT *
        FROM potential_connections
        ORDER BY RAND()
        LIMIT @limit
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("industries", "STRING", top_industries),
                bigquery.ScalarQueryParameter("limit", "INTEGER", limit)
            ]
        )
        
        recommendations = self.client.query(query, job_config=job_config).to_dataframe()
        
        return recommendations.to_dict('records')
    
    def get_recommendations(
        self,
        user_id: str,
        category: Optional[str] = None,
        limit: int = 10
    ) -> Dict[str, Union[str, List[Dict[str, Any]]]]:
        """
        Get personalized recommendations for a user.
        
        Args:
            user_id: User ID to get recommendations for
            category: Optional category to filter recommendations
            limit: Maximum number of recommendations per category
            
        Returns:
            Dictionary containing recommendations by category
        """
        # Get latest user insights
        user_insights = self.get_latest_insights(user_id)
        if not user_insights:
            return {
                'error': 'No insights available for user',
                'timestamp': datetime.now().isoformat()
            }
        
        recommendations: Dict[str, Union[str, List[Dict[str, Any]]]] = {
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id
        }
        
        # Generate recommendations based on category
        if category is None or category == 'music':
            recommendations['music'] = self.generate_music_recommendations(
                user_insights,
                limit=limit
            )
        
        if category is None or category == 'trends':
            recommendations['trends'] = self.generate_trend_recommendations(
                user_insights,
                limit=limit
            )
        
        if category is None or category == 'network':
            recommendations['network'] = self.generate_network_recommendations(
                user_insights,
                limit=limit
            )
        
        return recommendations
    
    def record_feedback(
        self,
        user_id: str,
        recommendation_id: str,
        feedback_type: str,
        feedback_text: Optional[str] = None
    ) -> None:
        """
        Record user feedback on recommendations.
        
        Args:
            user_id: User ID providing feedback
            recommendation_id: ID of the recommendation
            feedback_type: Type of feedback (e.g., 'like', 'dislike')
            feedback_text: Optional feedback text
        """
        table_id = f"{self.project_id}.{self.dataset_id}.recommendation_feedback"
        
        row = {
            'user_id': user_id,
            'recommendation_id': recommendation_id,
            'feedback_type': feedback_type,
            'feedback_text': feedback_text,
            'timestamp': datetime.now().isoformat()
        }
        
        errors = self.client.insert_rows_json(table_id, [row])
        if errors:
            raise Exception(f"Error inserting feedback: {errors}")
    
    def update_user_preferences(self, user_id: str) -> None:
        """
        Update user preferences based on feedback history.
        
        Args:
            user_id: User ID to update preferences for
        """
        # Query user's feedback history
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.recommendation_feedback`
        WHERE user_id = @user_id
        ORDER BY timestamp DESC
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
            ]
        )
        
        feedback_df = self.client.query(query, job_config=job_config).to_dataframe()
        
        if feedback_df.empty:
            return
        
        # Calculate preference scores
        preference_scores = feedback_df.groupby('recommendation_id').agg({
            'feedback_type': lambda x: sum(1 if f == 'like' else -1 for f in x)
        }).reset_index()
        
        # Update user preferences in BigQuery
        table_id = f"{self.project_id}.{self.dataset_id}.user_preferences"
        
        rows = [{
            'user_id': user_id,
            'recommendation_id': row['recommendation_id'],
            'preference_score': row['feedback_type'],
            'updated_at': datetime.now().isoformat()
        } for _, row in preference_scores.iterrows()]
        
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            raise Exception(f"Error updating preferences: {errors}") 