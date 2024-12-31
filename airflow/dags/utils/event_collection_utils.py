import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import requests
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class EventbriteClient:
    """Client for interacting with Eventbrite API."""
    
    BASE_URL = "https://www.eventbriteapi.com/v3"
    
    def __init__(self):
        self.api_key = os.getenv('EVENTBRITE_API_KEY')
        if not self.api_key:
            raise ValueError("Eventbrite API key not found in environment variables")
        
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def search_events(
        self,
        location: Optional[str] = None,
        categories: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1
    ) -> Dict[str, Any]:
        """
        Search for events using various filters.
        
        Args:
            location: Location to search in (e.g., "San Francisco, CA")
            categories: List of category IDs
            start_date: Start date in ISO format
            end_date: End date in ISO format
            page: Page number for pagination
            
        Returns:
            Dictionary containing search results
        """
        endpoint = f"{self.BASE_URL}/events/search"
        
        params = {
            'page': page,
            'expand': 'venue,ticket_availability,category,organizer'
        }
        
        if location:
            params['location.address'] = location
        if categories:
            params['categories'] = ','.join(categories)
        if start_date:
            params['start_date.range_start'] = start_date
        if end_date:
            params['start_date.range_end'] = end_date
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def get_event_details(self, event_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific event.
        
        Args:
            event_id: Eventbrite event ID
            
        Returns:
            Dictionary containing event details
        """
        endpoint = f"{self.BASE_URL}/events/{event_id}"
        params = {
            'expand': 'venue,ticket_availability,category,organizer,attendees'
        }
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def get_venue_details(self, venue_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a venue.
        
        Args:
            venue_id: Eventbrite venue ID
            
        Returns:
            Dictionary containing venue details
        """
        endpoint = f"{self.BASE_URL}/venues/{venue_id}"
        
        response = requests.get(endpoint, headers=self.headers)
        response.raise_for_status()
        
        return response.json()

def transform_eventbrite_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform Eventbrite event data into our standard schema.
    
    Args:
        event_data: Raw event data from Eventbrite
        
    Returns:
        Dictionary containing transformed event data
    """
    venue = event_data.get('venue', {})
    ticket_info = event_data.get('ticket_availability', {})
    
    # Extract price information
    min_price = None
    max_price = None
    if 'ticket_classes' in event_data:
        prices = [
            ticket['cost']['major_value']
            for ticket in event_data['ticket_classes']
            if ticket.get('cost')
        ]
        if prices:
            min_price = min(prices)
            max_price = max(prices)
    
    # Transform to our schema
    transformed_event = {
        'event_id': event_data['id'],
        'title': event_data['name']['text'],
        'description': event_data['description']['text'],
        'short_description': event_data['description']['text'][:200] + '...',
        'start_datetime': event_data['start']['utc'],
        'end_datetime': event_data['end']['utc'],
        'location': {
            'venue_name': venue.get('name'),
            'address': venue.get('address', {}).get('localized_address_display'),
            'city': venue.get('address', {}).get('city'),
            'state': venue.get('address', {}).get('region'),
            'country': venue.get('address', {}).get('country'),
            'coordinates': {
                'lat': venue.get('latitude'),
                'lng': venue.get('longitude')
            }
        },
        'categories': [
            cat['name'] for cat in event_data.get('categories', [])
        ],
        'tags': [
            tag['name'] for tag in event_data.get('tags', [])
        ],
        'price_info': {
            'currency': ticket_info.get('currency', 'USD'),
            'min_price': min_price,
            'max_price': max_price,
            'price_tier': calculate_price_tier(min_price, max_price)
        },
        'attributes': {
            'indoor_outdoor': extract_indoor_outdoor(event_data),
            'age_restriction': extract_age_restriction(event_data),
            'accessibility_features': extract_accessibility_features(event_data),
            'dress_code': extract_dress_code(event_data)
        },
        'images': [
            img['url'] for img in event_data.get('images', [])
        ],
        'source': {
            'platform': 'eventbrite',
            'url': event_data['url'],
            'last_updated': datetime.utcnow().isoformat()
        },
        'metadata': {
            'popularity_score': calculate_popularity_score(event_data),
            'social_mentions': None,  # To be implemented
            'verified': event_data['organizer'].get('verified', False)
        }
    }
    
    return transformed_event

def calculate_price_tier(min_price: Optional[float], max_price: Optional[float]) -> str:
    """Calculate price tier based on price range."""
    if not min_price or not max_price:
        return 'free'
    
    avg_price = (min_price + max_price) / 2
    if avg_price < 25:
        return 'budget'
    elif avg_price < 75:
        return 'medium'
    else:
        return 'premium'

def calculate_popularity_score(event_data: Dict[str, Any]) -> float:
    """Calculate popularity score based on various factors."""
    score = 0.0
    
    # Factor in capacity and ticket sales
    capacity = event_data.get('capacity', 0)
    tickets_sold = event_data.get('tickets_sold', 0)
    if capacity > 0:
        score += (tickets_sold / capacity) * 0.4
    
    # Factor in social engagement
    likes = event_data.get('likes', 0)
    shares = event_data.get('shares', 0)
    score += min((likes + shares) / 1000, 0.3)
    
    # Factor in organizer reputation
    if event_data.get('organizer', {}).get('verified'):
        score += 0.2
    
    # Factor in event age
    created = datetime.fromisoformat(event_data['created'].replace('Z', '+00:00'))
    current_time = datetime.now(created.tzinfo)  # Use the same timezone as created
    age_hours = (current_time - created).total_seconds() / 3600
    if age_hours < 48:  # Boost new events
        score += 0.1
    
    return min(score, 1.0)

def extract_indoor_outdoor(event_data: Dict[str, Any]) -> str:
    """Extract indoor/outdoor status from event data."""
    description = event_data['description']['text'].lower()
    
    if 'outdoor' in description and 'indoor' in description:
        return 'both'
    elif 'outdoor' in description:
        return 'outdoor'
    elif 'indoor' in description:
        return 'indoor'
    else:
        return 'indoor'  # Default to indoor

def extract_age_restriction(event_data: Dict[str, Any]) -> str:
    """Extract age restriction from event data."""
    description = event_data['description']['text'].lower()
    
    if '21+' in description or '21 +' in description:
        return '21+'
    elif '18+' in description or '18 +' in description:
        return '18+'
    else:
        return 'all'

def extract_accessibility_features(event_data: Dict[str, Any]) -> List[str]:
    """Extract accessibility features from event data."""
    description = event_data['description']['text'].lower()
    features = []
    
    if 'wheelchair' in description:
        features.append('wheelchair')
    if 'hearing' in description or 'audio assist' in description:
        features.append('hearing_assistance')
    if 'sign language' in description:
        features.append('sign_language')
    
    return features

def extract_dress_code(event_data: Dict[str, Any]) -> str:
    """Extract dress code from event data."""
    description = event_data['description']['text'].lower()
    
    if 'formal' in description or 'black tie' in description:
        return 'formal'
    elif 'business' in description or 'smart casual' in description:
        return 'business'
    else:
        return 'casual'

def upload_to_gcs(data: Dict[str, Any], bucket_name: str, blob_path: str) -> str:
    """Upload data to Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type='application/json'
    )
    
    return f"gs://{bucket_name}/{blob_path}" 