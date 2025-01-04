import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import logging
import asyncio
import re
from dateutil.parser import parse as parse_date
import pytz
from .base import BaseScraper

logger = logging.getLogger(__name__)

class FacebookEventsScraper(BaseScraper):
    """Scraper for Facebook events using Meta Graph API"""
    
    def __init__(self, access_token: Optional[str] = None):
        """Initialize the scraper with Meta access token"""
        super().__init__(platform="facebook")
        self.access_token = access_token or os.getenv("META_ACCESS_TOKEN")
        if not self.access_token:
            raise ValueError("Meta access token is required")
        self.base_url = "https://graph.facebook.com/v18.0"
        self.headers.update({
            'Accept': 'application/json'
        })
        
        # Common patterns for event detection
        self._date_patterns = [
            r'(?i)(?:on\s+)?(?:date:\s*)?(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{4})',
            r'(?i)(?:on\s+)?(?:date:\s*)?(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?\s*,?\s*\d{4}',
            r'(?i)(?:on\s+)?(?:date:\s*)?\d{1,2}/\d{1,2}/\d{4}',
            r'(?i)(?:on\s+)?(?:date:\s*)?\d{4}-\d{1,2}-\d{1,2}'
        ]
        
        self._time_patterns = [
            r'(?i)(?:at|from|time:)\s*(\d{1,2}(?::\d{2})?\s*(?:am|pm))',
            r'(?i)(?:at|from|time:)\s*(\d{1,2}(?::\d{2})?(?:\s*hrs)?)',
            r'(?i)(\d{1,2}(?::\d{2})?\s*(?:am|pm)\s*(?:-|to|until)\s*\d{1,2}(?::\d{2})?\s*(?:am|pm))',
        ]
        
        self._price_patterns = [
            r'(?i)(?:price[s]?|tickets?|admission|entry)[\s:]*(?:is\s*)?[$€£]\s*(\d+(?:\.\d{2})?)',
            r'(?i)(?:price[s]?|tickets?|admission|entry)[\s:]*.{0,20}?(\d+(?:\.\d{2})?)\s*(?:dollars|euro|pound|usd|eur|gbp)',
            r'(?i)(?:free entry|free admission|free event)',
        ]
        
        self._venue_patterns = [
            r'(?i)(?:venue|location|place|at)[\s:]*([^.!?\n]+(?:hall|theater|theatre|arena|stadium|center|centre|club|bar|restaurant|cafe|gallery|museum)[^.!?\n]*)',
            r'(?i)(?:venue|location|place|at)[\s:]*([^.!?\n]+)',
        ]
        
        self._online_patterns = [
            r'(?i)(?:zoom|online|virtual|livestream|live stream|webinar|online event)',
            r'(?i)(?:streaming|web conference|digital event|remote event)',
        ]
        
        # Event keywords with confidence scores
        self._event_keywords = {
            'high_confidence': [
                'event', 'concert', 'festival', 'show', 'performance', 'exhibition',
                'conference', 'workshop', 'seminar', 'meetup', 'gathering',
                'celebration', 'party', 'gala', 'ceremony'
            ],
            'medium_confidence': [
                'join us', 'dont miss', "don't miss", 'save the date', 'mark your calendar',
                'rsvp', 'register now', 'get tickets', 'book now'
            ],
            'low_confidence': [
                'happening', 'presenting', 'featuring', 'special', 'exclusive',
                'limited seats', 'limited space', 'book early'
            ]
        }
        
    def _is_event_post(self, post: Dict[str, Any]) -> bool:
        """Check if a post is about an event with confidence scoring"""
        message = post.get('message', '').lower()
        confidence_score = 0
        
        # Check attachments for event links (highest confidence)
        attachments = post.get('attachments', {}).get('data', [])
        for attachment in attachments:
            if attachment.get('type') == 'event':
                return True
            
            # Check if URL contains event indicators
            url = attachment.get('url', '').lower()
            if 'events' in url or 'event' in url:
                confidence_score += 3
        
        # Check for date and time patterns
        if any(bool(re.search(pattern, message)) for pattern in self._date_patterns):
            confidence_score += 2
        if any(bool(re.search(pattern, message)) for pattern in self._time_patterns):
            confidence_score += 2
        
        # Check for venue patterns
        if any(bool(re.search(pattern, message)) for pattern in self._venue_patterns):
            confidence_score += 1
        
        # Check for price patterns
        if any(bool(re.search(pattern, message)) for pattern in self._price_patterns):
            confidence_score += 1
        
        # Check for event keywords with different confidence levels
        for keyword in self._event_keywords['high_confidence']:
            if keyword in message:
                confidence_score += 2
        
        for keyword in self._event_keywords['medium_confidence']:
            if keyword in message:
                confidence_score += 1
        
        for keyword in self._event_keywords['low_confidence']:
            if keyword in message:
                confidence_score += 0.5
        
        # Return True if confidence score meets threshold
        return confidence_score >= 3
    
    def _extract_datetime(self, text: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Extract start and end datetime from text"""
        try:
            # First try to find a date
            date_match = None
            for pattern in self._date_patterns:
                match = re.search(pattern, text)
                if match:
                    date_match = match.group(0)
                    break
            
            if not date_match:
                return None, None
            
            # Try to find a time
            time_match = None
            for pattern in self._time_patterns:
                match = re.search(pattern, text)
                if match:
                    time_match = match.group(0)
                    break
            
            # Parse the date
            date_str = date_match.strip()
            start_time = parse_date(date_str)
            
            # If time was found, update the datetime
            if time_match:
                time_str = time_match.strip()
                if '-' in time_str or 'to' in time_str or 'until' in time_str:
                    # Handle time range
                    start_time_str, end_time_str = re.split(r'\s*(?:-|to|until)\s*', time_str)
                    start_time = parse_date(f"{date_str} {start_time_str}")
                    end_time = parse_date(f"{date_str} {end_time_str}")
                else:
                    # Single time
                    start_time = parse_date(f"{date_str} {time_str}")
                    end_time = start_time + timedelta(hours=2)  # Default duration
            else:
                end_time = start_time + timedelta(hours=2)  # Default duration
            
            # Ensure timezone is set
            if start_time.tzinfo is None:
                start_time = pytz.UTC.localize(start_time)
            if end_time.tzinfo is None:
                end_time = pytz.UTC.localize(end_time)
            
            return start_time, end_time
            
        except Exception as e:
            logger.debug(f"Error extracting datetime: {str(e)}")
            return None, None
    
    def _extract_price(self, text: str) -> Dict[str, Any]:
        """Extract price information from text"""
        price_info = {
            "currency": "USD",
            "min_price": 0.0,
            "max_price": 0.0,
            "price_tier": "unknown"
        }
        
        try:
            # Check for free event
            if bool(re.search(r'(?i)free', text)):
                price_info["price_tier"] = "free"
                return price_info
            
            # Extract price values
            prices = []
            for pattern in self._price_patterns:
                matches = re.finditer(pattern, text)
                for match in matches:
                    try:
                        price_str = match.group(1)
                        price = float(re.sub(r'[^\d.]', '', price_str))
                        prices.append(price)
                    except (IndexError, ValueError):
                        continue
            
            if prices:
                price_info["min_price"] = min(prices)
                price_info["max_price"] = max(prices)
                price_info["price_tier"] = "paid"
        
        except Exception as e:
            logger.debug(f"Error extracting price: {str(e)}")
        
        return price_info
    
    def _extract_venue(self, text: str) -> Dict[str, Optional[str]]:
        """Extract venue information from text"""
        venue_info: Dict[str, Optional[str]] = {
            "name": None,
            "address": None
        }
        
        try:
            for pattern in self._venue_patterns:
                match = re.search(pattern, text)
                if match:
                    venue = match.group(1).strip()
                    venue_info["name"] = venue
                    break
        
        except Exception as e:
            logger.debug(f"Error extracting venue: {str(e)}")
        
        return venue_info
    
    def _is_online_event(self, text: str) -> bool:
        """Determine if event is online"""
        return any(bool(re.search(pattern, text, re.IGNORECASE)) for pattern in self._online_patterns)
    
    async def _extract_event_from_post(self, post: Dict[str, Any], page: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract event information from a post"""
        try:
            message = post.get('message', '')
            attachments = post.get('attachments', {}).get('data', [])
            
            # Initialize event data
            event_data = {
                'id': post.get('id'),
                'name': None,
                'description': message,
                'start_time': None,
                'end_time': None,
                'place': {},
                'is_online': self._is_online_event(message)
            }
            
            # Extract information from attachments
            for attachment in attachments:
                if attachment.get('type') == 'event':
                    event_data.update({
                        'name': attachment.get('title'),
                        'description': attachment.get('description') or event_data['description']
                    })
                    break
            
            # If no explicit event attachment, use post title/message
            if not event_data['name']:
                # Try to extract title from first line of message
                message_lines = event_data['description'].split('\n')
                event_data['name'] = message_lines[0] if message_lines else 'Untitled Event'
            
            # Extract datetime
            start_time, end_time = self._extract_datetime(message)
            if start_time:
                event_data['start_time'] = start_time.isoformat()
            if end_time:
                event_data['end_time'] = end_time.isoformat()
            
            # Extract venue information
            venue_info = self._extract_venue(message)
            if venue_info['name']:
                event_data['place'] = {
                    'name': venue_info['name'],
                    'location': {}
                }
            else:
                event_data['place'] = {
                    'name': page.get('name'),
                    'location': {}
                }
            
            # Extract price information
            event_data['price_info'] = self._extract_price(message)
            
            return self.standardize_event(event_data)
            
        except Exception as e:
            logger.error(f"Error extracting event from post: {str(e)}")
            return None
    
    def _matches_date_range(self, event: Dict[str, Any], date_range: Optional[Dict[str, datetime]]) -> bool:
        """Check if event falls within the specified date range"""
        if not date_range or not event.get('start_datetime'):
            return True
            
        try:
            start_time = datetime.fromisoformat(event['start_datetime'].replace('Z', '+00:00'))
            return date_range['start'] <= start_time <= date_range['end']
        except (ValueError, TypeError):
            return True
    
    async def scrape_events(self, location: Dict[str, Any], date_range: Optional[Dict[str, datetime]] = None) -> List[Dict[str, Any]]:
        """Scrape events for a given location and date range"""
        events = []
        
        # First, search for pages in the given location
        search_params = {
            'access_token': self.access_token,
            'type': 'page',
            'q': f"events {location.get('city')}, {location.get('state')}",
            'fields': 'id,name,feed{type,message,created_time,attachments{type,url,title,description}}'
        }
        
        try:
            if not self.session:
                raise RuntimeError("Session not initialized. Use 'async with' context manager.")
            
            # Search for pages
            async with self.session.get(f"{self.base_url}/search", params=search_params) as response:
                if response.status == 200:
                    data = await response.json()
                    pages = data.get('data', [])
                    
                    # Process each page's feed
                    for page in pages:
                        if 'feed' in page:
                            feed_data = page['feed'].get('data', [])
                            for post in feed_data:
                                # Check if post is about an event
                                if self._is_event_post(post):
                                    event = await self._extract_event_from_post(post, page)
                                    if event and self._matches_date_range(event, date_range):
                                        events.append(event)
                else:
                    logger.error(f"Failed to fetch pages: {response.status}")
                    logger.error(f"Response: {await response.text()}")
        
        except Exception as e:
            logger.error(f"Error scraping events: {str(e)}")
        
        return events
    
    async def get_event_details(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific event"""
        # Since we can't directly access events, we'll try to get the post details
        params = {
            'access_token': self.access_token,
            'fields': 'message,created_time,attachments{type,url,title,description}'
        }
        
        url = f"{self.base_url}/{event_id}"
        
        try:
            if not self.session:
                return None
                
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    post_data = await response.json()
                    # Convert post data to event format
                    return await self._extract_event_from_post(post_data, {})
                else:
                    logger.error(f"Failed to fetch post details: {response.status}")
                    logger.error(f"Response: {await response.text()}")
        except Exception as e:
            logger.error(f"Error getting event details: {str(e)}")
        
        return None
    
    def standardize_event(self, raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert Facebook event format to our standard schema"""
        try:
            # Extract place information
            place = raw_event.get('place', {})
            location = place.get('location', {})
            
            # Build standardized event
            return {
                "event_id": f"fb_{raw_event['id']}",
                "title": raw_event.get('name'),
                "description": raw_event.get('description', ''),
                "start_datetime": raw_event.get('start_time'),
                "end_datetime": raw_event.get('end_time'),
                "location": {
                    "venue_name": place.get('name'),
                    "address": location.get('street'),
                    "city": location.get('city'),
                    "state": location.get('state'),
                    "country": location.get('country'),
                    "coordinates": {
                        "lat": float(location.get('latitude', 0)),
                        "lng": float(location.get('longitude', 0))
                    }
                },
                "categories": [],  # Categories not available from posts
                "price_info": {
                    "currency": "USD",
                    "min_price": 0.0,
                    "max_price": 0.0,
                    "price_tier": "unknown"  # Would need to parse from description
                },
                "source": {
                    "platform": self.platform,
                    "url": f"https://facebook.com/{raw_event['id']}",
                    "last_updated": datetime.utcnow().isoformat()
                },
                "attributes": {
                    "is_online": False,  # Would need to parse from description
                    "online_event_format": None,
                    "attending_count": 0,  # Not available from posts
                    "interested_count": 0,  # Not available from posts
                    "cover_image": None,  # Would need additional processing of attachments
                    "ticket_uri": None  # Would need to parse from description or attachments
                }
            }
        except Exception as e:
            logger.error(f"Error standardizing event: {str(e)}")
            return None 