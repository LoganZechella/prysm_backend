"""
Meetup scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import json
import re
from bs4 import BeautifulSoup
import time

from app.scrapers.base import ScrapflyBaseScraper, ScrapflyError
from app.utils.retry_handler import RetryError

logger = logging.getLogger(__name__)

class MeetupScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Meetup events using Scrapfly"""
    
    def __init__(self, api_key: str, **kwargs):
        """Initialize the Meetup scraper"""
        super().__init__(api_key=api_key, platform='meetup', **kwargs)
        
    async def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get Meetup-specific Scrapfly configuration"""
        logger.debug(f"[meetup] Building request config for URL: {url}")
        
        base_config = await super()._get_request_config(url, config)
        
        # Add Meetup-specific settings for GraphQL
        meetup_config = {
            'render_js': False,  # No need for JS rendering for API requests
            'asp': True,  # Keep anti-scraping protection
            'country': 'us',
            'method': 'POST',
            'headers': {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Origin': 'https://www.meetup.com',
                'Referer': 'https://www.meetup.com/find/',
                'apollographql-client-name': 'nextjs-web',
                'apollographql-client-version': '1.0.0',
                'X-Meetup-View-Id': f'{"".join([str(int(time.time() * 1000))[i:i+2] for i in range(0, 13, 2)])}',
            }
        }
        
        final_config = {**base_config, **meetup_config}
        logger.debug(f"[meetup] Final request config: {json.dumps(final_config, indent=2)}")
        
        return final_config
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape events from Meetup.
        
        Args:
            location: Location parameters (city, state, lat, lng)
            date_range: Date range to search
            categories: Optional list of event categories
            
        Returns:
            List of scraped events
        """
        events = []
        page = 1
        max_pages = 10  # Limit to prevent infinite loops
        
        try:
            while page <= max_pages:
                url, query = self._build_search_url(location, date_range, categories, page)
                logger.info(f"[meetup] Scraping page {page}")
                logger.debug(f"[meetup] GraphQL query: {json.dumps(query, indent=2)}")
                
                try:
                    # Make GraphQL request
                    request_config = {
                        'render_js': False,  # No need for JS rendering
                        'asp': True,  # Keep anti-scraping protection
                        'country': 'us',
                        'method': 'POST',
                        'headers': {
                            'Accept': 'application/json',
                            'Content-Type': 'application/json',
                            'Origin': 'https://www.meetup.com',
                            'Referer': 'https://www.meetup.com/find/',
                            'apollographql-client-name': 'nextjs-web',
                            'apollographql-client-version': '1.0.0'
                        }
                    }
                    
                    response = await self._make_request(url, request_config, json_data=query)
                    if not response:
                        logger.warning("[meetup] No response received")
                        break
                    
                    # Parse response
                    logger.debug(f"[meetup] Raw response: {response[:2000]}")  # Log first 2000 chars
                    try:
                        data = json.loads(response)
                        logger.debug(f"[meetup] Parsed JSON data: {json.dumps(data, indent=2)}")
                    except json.JSONDecodeError as e:
                        logger.error(f"[meetup] Failed to parse JSON response: {str(e)}")
                        break
                        
                    if not data or 'data' not in data or 'result' not in data['data']:
                        logger.warning(f"[meetup] Invalid response format on page {page}")
                        logger.debug(f"[meetup] Response keys: {list(data.keys()) if data else None}")
                        if data and 'data' in data:
                            logger.debug(f"[meetup] Data keys: {list(data['data'].keys())}")
                        break
                    
                    search_results = data['data']['result']
                    logger.info(f"[meetup] Found {search_results.get('totalCount', 0)} total events")
                    
                    new_events = self._parse_graphql_events(search_results)
                    if not new_events:
                        logger.info(f"[meetup] No more events found on page {page}")
                        break
                    
                    events.extend(new_events)
                    logger.debug(f"[meetup] Found {len(new_events)} events on page {page}")
                    
                    # Check if there's a next page
                    page_info = search_results.get('pageInfo', {})
                    if not page_info.get('hasNextPage', False):
                        logger.info("[meetup] No more pages available")
                        break
                    
                    page += 1
                    
                except ScrapflyError as e:
                    logger.error(f"[meetup] Scrapfly error on page {page}: {str(e)}")
                    break
                except RetryError as e:
                    logger.error(f"[meetup] Max retries exceeded on page {page}: {str(e)}")
                    break
                except Exception as e:
                    logger.error(f"[meetup] Unexpected error on page {page}: {str(e)}")
                    break
            
            logger.info(f"[meetup] Successfully scraped {len(events)} total events")
            return events
            
        except Exception as e:
            logger.error(f"[meetup] Failed to scrape Meetup events: {str(e)}")
            raise
    
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int
    ) -> tuple[str, dict]:
        """Build the Meetup GraphQL API endpoint and query"""
        base_url = "https://www.meetup.com/gql"
        
        # Format location string
        location_str = f"us--{location['state'].lower()}--{location['city'].replace(' ', '%20')}"
        
        # Build GraphQL query
        query = {
            "operationName": "SearchEvents",
            "variables": {
                "first": 20,
                "after": f"{(page - 1) * 20}" if page > 1 else None,
                "location": location_str,
                "source": "EVENTS"
            },
            "query": """
                query SearchEvents($first: Int!, $after: String, $location: String!, $source: String) {
                    result: searchEvents(input: {first: $first, after: $after, location: $location, source: $source}) {
                        pageInfo {
                            hasNextPage
                            endCursor
                            __typename
                        }
                        totalCount
                        edges {
                            node {
                                id
                                title
                                description
                                dateTime
                                eventType
                                eventUrl
                                isOnline
                                maxTickets
                                rsvps {
                                    totalCount
                                    __typename
                                }
                                venue {
                                    id
                                    name
                                    lat
                                    lon
                                    city
                                    state
                                    country
                                    __typename
                                }
                                group {
                                    id
                                    name
                                    urlname
                                    timezone
                                    isPrivate
                                    isNewGroup
                                    __typename
                                }
                                featuredEventPhoto {
                                    baseUrl
                                    highResUrl
                                    id
                                    __typename
                                }
                                __typename
                            }
                            metadata {
                                recId
                                recSource
                                __typename
                            }
                            __typename
                        }
                        __typename
                    }
                }
            """
        }
        
        return base_url, query
    
    def _map_categories(self, categories: List[str]) -> List[str]:
        """Map generic categories to Meetup category IDs"""
        category_mapping = {
            "tech": "292",
            "business": "2",
            "career": "2",
            "education": "8",
            "fitness": "9",
            "health": "14",
            "sports": "32",
            "outdoors": "23",
            "photography": "27",
            "arts": "1",
            "culture": "4",
            "music": "21",
            "social": "31",
            "dance": "5",
            "food": "10",
            "language": "16",
            "pets": "26",
            "hobbies": "15"
        }
        
        return [category_mapping[cat.lower()]
                for cat in categories
                if cat.lower() in category_mapping]
    
    def _parse_graphql_events(self, search_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse events from GraphQL response"""
        events = []
        logger.debug(f"[meetup] Parsing GraphQL events from search results")
        
        try:
            for edge in search_results.get('edges', []):
                node = edge.get('node')
                if not node:
                    continue
                
                try:
                    venue = node.get('venue', {})
                    group = node.get('group', {})
                    photo = node.get('featuredEventPhoto', {})
                    
                    # Parse datetime (Meetup uses ISO format)
                    try:
                        start_time = datetime.fromisoformat(node.get('dateTime').replace('Z', '+00:00'))
                    except (ValueError, TypeError, AttributeError) as e:
                        logger.warning(f"[meetup] Failed to parse event date: {str(e)}")
                        continue
                    
                    event = {
                        'title': node.get('title', ''),
                        'description': node.get('description', ''),
                        'start_time': start_time,
                        'event_type': node.get('eventType', ''),
                        'location': {
                            'venue_name': venue.get('name', ''),
                            'city': venue.get('city', ''),
                            'state': venue.get('state', ''),
                            'country': venue.get('country', ''),
                            'latitude': venue.get('lat', 0),
                            'longitude': venue.get('lon', 0)
                        },
                        'source': self.platform,
                        'event_id': node.get('id', ''),
                        'url': node.get('eventUrl', ''),
                        'image_url': photo.get('highResUrl', ''),
                        'is_online': node.get('isOnline', False),
                        'attendance_count': node.get('rsvps', {}).get('totalCount', 0),
                        'max_tickets': node.get('maxTickets'),
                        'group': {
                            'id': group.get('id', ''),
                            'name': group.get('name', ''),
                            'url': f"https://www.meetup.com/{group.get('urlname', '')}",
                            'timezone': group.get('timezone', ''),
                            'is_private': group.get('isPrivate', False),
                            'is_new': group.get('isNewGroup', False)
                        },
                        'metadata': {
                            'rec_id': edge.get('metadata', {}).get('recId', ''),
                            'rec_source': edge.get('metadata', {}).get('recSource', '')
                        }
                    }
                    
                    events.append(event)
                    logger.debug(f"[meetup] Successfully parsed event: {event['title']}")
                    
                except Exception as e:
                    logger.warning(f"[meetup] Error parsing event: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"[meetup] Error parsing GraphQL events: {str(e)}")
            
        logger.info(f"[meetup] Successfully parsed {len(events)} events")
        return events 