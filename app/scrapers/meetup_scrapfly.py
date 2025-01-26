"""
Meetup scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import json
from scrapfly import ScrapeConfig, ScrapflyClient
import time
from urllib.parse import urlencode
import re

from app.scrapers.base import ScrapflyBaseScraper, ScrapflyError
from app.utils.retry_handler import RetryError
from app.models.event import EventModel

class MeetupScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Meetup events using Scrapfly"""
    
    def __init__(self, api_key: str, **kwargs):
        """Initialize the Meetup scraper"""
        super().__init__(api_key=api_key, platform='meetup', **kwargs)
        self.base_url = "https://www.meetup.com/find/"
        self.logger = logging.getLogger(__name__)
        
    def _build_events_url(self, location: str = "Louisville", limit: int = 10) -> str:
        """Build the Meetup events URL with query parameters"""
        params = {
            'location': location,
            'source': 'EVENTS',
            'dateRange': 'this_week',
            'eventType': 'in-person',
            'pageSize': str(limit)
        }
        return f"{self.base_url}?{urlencode(params)}"

    async def extract_events(self, html_content: str) -> List[EventModel]:
        """Extract events from the HTML content."""
        self.logger.info(f"HTML content preview: {html_content[:2000]}")
        
        # Count script tags for debugging
        script_tags = re.findall(r'<script[^>]*>.*?</script>', html_content, re.DOTALL)
        self.logger.info(f"Found {len(script_tags)} script tags")
        
        # Try different patterns to find the data
        patterns = {
            'initial_state': r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
            'preloaded_state': r'window\.__PRELOADED_STATE__\s*=\s*({.*?});',
            'events_array': r'"events":\s*(\[.*?\])',
            'search_results': r'"searchResults":\s*({.*?})',
            'next_data': r'<script[^>]*type="application/json"[^>]*>(.*?)</script>'
        }
        
        events = []
        for pattern_name, pattern in patterns.items():
            matches = re.findall(pattern, html_content, re.DOTALL)
            self.logger.info(f"Pattern '{pattern}' found {len(matches)} matches")
            
            if matches:
                for match in matches:
                    try:
                        data = json.loads(match)
                        self.logger.info(f"Successfully parsed JSON data with keys: {list(data.keys())}")
                        
                        if pattern_name == 'next_data':
                            self.logger.info("Found Next.js data, attempting to parse")
                            
                            if 'props' in data:
                                props = data['props']
                                self.logger.info(f"Props keys: {list(props.keys())}")
                                
                                if 'pageProps' in props:
                                    page_props = props['pageProps']
                                    self.logger.info(f"PageProps keys: {list(page_props.keys())}")
                                    
                                    if '__APOLLO_STATE__' in page_props:
                                        apollo_state = page_props['__APOLLO_STATE__']
                                        self.logger.info(f"Apollo state keys: {list(apollo_state.keys())}")
                                        
                                        # Extract events from Apollo state
                                        for key, value in apollo_state.items():
                                            if isinstance(value, dict) and value.get('__typename') == 'Event':
                                                try:
                                                    # Extract basic event info
                                                    event_id = value.get('id')
                                                    title = value.get('title')
                                                    description = value.get('description')
                                                    start_time = value.get('dateTime')
                                                    event_url = value.get('eventUrl')
                                                    is_online = value.get('isOnline', False)
                                                    rsvp_count = value.get('rsvps', {}).get('totalCount', 0)
                                                    
                                                    # Skip online events
                                                    if is_online:
                                                        continue
                                                        
                                                    if not all([event_id, title, start_time]):
                                                        self.logger.warning(f"Skipping event due to missing required fields: {event_id}")
                                                        continue
                                                    
                                                    # Get venue info
                                                    venue_ref = value.get('venue', {}).get('__ref')
                                                    venue = apollo_state.get(venue_ref, {})
                                                    
                                                    # Get group info
                                                    group_ref = value.get('group', {}).get('__ref')
                                                    group = apollo_state.get(group_ref, {})
                                                    
                                                    # Create event model
                                                    event = EventModel(
                                                        platform_id=event_id,
                                                        title=title,
                                                        description=description or '',
                                                        start_datetime=datetime.fromisoformat(start_time.replace('Z', '+00:00')),
                                                        url=event_url or '',
                                                        venue_name=venue.get('name', ''),
                                                        venue_lat=venue.get('lat'),
                                                        venue_lon=venue.get('lon'),
                                                        venue_city=venue.get('city', ''),
                                                        venue_state=venue.get('state', ''),
                                                        venue_country=venue.get('country', ''),
                                                        organizer_name=group.get('name', ''),
                                                        organizer_id=group.get('id', ''),
                                                        rsvp_count=rsvp_count,
                                                        is_online=is_online,
                                                        platform='meetup'
                                                    )
                                                    events.append(event)
                                                    self.logger.info(f"Successfully extracted event: {title}")
                                                    
                                                except Exception as e:
                                                    self.logger.error(f"Error processing event {event_id}: {str(e)}")
                                                    continue
                                                    
                                    else:
                                        self.logger.warning("No Apollo state found in pageProps")
                                else:
                                    self.logger.warning("No pageProps found in props")
                            else:
                                self.logger.warning("No props found in Next.js data")
                                
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Failed to parse JSON data: {e}")
                    except Exception as e:
                        self.logger.error(f"Error processing match: {e}")
        
        self.logger.info(f"Successfully extracted {len(events)} events")
        return events

    async def scrape_events(self, location: str = "Louisville", limit: int = 10) -> List[EventModel]:
        """Scrape events from Meetup."""
        try:
            url = self._build_events_url(location=location, limit=limit)
            self.logger.info(f"Scraping Meetup events from URL: {url}")
            
            config = ScrapeConfig(
                url=url,
                render_js=True,
                asp=True,
                country='us',
                method='GET',
                headers={
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
            )
            
            result = await self.client.async_scrape(config)
            
            if not result or not result.content:
                raise ScrapflyError("Empty response from Scrapfly")
            
            return await self.extract_events(result.content)
            
        except Exception as e:
            self.logger.error(f"Error scraping events: {str(e)}")
            raise ScrapflyError(f"Failed to scrape events: {str(e)}")

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
        self.logger.debug(f"[meetup] Parsing GraphQL events from search results")
        
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
                        self.logger.warning(f"[meetup] Failed to parse event date: {str(e)}")
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
                    self.logger.debug(f"[meetup] Successfully parsed event: {event['title']}")
                    
                except Exception as e:
                    self.logger.warning(f"[meetup] Error parsing event: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"[meetup] Error parsing GraphQL events: {str(e)}")
            
        self.logger.info(f"[meetup] Successfully parsed {len(events)} events")
        return events 