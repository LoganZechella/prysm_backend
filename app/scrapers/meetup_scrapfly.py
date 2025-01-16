"""
Meetup scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import json
import re
from bs4 import BeautifulSoup

from app.scrapers.base import ScrapflyBaseScraper, ScrapflyError
from app.utils.retry_handler import RetryError

logger = logging.getLogger(__name__)

class MeetupScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Meetup events using Scrapfly"""
    
    def __init__(self, api_key: str):
        """Initialize the Meetup scraper"""
        super().__init__(
            api_key,
            requests_per_second=1.5,  # Meetup specific rate limit
            max_retries=3,
            error_threshold=0.2,
            monitor_window=15,
            circuit_failure_threshold=3
        )
        self.platform = "meetup"
        
    def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get Meetup-specific Scrapfly configuration"""
        base_config = super()._get_request_config(url, config)
        
        # Add Meetup-specific settings
        meetup_config = {
            'render_js': True,  # Enable JavaScript rendering
            'asp': True,  # Enable anti-scraping protection
            'country': 'us',  # Set country for geo-targeting
            'headers': {
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/91.0.4472.114',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
        }
        
        return {**base_config, **meetup_config}
    
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
                url = self._build_search_url(location, date_range, categories, page)
                logger.info(f"Scraping Meetup page {page}: {url}")
                
                try:
                    soup = await self._make_request(url)
                    if not soup:
                        break
                    
                    # First try to get events from JSON data
                    new_events = self._parse_json_events(soup)
                    
                    # Fallback to HTML parsing if JSON parsing fails
                    if not new_events:
                        new_events = self._parse_event_cards(soup)
                    
                    if not new_events:
                        logger.info(f"No more events found on page {page}")
                        break
                    
                    events.extend(new_events)
                    
                    # Check if there's a next page
                    if not self._has_next_page(soup):
                        break
                    
                    page += 1
                    
                except ScrapflyError as e:
                    logger.error(f"Scrapfly error on page {page}: {str(e)}")
                    break
                except RetryError as e:
                    logger.error(f"Max retries exceeded on page {page}: {str(e)}")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error on page {page}: {str(e)}")
                    break
            
            return events
            
        except Exception as e:
            logger.error(f"Failed to scrape Meetup events: {str(e)}")
            raise
    
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int
    ) -> str:
        """Build the Meetup search URL with parameters"""
        base_url = "https://www.meetup.com/find/events"
        
        # Format location
        location_str = f"{location['city']}-{location['state'].lower()}"
        
        # Format dates
        start_date = int(date_range['start'].timestamp() * 1000)  # Meetup uses milliseconds
        end_date = int(date_range['end'].timestamp() * 1000)
        
        # Build query parameters
        params = [
            f"location={location_str}",
            f"start={start_date}",
            f"end={end_date}",
            f"page={page}",
            "source=EVENTS"
        ]
        
        if categories:
            # Map categories to Meetup's category IDs
            category_ids = self._map_categories(categories)
            if category_ids:
                params.append(f"topic_id={','.join(category_ids)}")
        
        return f"{base_url}?{'&'.join(params)}"
    
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
    
    def _parse_json_events(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse events from embedded JSON data"""
        events = []
        
        try:
            # Look for JSON data in script tags
            scripts = soup.find_all("script", {"type": "application/json"})
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if not isinstance(data, dict):
                        continue
                    
                    # Look for event data in various JSON structures
                    event_data = (
                        data.get("props", {}).get("pageProps", {}).get("events") or
                        data.get("props", {}).get("initialState", {}).get("events")
                    )
                    
                    if not event_data:
                        continue
                    
                    if isinstance(event_data, list):
                        for event in event_data:
                            parsed_event = self._parse_json_event(event)
                            if parsed_event:
                                events.append(parsed_event)
                                
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"Error parsing JSON event data: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error parsing JSON data: {str(e)}")
            
        return events
    
    def _parse_json_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single event from JSON data"""
        try:
            # Extract required fields
            event_id = event_data.get("id") or event_data.get("eventId")
            title = event_data.get("title") or event_data.get("name")
            start_time = event_data.get("dateTime") or event_data.get("startTime")
            
            if not all([event_id, title, start_time]):
                return None
            
            # Parse dates
            try:
                start_time = datetime.fromtimestamp(int(start_time) / 1000)  # Meetup uses milliseconds
                end_time = None
                if event_data.get("endTime"):
                    end_time = datetime.fromtimestamp(int(event_data["endTime"]) / 1000)
            except (ValueError, TypeError):
                return None
            
            # Extract venue data
            venue = event_data.get("venue", {})
            group = event_data.get("group", {})
            
            return {
                "title": title,
                "description": event_data.get("description", ""),
                "start_time": start_time,
                "end_time": end_time,
                "location": {
                    "venue_name": venue.get("name", ""),
                    "address": venue.get("address", ""),
                    "city": venue.get("city", ""),
                    "state": venue.get("state", ""),
                    "latitude": float(venue.get("lat", 0)),
                    "longitude": float(venue.get("lng", 0))
                },
                "source": self.platform,
                "event_id": str(event_id),
                "url": event_data.get("eventUrl", ""),
                "group_name": group.get("name", ""),
                "group_url": group.get("urlname", ""),
                "is_online": event_data.get("isOnline", False),
                "rsvp_limit": event_data.get("rsvpLimit"),
                "rsvp_count": event_data.get("rsvpCount", 0),
                "fee": {
                    "amount": event_data.get("fee", {}).get("amount", 0),
                    "currency": event_data.get("fee", {}).get("currency", "USD")
                } if event_data.get("fee") else None,
                "topics": [
                    topic.get("name", "").lower()
                    for topic in event_data.get("topics", [])
                ]
            }
            
        except Exception as e:
            logger.warning(f"Error parsing JSON event: {str(e)}")
            return None
    
    def _parse_event_cards(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse event cards from search results page"""
        events = []
        
        try:
            # Find event cards using various possible class patterns
            event_cards = soup.find_all("div", {
                "class": re.compile(r"group-card|eventCard|event-card")
            })
            
            for card in event_cards:
                try:
                    event = self._parse_event_card(card)
                    if event:
                        events.append(event)
                except Exception as e:
                    logger.warning(f"Error parsing event card: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error parsing event cards: {str(e)}")
            
        return events
    
    def _parse_event_card(self, card: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Parse a single event card"""
        try:
            # Extract basic event info using flexible selectors
            title_elem = card.find(
                ["h3", "h2", "div"],
                {"class": re.compile(r".*title.*|.*name.*", re.I)}
            )
            if not title_elem:
                return None
            
            # Extract date and time
            date_elem = card.find(
                ["time", "span", "div"],
                {"class": re.compile(r".*time.*|.*date.*", re.I)}
            )
            if not date_elem:
                return None
            
            # Try to find the event URL and ID
            link = card.find("a", href=re.compile(r"/events/\d+"))
            if not link:
                return None
            
            event_id = re.search(r"/events/(\d+)", link["href"]).group(1)
            
            # Extract location info
            venue_elem = card.find(
                ["address", "div", "span"],
                {"class": re.compile(r".*venue.*|.*location.*", re.I)}
            )
            
            # Extract group info
            group_elem = card.find(
                ["a", "div", "span"],
                {"class": re.compile(r".*group.*|.*host.*", re.I)}
            )
            
            return {
                "title": title_elem.get_text(strip=True),
                "start_time": self._parse_date(date_elem.get_text(strip=True)),
                "location": {
                    "venue_name": venue_elem.get_text(strip=True) if venue_elem else "",
                    "address": "",
                    "latitude": 0,
                    "longitude": 0
                },
                "source": self.platform,
                "event_id": event_id,
                "url": f"https://www.meetup.com/events/{event_id}",
                "group_name": group_elem.get_text(strip=True) if group_elem else ""
            }
            
        except Exception as e:
            logger.warning(f"Error parsing event card: {str(e)}")
            return None
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from event card"""
        try:
            # Add proper date parsing logic based on Meetup's format
            return datetime.now()  # Placeholder
        except Exception:
            return datetime.now()
    
    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there's a next page of results"""
        try:
            # Look for next page button or link
            next_elem = soup.find(
                ["a", "button", "div"],
                {
                    "class": re.compile(r".*next.*", re.I),
                    "aria-label": re.compile(r".*next.*", re.I)
                }
            )
            
            if not next_elem:
                return False
            
            # Check if the next button is disabled
            disabled = (
                "disabled" in next_elem.get("class", []) or
                next_elem.get("aria-disabled") == "true" or
                next_elem.get("disabled") is not None
            )
            
            return not disabled
            
        except Exception:
            return False 