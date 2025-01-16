"""
Facebook scraper implementation using Scrapfly.
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

class FacebookScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Facebook events using Scrapfly"""
    
    def __init__(self, api_key: str):
        """Initialize the Facebook scraper"""
        super().__init__(
            api_key,
            requests_per_second=1.0,  # Facebook has strict rate limits
            max_retries=3,
            error_threshold=0.2,
            monitor_window=15,
            circuit_failure_threshold=3
        )
        self.platform = "facebook"
        
    def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get Facebook-specific Scrapfly configuration"""
        base_config = super()._get_request_config(url, config)
        
        # Add Facebook-specific settings
        facebook_config = {
            'render_js': True,  # Enable JavaScript rendering
            'asp': True,  # Enable anti-scraping protection
            'country': 'us',  # Set country for geo-targeting
            'premium_proxy': True,  # Use premium proxies for better success rate
            'headers': {
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/91.0.4472.114',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'navigate'
            }
        }
        
        return {**base_config, **facebook_config}
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape events from Facebook.
        
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
                logger.info(f"Scraping Facebook page {page}: {url}")
                
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
            logger.error(f"Failed to scrape Facebook events: {str(e)}")
            raise
    
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int
    ) -> str:
        """Build the Facebook search URL with parameters"""
        base_url = "https://www.facebook.com/events/search"
        
        # Format dates
        start_date = int(date_range['start'].timestamp())
        end_date = int(date_range['end'].timestamp())
        
        # Build query parameters
        params = [
            f"location={location['city']}%2C+{location['state']}",
            f"start_time={start_date}",
            f"end_time={end_date}",
            f"page={page}"
        ]
        
        if categories:
            # Map categories to Facebook's category IDs
            category_ids = self._map_categories(categories)
            if category_ids:
                params.append(f"category_ids={','.join(category_ids)}")
        
        return f"{base_url}?{'&'.join(params)}"
    
    def _map_categories(self, categories: List[str]) -> List[str]:
        """Map generic categories to Facebook category IDs"""
        category_mapping = {
            "music": "185",
            "nightlife": "192",
            "arts": "183",
            "food": "187",
            "sports": "199",
            "fitness": "176",
            "charity": "184",
            "community": "186",
            "family": "178",
            "education": "174",
            "business": "189"
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
                    event_data = data.get("eventData") or data.get("events") or data.get("data", {}).get("events")
                    if not event_data:
                        continue
                    
                    if isinstance(event_data, list):
                        for event in event_data:
                            parsed_event = self._parse_json_event(event)
                            if parsed_event:
                                events.append(parsed_event)
                    elif isinstance(event_data, dict):
                        parsed_event = self._parse_json_event(event_data)
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
            event_id = event_data.get("id")
            title = event_data.get("name")
            start_time = event_data.get("startTime") or event_data.get("start_time")
            
            if not all([event_id, title, start_time]):
                return None
            
            # Parse dates
            try:
                start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                end_time = None
                if event_data.get("endTime") or event_data.get("end_time"):
                    end_time = datetime.fromisoformat(
                        (event_data.get("endTime") or event_data.get("end_time")).replace("Z", "+00:00")
                    )
            except (ValueError, AttributeError):
                return None
            
            # Extract location data
            place = event_data.get("place", {})
            location = {
                "venue_name": place.get("name", ""),
                "address": place.get("address", {}).get("formatted", ""),
                "city": place.get("address", {}).get("city", ""),
                "state": place.get("address", {}).get("state", ""),
                "latitude": float(place.get("location", {}).get("latitude", 0)),
                "longitude": float(place.get("location", {}).get("longitude", 0))
            }
            
            return {
                "title": title,
                "description": event_data.get("description", ""),
                "start_time": start_time,
                "end_time": end_time,
                "location": location,
                "image_url": event_data.get("coverUrl") or event_data.get("cover", {}).get("source", ""),
                "source": self.platform,
                "event_id": str(event_id),
                "url": f"https://www.facebook.com/events/{event_id}",
                "attendance_count": event_data.get("attendingCount", 0),
                "interested_count": event_data.get("interestedCount", 0),
                "organizer": event_data.get("owner", {}).get("name", ""),
                "is_online": event_data.get("isOnline", False),
                "ticket_url": event_data.get("ticketUrl", ""),
                "status": event_data.get("eventStatus", "scheduled")
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
                "class": re.compile(r"event-card|event_card|eventCard")
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
                ["h2", "h3", "div"],
                {"class": re.compile(r".*title.*|.*name.*", re.I)}
            )
            if not title_elem:
                return None
            
            # Extract date and time
            date_elem = card.find(
                ["time", "div", "span"],
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
            location_elem = card.find(
                ["div", "span"],
                {"class": re.compile(r".*location.*|.*venue.*", re.I)}
            )
            
            return {
                "title": title_elem.get_text(strip=True),
                "start_time": self._parse_date(date_elem.get_text(strip=True)),
                "location": {
                    "venue_name": location_elem.get_text(strip=True) if location_elem else "",
                    "address": "",
                    "latitude": 0,
                    "longitude": 0
                },
                "source": self.platform,
                "event_id": event_id,
                "url": f"https://www.facebook.com/events/{event_id}"
            }
            
        except Exception as e:
            logger.warning(f"Error parsing event card: {str(e)}")
            return None
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from event card"""
        try:
            # Add proper date parsing logic based on Facebook's format
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