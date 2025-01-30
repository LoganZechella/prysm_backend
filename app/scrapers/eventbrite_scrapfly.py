"""
Eventbrite scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import json
from bs4 import BeautifulSoup
import re
from scrapfly import ScrapeConfig

from app.scrapers.base import ScrapflyBaseScraper, ScrapflyError
from app.utils.retry_handler import RetryError

logger = logging.getLogger(__name__)

class EventbriteScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Eventbrite events using Scrapfly"""
    
    def __init__(self, api_key: str, **kwargs):
        """Initialize the Eventbrite scraper"""
        super().__init__(api_key=api_key, platform='eventbrite', **kwargs)
        self.default_city = "Louisville"
        self.default_state = "KY"
        self.default_country = "US"
        
    async def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """Make a request to Eventbrite and return BeautifulSoup object"""
        try:
            logger.debug(f"[eventbrite] Making request to: {url}")
            config = await self._get_request_config(url)
            result = await self.client.async_scrape(ScrapeConfig(**config))
            
            if not result.success:
                logger.error(f"[eventbrite] Request failed: {result.error}")
                return None
                
            return BeautifulSoup(result.content, 'html.parser')
            
        except Exception as e:
            logger.error(f"[eventbrite] Request error: {str(e)}")
            return None
    
    async def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get Eventbrite-specific Scrapfly configuration"""
        logger.debug(f"[eventbrite] Building request config for URL: {url}")
        
        base_config = await super()._get_request_config(url, config)
        logger.debug(f"[eventbrite] Base config from parent: {json.dumps(base_config, indent=2)}")
        
        # Add Eventbrite-specific settings
        eventbrite_config = {
            'url': url,
            'render_js': True,  # Enable JavaScript rendering
            'asp': True,  # Enable anti-scraping protection
            'country': 'US',
            'tags': ['eventbrite', 'events'],
            'session': True,  # Enable session handling
            'retry': {  # Add retry configuration
                'enabled': True,
                'max_retries': 3,
                'retry_delay': 1000,
                'exceptions': [
                    'ScrapflyAspError',
                    'ScrapflyProxyError',
                    'UpstreamHttpClientError'
                ]
            },
            'headers': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
                'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '"macOS"',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            }
        }
        
        logger.debug(f"[eventbrite] Adding Eventbrite-specific config: {json.dumps(eventbrite_config, indent=2)}")
        final_config = {**base_config, **eventbrite_config}
        logger.debug(f"[eventbrite] Final merged config: {json.dumps(final_config, indent=2)}")
        
        return final_config
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape events from Eventbrite.
        
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
                logger.info(f"Scraping Eventbrite page {page}: {url}")
                
                try:
                    soup = await self._make_request(url)
                    if not soup:
                        logger.warning("[eventbrite] No HTML content returned from request")
                        break
                    
                    # Log the page title and any error messages
                    logger.debug(f"[eventbrite] Page title: {soup.title.string if soup.title else 'No title'}")
                    error_messages = soup.find_all(text=re.compile(r'error|no results|not found', re.I))
                    if error_messages:
                        logger.warning(f"[eventbrite] Found error messages: {[msg.strip() for msg in error_messages]}")
                    
                    # First try to get events from JSON-LD
                    new_events = self._parse_json_ld(soup)
                    logger.debug(f"[eventbrite] Found {len(new_events)} events from JSON-LD")
                    
                    # Fallback to HTML parsing if JSON-LD fails
                    if not new_events:
                        logger.debug("[eventbrite] No events found in JSON-LD, trying HTML parsing")
                        new_events = self._parse_html(soup)
                        logger.debug(f"[eventbrite] Found {len(new_events)} events from HTML")
                    
                    if not new_events:
                        logger.info(f"No more events found on page {page}")
                        break
                    
                    events.extend(new_events)
                    logger.debug(f"[eventbrite] Total events found so far: {len(events)}")
                    
                    # Check if there's a next page
                    has_next = self._has_next_page(soup)
                    logger.debug(f"[eventbrite] Has next page: {has_next}")
                    if not has_next:
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
            logger.error(f"Failed to scrape Eventbrite events: {str(e)}")
            raise
    
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int
    ) -> str:
        """Build the Eventbrite search URL with parameters"""
        base_url = "https://www.eventbrite.com/d"
        
        # Format location
        location_str = f"{location['state'].lower()}--{location['city'].lower()}"
        
        # Build URL path
        path = f"/{location_str}/events--next-week"
        
        # Build query parameters
        params = [
            f"page={page}"
        ]
        
        if categories:
            # Map categories to Eventbrite's category IDs
            category_ids = self._map_categories(categories)
            if category_ids:
                params.append(f"categories={','.join(category_ids)}")
        
        # Construct final URL
        query = '&'.join(params)
        return f"{base_url}{path}/?{query}"
    
    def _map_categories(self, categories: List[str]) -> List[str]:
        """Map generic categories to Eventbrite category IDs"""
        category_mapping = {
            "music": "103",
            "business": "101",
            "food": "110",
            "arts": "105",
            "film": "104",
            "sports": "108",
            "health": "107",
            "science": "102",
            "technology": "102",
            "charity": "111",
            "community": "113",
            "fashion": "106",
            "lifestyle": "109",
            "auto": "118",
            "hobbies": "119"
        }
        
        return [category_mapping[cat.lower()]
                for cat in categories
                if cat.lower() in category_mapping]
    
    def _parse_json_ld(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse events from JSON-LD data in the page"""
        events = []
        logger.debug("[eventbrite] Starting JSON-LD parsing")
        
        try:
            # Find all JSON-LD script tags
            script_tags = soup.find_all("script", {"type": "application/ld+json"})
            logger.debug(f"[eventbrite] Found {len(script_tags)} JSON-LD script tags")
            
            for script_tag in script_tags:
                try:
                    data = json.loads(script_tag.string)
                    logger.debug(f"[eventbrite] Parsed JSON-LD data: {json.dumps(data, indent=2)}")
                    
                    # Handle both single event and event list formats
                    if isinstance(data, dict):
                        if data.get("@type") == "Event":
                            logger.debug("[eventbrite] Found single event in JSON-LD")
                            events.append(self._parse_event_json_ld(data))
                        elif "itemListElement" in data:
                            logger.debug("[eventbrite] Found event list in JSON-LD")
                            for item in data["itemListElement"]:
                                if isinstance(item, dict) and "item" in item:
                                    events.append(self._parse_event_json_ld(item["item"]))
                    
                except json.JSONDecodeError:
                    logger.warning("[eventbrite] Failed to parse JSON-LD script tag")
                    continue
                except Exception as e:
                    logger.warning(f"[eventbrite] Error parsing event JSON-LD: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"[eventbrite] Error parsing JSON-LD data: {str(e)}")
        
        logger.debug(f"[eventbrite] Found {len(events)} events in JSON-LD")
        return [event for event in events if event is not None]
    
    def _parse_event_json_ld(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single event from JSON-LD data"""
        try:
            # Parse dates
            start_time = datetime.fromisoformat(
                event_data["startDate"].replace("Z", "+00:00")
            )
            end_time = datetime.fromisoformat(
                event_data["endDate"].replace("Z", "+00:00")
            )
            
            # Extract location data
            location = event_data.get("location", {})
            address = location.get("address", {})
            geo = location.get("geo", {})
            
            # Extract venue information with defaults
            venue_name = location.get("name", "Unknown Venue")  # Default venue name
            venue_city = address.get("addressLocality", self.default_city)  # Default city
            venue_state = address.get("addressRegion", self.default_state)  # Default state
            venue_country = address.get("addressCountry", self.default_country)  # Default country
            venue_lat = float(geo.get("latitude", 0.0))  # Default latitude
            venue_lon = float(geo.get("longitude", 0.0))  # Default longitude
            
            # Extract event URL and ID
            url = event_data.get("url", "")
            event_id = url.split("-")[-1] if url else ""
            
            # Extract price information
            offers = event_data.get("offers", [])
            if isinstance(offers, dict):
                offers = [offers]
            
            prices = []
            for offer in offers:
                if isinstance(offer, dict):
                    price = {
                        "amount": float(offer.get("price", 0)),
                        "currency": offer.get("priceCurrency", "USD"),
                        "availability": offer.get("availability", "")
                    }
                    prices.append(price)
            
            return {
                "platform_id": event_id,
                "title": event_data.get("name", ""),
                "description": event_data.get("description", ""),
                "start_datetime": start_time,
                "end_datetime": end_time,
                "url": url,
                "venue_name": venue_name,
                "venue_lat": venue_lat,
                "venue_lon": venue_lon,
                "venue_city": venue_city,
                "venue_state": venue_state,
                "venue_country": venue_country,
                "organizer_name": event_data.get("organizer", {}).get("name", ""),
                "organizer_id": "",
                "platform": "eventbrite",
                "is_online": event_data.get("eventAttendanceMode", "") == "OnlineEventAttendanceMode",
                "rsvp_count": 0,  # Eventbrite doesn't provide this in JSON-LD
                "price_info": prices,
                "categories": [
                    cat.get("name", "").lower()
                    for cat in event_data.get("superEvent", {}).get("type", [])
                ],
                "image_url": event_data.get("image", ""),
                "technical_level": 0.5  # Default technical level
            }
            
        except Exception as e:
            logger.warning(f"Error parsing event: {str(e)}")
            return None
    
    def _parse_html(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Fallback HTML parser for when JSON-LD is not available"""
        events = []
        logger.debug("[eventbrite] Starting HTML parsing")
        
        try:
            event_cards = soup.find_all("div", {"class": re.compile(r"eds-event-card.*")})
            logger.debug(f"[eventbrite] Found {len(event_cards)} event cards")
            
            for card in event_cards:
                try:
                    # Extract basic event info
                    title_elem = card.find("div", {"class": re.compile(r"eds-event-card__formatted-name.*")})
                    date_elem = card.find("div", {"class": re.compile(r"eds-event-card__formatted-date.*")})
                    location_elem = card.find("div", {"class": re.compile(r"card-text--truncated__one.*")})
                    
                    logger.debug(f"[eventbrite] Card elements - Title: {title_elem is not None}, Date: {date_elem is not None}, Location: {location_elem is not None}")
                    
                    if not (title_elem and date_elem):
                        logger.debug("[eventbrite] Skipping card - missing required elements")
                        continue
                    
                    # Get event URL and ID
                    link = card.find("a", {"class": re.compile(r"eds-event-card-content__action-link.*")})
                    url = link.get("href", "") if link else ""
                    event_id = url.split("-")[-1] if url else ""
                    
                    logger.debug(f"[eventbrite] Event URL: {url}, ID: {event_id}")
                    
                    # Extract image URL
                    img = card.find("img", {"class": re.compile(r"eds-event-card__image.*")})
                    image_url = img.get("src", "") if img else ""
                    
                    # Parse start datetime
                    start_datetime = self._parse_date(date_elem.get_text(strip=True))
                    # Set end datetime to 2 hours after start by default
                    end_datetime = start_datetime + timedelta(hours=2)
                    
                    # Extract venue information with defaults
                    venue_name = location_elem.get_text(strip=True) if location_elem else "Unknown Venue"
                    venue_city = self.default_city  # Default city
                    venue_state = self.default_state  # Default state
                    venue_country = self.default_country  # Default country
                    venue_lat = 0.0  # Default latitude
                    venue_lon = 0.0  # Default longitude
                    
                    event = {
                        "platform_id": event_id,
                        "title": title_elem.get_text(strip=True),
                        "description": "",  # Full description requires fetching event page
                        "start_datetime": start_datetime,
                        "end_datetime": end_datetime,
                        "url": url,
                        "venue_name": venue_name,
                        "venue_lat": venue_lat,
                        "venue_lon": venue_lon,
                        "venue_city": venue_city,
                        "venue_state": venue_state,
                        "venue_country": venue_country,
                        "organizer_name": "",  # Not available in card view
                        "organizer_id": "",
                        "platform": "eventbrite",
                        "is_online": False,  # Default to in-person
                        "rsvp_count": 0,
                        "price_info": [],  # Price info requires fetching event page
                        "categories": [],  # Categories not available in card view
                        "image_url": image_url,
                        "technical_level": 0.5  # Default technical level
                    }
                    
                    logger.debug(f"[eventbrite] Parsed event: {json.dumps(event, indent=2, default=str)}")
                    events.append(event)
                    
                except Exception as e:
                    logger.warning(f"[eventbrite] Error parsing event card: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"[eventbrite] Error parsing HTML: {str(e)}")
        
        logger.debug(f"[eventbrite] Found {len(events)} events in HTML")
        return events
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from event card"""
        try:
            # Eventbrite date formats can vary, but common formats are:
            # "Sat, Feb 8" or "Saturday, February 8"
            # "7:00 PM" or "19:00"
            
            # First try to find a date pattern
            date_match = re.search(r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+([A-Za-z]+)\s+(\d{1,2})', date_str)
            if not date_match:
                logger.warning(f"[eventbrite] Could not parse date from: {date_str}")
                return datetime.now()
            
            month = date_match.group(1)
            day = int(date_match.group(2))
            
            # Try to find time pattern
            time_match = re.search(r'(\d{1,2}):(\d{2})(?:\s*(AM|PM))?', date_str)
            if not time_match:
                logger.warning(f"[eventbrite] Could not parse time from: {date_str}")
                return datetime.now()
            
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            ampm = time_match.group(3)
            
            # Convert to 24-hour format if needed
            if ampm and ampm.upper() == 'PM' and hour < 12:
                hour += 12
            elif ampm and ampm.upper() == 'AM' and hour == 12:
                hour = 0
            
            # Use current year, but if the date would be in the past,
            # use next year instead
            year = datetime.now().year
            dt = datetime.strptime(f"{year} {month} {day} {hour}:{minute}", "%Y %B %d %H:%M")
            if dt < datetime.now():
                dt = dt.replace(year=year + 1)
            
            return dt
            
        except Exception as e:
            logger.warning(f"[eventbrite] Error parsing date '{date_str}': {str(e)}")
            return datetime.now()
    
    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there's a next page of results"""
        try:
            pagination = soup.find("nav", {"aria-label": "Pagination"})
            if not pagination:
                return False
            
            next_button = pagination.find(
                "button",
                {"aria-label": re.compile(r".*[Nn]ext.*")}
            )
            
            return next_button is not None and not next_button.get("disabled")
            
        except Exception:
            return False 