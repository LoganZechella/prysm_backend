"""
Eventbrite scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import json
from bs4 import BeautifulSoup
import re

from app.scrapers.base import ScrapflyBaseScraper, ScrapflyError
from app.utils.retry_handler import RetryError

logger = logging.getLogger(__name__)

class EventbriteScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Eventbrite events using Scrapfly"""
    
    def __init__(self, api_key: str):
        """Initialize the Eventbrite scraper"""
        super().__init__(
            api_key,
            requests_per_second=2.0,  # Eventbrite specific rate limit
            max_retries=3,
            error_threshold=0.3,
            monitor_window=15,
            circuit_failure_threshold=5
        )
        self.platform = "eventbrite"
        
    def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get Eventbrite-specific Scrapfly configuration"""
        base_config = super()._get_request_config(url, config)
        
        # Add Eventbrite-specific settings
        eventbrite_config = {
            'render_js': True,  # Enable JavaScript rendering
            'asp': True,  # Enable anti-scraping protection
            'country': 'us',  # Set country for geo-targeting
            'headers': {
                'Accept-Language': 'en-US,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/91.0.4472.114'
            }
        }
        
        return {**base_config, **eventbrite_config}
    
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
                        break
                    
                    # First try to get events from JSON-LD
                    new_events = self._parse_json_ld(soup)
                    
                    # Fallback to HTML parsing if JSON-LD fails
                    if not new_events:
                        new_events = self._parse_html(soup)
                    
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
        location_str = f"{location['city']}-{location['state'].lower()}"
        
        # Format dates
        start_date = date_range['start'].strftime("%Y-%m-%d")
        end_date = date_range['end'].strftime("%Y-%m-%d")
        
        # Build query parameters
        params = [
            f"location={location_str}",
            f"start-date={start_date}",
            f"end-date={end_date}",
            f"page={page}"
        ]
        
        if categories:
            # Map categories to Eventbrite's category IDs
            category_ids = self._map_categories(categories)
            if category_ids:
                params.append(f"categories={','.join(category_ids)}")
        
        query_string = '&'.join(params)
        return f"{base_url}/events/search/?{query_string}"
    
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
        
        try:
            # Find all JSON-LD script tags
            script_tags = soup.find_all("script", {"type": "application/ld+json"})
            
            for script_tag in script_tags:
                try:
                    data = json.loads(script_tag.string)
                    
                    # Handle both single event and event list formats
                    if isinstance(data, dict):
                        if data.get("@type") == "Event":
                            events.append(self._parse_event_json_ld(data))
                        elif "itemListElement" in data:
                            for item in data["itemListElement"]:
                                if isinstance(item, dict) and "item" in item:
                                    events.append(self._parse_event_json_ld(item["item"]))
                    
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"Error parsing event JSON-LD: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"Error parsing JSON-LD data: {str(e)}")
        
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
            
            venue_info = {
                "venue_name": location.get("name", ""),
                "address": ", ".join(filter(None, [
                    address.get("streetAddress", ""),
                    address.get("addressLocality", ""),
                    address.get("addressRegion", ""),
                    address.get("postalCode", "")
                ])),
                "latitude": float(geo.get("latitude", 0)),
                "longitude": float(geo.get("longitude", 0))
            }
            
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
                "title": event_data.get("name", ""),
                "description": event_data.get("description", ""),
                "start_time": start_time,
                "end_time": end_time,
                "location": venue_info,
                "image_url": event_data.get("image", ""),
                "source": self.platform,
                "event_id": event_id,
                "url": url,
                "prices": prices,
                "organizer": event_data.get("organizer", {}).get("name", ""),
                "performers": [p.get("name", "") for p in event_data.get("performers", [])],
                "categories": [
                    cat.get("name", "").lower()
                    for cat in event_data.get("superEvent", {}).get("type", [])
                ],
                "tags": event_data.get("keywords", "").split(",") if event_data.get("keywords") else [],
                "attendance_mode": event_data.get("eventAttendanceMode", "offline"),
                "status": event_data.get("eventStatus", "scheduled")
            }
            
        except Exception as e:
            logger.warning(f"Error parsing event: {str(e)}")
            return None
    
    def _parse_html(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Fallback HTML parser for when JSON-LD is not available"""
        events = []
        
        try:
            event_cards = soup.find_all("div", {"class": re.compile(r"eds-event-card.*")})
            
            for card in event_cards:
                try:
                    # Extract basic event info
                    title_elem = card.find("div", {"class": re.compile(r"eds-event-card__formatted-name.*")})
                    date_elem = card.find("div", {"class": re.compile(r"eds-event-card__formatted-date.*")})
                    location_elem = card.find("div", {"class": re.compile(r"card-text--truncated__one.*")})
                    
                    if not (title_elem and date_elem):
                        continue
                    
                    # Get event URL and ID
                    link = card.find("a", {"class": re.compile(r"eds-event-card-content__action-link.*")})
                    url = link.get("href", "") if link else ""
                    event_id = url.split("-")[-1] if url else ""
                    
                    # Get image URL
                    img = card.find("img", {"class": re.compile(r"eds-event-card__image.*")})
                    image_url = img.get("src", "") if img else ""
                    
                    event = {
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
                        "url": url,
                        "image_url": image_url
                    }
                    
                    events.append(event)
                    
                except Exception as e:
                    logger.warning(f"Error parsing event card: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"Error parsing HTML: {str(e)}")
        
        return events
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from event card"""
        try:
            # Add proper date parsing logic based on Eventbrite's format
            return datetime.now()  # Placeholder
        except Exception:
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