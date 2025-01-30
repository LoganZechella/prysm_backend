"""
Meetup scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
import json
from bs4 import BeautifulSoup
import re
from scrapfly import ScrapeConfig, ScrapflyClient
from urllib.parse import urlencode

from app.scrapers.base import ScrapflyBaseScraper, ScrapflyError
from app.utils.retry_handler import RetryError
from app.models.event import EventModel

class MeetupScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Meetup events using Scrapfly"""
    
    def __init__(self, api_key: str, **kwargs):
        """Initialize the Meetup scraper"""
        super().__init__(api_key=api_key, platform='meetup', **kwargs)
        self.base_url = "https://www.meetup.com/find"
        self.logger = logging.getLogger(__name__)
        self.default_city = "Louisville"
        self.default_state = "KY"
        self.default_country = "US"
    
    async def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """Make a request to Meetup and return BeautifulSoup object"""
        try:
            self.logger.debug(f"[meetup] Making request to: {url}")
            config = await self._get_request_config(url)
            result = await self.client.async_scrape(ScrapeConfig(**config))
            
            if not result.success:
                self.logger.error(f"[meetup] Request failed: {result.error}")
                return None
            
            # Log response details
            self.logger.debug(f"[meetup] Response status code: {result.status_code}")
            self.logger.debug(f"[meetup] Response headers: {result.headers}")
            self.logger.debug(f"[meetup] Response content length: {len(result.content)}")
            
            # Parse HTML and log structure
            soup = BeautifulSoup(result.content, 'html.parser')
            self.logger.debug(f"[meetup] HTML title: {soup.title.string if soup.title else 'No title'}")
            self.logger.debug(f"[meetup] Found elements: {[tag.name for tag in soup.find_all()][:10]}")  # First 10 elements
            
            return soup
            
        except Exception as e:
            self.logger.error(f"[meetup] Request error: {str(e)}")
            return None
    
    async def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get Meetup-specific Scrapfly configuration"""
        base_config = await super()._get_request_config(url, config)
        
        # Add Meetup-specific settings
        meetup_config = {
            'url': url,
            'render_js': True,  # Meetup requires JavaScript
            'asp': True,
            'country': 'us',
            'headers': {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
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
            location: Dictionary with city, state, country, latitude, longitude
            date_range: Dictionary with start and end datetime
            categories: Optional list of category strings
            
        Returns:
            List of event dictionaries
        """
        events = []
        page = 1
        max_pages = 10  # Limit to prevent infinite loops
        
        try:
            while page <= max_pages:
                # Build URL for the current page
                url = self._build_search_url(location, date_range, categories, page)
                self.logger.info(f"[meetup] Scraping page {page}: {url}")
                
                # Make request
                soup = await self._make_request(url)
                if not soup:
                    self.logger.error("[meetup] Failed to get page content")
                    break
                
                # Parse events from the page
                new_events = self._parse_html(soup)
                self.logger.debug(f"[meetup] Found {len(new_events)} events on page {page}")
                
                if not new_events:
                    self.logger.info("[meetup] No more events found")
                    break
                
                events.extend(new_events)
                
                # Check for next page
                if not self._has_next_page(soup):
                    break
                
                page += 1
            
            self.logger.info(f"[meetup] Total events found: {len(events)}")
            return events
            
        except Exception as e:
            self.logger.error(f"[meetup] Error scraping events: {str(e)}")
            raise ScrapflyError(f"Failed to scrape events: {str(e)}")
    
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int = 1
    ) -> str:
        """Build the Meetup search URL with parameters"""
        # Format location
        location_str = f"{location['city'].lower()}-{location['state'].lower()}"
        
        # Calculate date range
        days_ahead = (date_range['end'] - date_range['start']).days
        date_range_str = 'upcoming' if days_ahead <= 7 else 'upcoming'
        
        # Build query parameters
        params = {
            'keywords': '',
            'location': location_str,
            'source': 'EVENTS',
            'distance': 'twentyFiveMiles',
            'page': str(page)
        }
        
        # Add categories if provided
        if categories:
            params['topics'] = ','.join(self._map_categories(categories))
        
        # Build final URL
        url = f"{self.base_url}/events"
        if params:
            url += f"?{urlencode(params)}"
        
        self.logger.debug(f"[meetup] Built URL: {url}")
        return url
    
    def _parse_html(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse events from Meetup HTML"""
        events = []
        
        # Log the HTML structure we're searching through
        self.logger.debug("[meetup] Starting HTML parsing")
        self.logger.debug(f"[meetup] HTML classes found: {[cls for tag in soup.find_all() for cls in tag.get('class', [])][:20]}")
        
        # Try multiple selectors for event cards
        event_cards = soup.find_all("div", {"data-testid": "event-card"})
        if not event_cards:
            self.logger.debug("[meetup] No events found with 'event-card' data-testid, trying alternate selectors")
            event_cards = soup.find_all("div", class_=lambda x: x and any(cls in x for cls in ["event-card", "eventCard"]))
            if not event_cards:
                self.logger.debug("[meetup] No events found with alternate selectors")
                event_cards = soup.find_all("div", class_=lambda x: x and "group-card" in x)
        
        self.logger.debug(f"[meetup] Found {len(event_cards)} event cards")
        
        # Log the first card's structure if any found
        if event_cards:
            first_card = event_cards[0]
            self.logger.debug(f"[meetup] First card classes: {first_card.get('class')}")
            self.logger.debug(f"[meetup] First card structure: {[child.name for child in first_card.children if child.name]}")
            self.logger.debug(f"[meetup] First card HTML: {first_card}")
        
        for i, card in enumerate(event_cards):
            try:
                self.logger.debug(f"[meetup] Processing card {i+1}")
                
                # Extract title and URL with detailed logging
                title_elem = card.find("a", {"class": "eventCard--link"})
                if not title_elem:
                    self.logger.debug(f"[meetup] Card {i+1}: No title element found with 'eventCard--link' class")
                    title_elem = card.find("a", {"data-testid": "event-card-link"})
                    if not title_elem:
                        self.logger.debug(f"[meetup] Card {i+1}: No title element found with alternate selector")
                        continue
                
                title = title_elem.get_text(strip=True)
                url = title_elem.get("href")
                self.logger.debug(f"[meetup] Card {i+1}: Found title: {title}")
                self.logger.debug(f"[meetup] Card {i+1}: Found URL: {url}")
                
                if not url.startswith("http"):
                    url = f"https://www.meetup.com{url}"
                
                # Extract date with detailed logging
                date_elem = card.find("time")
                if not date_elem:
                    self.logger.debug(f"[meetup] Card {i+1}: No date element found")
                    continue
                
                try:
                    datetime_str = date_elem.get("datetime")
                    self.logger.debug(f"[meetup] Card {i+1}: Found datetime string: {datetime_str}")
                    start_time = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                    self.logger.debug(f"[meetup] Card {i+1}: Parsed datetime: {start_time}")
                except (ValueError, AttributeError) as e:
                    self.logger.warning(f"[meetup] Card {i+1}: Could not parse date for event: {title}. Error: {str(e)}")
                    continue
                
                # Rest of the parsing with debug logging
                venue_elem = card.find("p", {"class": "venueDisplay"})
                if not venue_elem:
                    self.logger.debug(f"[meetup] Card {i+1}: No venue element found with 'venueDisplay' class")
                    venue_elem = card.find("div", {"data-testid": "venue-info"})
                
                venue_name = "Online Event"
                venue_city = location.get('city', self.default_city)
                venue_state = location.get('state', self.default_state)
                venue_country = location.get('country', self.default_country)
                is_online = True
                
                if venue_elem:
                    venue_text = venue_elem.get_text(strip=True)
                    self.logger.debug(f"[meetup] Card {i+1}: Found venue text: {venue_text}")
                    if venue_text and 'online' not in venue_text.lower():
                        venue_name = venue_text
                        is_online = False
                
                # Extract group name (organizer)
                group_elem = card.find("p", {"class": "groupName"})
                group_name = group_elem.get_text(strip=True) if group_elem else ""
                
                # Extract RSVP count
                rsvp_elem = card.find("p", {"class": "attendanceCount"})
                rsvp_count = 0
                if rsvp_elem:
                    rsvp_text = rsvp_elem.get_text(strip=True)
                    try:
                        rsvp_count = int(re.search(r'\d+', rsvp_text).group())
                    except (AttributeError, ValueError):
                        pass
                
                # Create and append event with all gathered data
                event = {
                    "platform_id": url.split('/')[-1],
                    "title": title,
                    "description": "",  # Need to fetch event page for full description
                    "start_datetime": start_time,
                    "end_datetime": start_time + timedelta(hours=2),  # Default duration
                    "url": url,
                    "venue_name": venue_name,
                    "venue_city": venue_city,
                    "venue_state": venue_state,
                    "venue_country": venue_country,
                    "organizer_name": group_name,
                    "organizer_id": "",
                    "platform": "meetup",
                    "is_online": is_online,
                    "rsvp_count": rsvp_count,
                    "categories": []
                }
                
                events.append(event)
                self.logger.debug(f"[meetup] Successfully parsed event: {title}")
                
            except Exception as e:
                self.logger.error(f"[meetup] Error parsing event card {i+1}: {str(e)}")
                if hasattr(e, '__traceback__'):
                    import traceback
                    self.logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
                continue
        
        self.logger.info(f"[meetup] Successfully parsed {len(events)} events from {len(event_cards)} cards")
        return events
    
    def _has_next_page(self, soup: BeautifulSoup) -> bool:
        """Check if there's a next page of results"""
        next_button = soup.find("button", {"aria-label": "Next page"})
        return next_button is not None and not next_button.get("disabled")
    
    def _map_categories(self, categories: List[str]) -> List[str]:
        """Map generic categories to Meetup category IDs"""
        category_mapping = {
            "music": "music",
            "nightlife": "nightlife",
            "arts": "arts",
            "food": "food-and-drink",
            "sports": "sports-and-fitness",
            "fitness": "health-and-wellness",
            "charity": "community",
            "community": "community",
            "family": "family",
            "education": "education",
            "business": "career-and-business",
            "tech": "tech",
            "games": "games",
            "language": "language-and-culture",
            "social": "socializing",
            "outdoors": "outdoors-and-adventure"
        }
        
        mapped = []
        for cat in categories:
            cat_lower = cat.lower()
            if cat_lower in category_mapping:
                mapped.append(category_mapping[cat_lower])
            else:
                self.logger.warning(f"[meetup] Unknown category: {cat}")
        
        return mapped 