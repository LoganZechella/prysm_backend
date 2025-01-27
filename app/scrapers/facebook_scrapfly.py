"""
Facebook scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
import json
import os
import random
import base64
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import re
from scrapfly import ScrapflyClient, ScrapeConfig
from scrapfly.errors import ScrapflyError
import asyncio
import requests
from dateutil import parser
import hashlib

from app.scrapers.base import ScrapflyBaseScraper
from app.utils.retry_handler import RetryError
from app.models.event import EventModel
from app.services.location_recommendations import LocationService

logger = logging.getLogger(__name__)

class FacebookScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Facebook events using Scrapfly"""
    
    def __init__(self, api_key: str, **kwargs):
        """Initialize the Facebook scraper"""
        super().__init__(api_key=api_key, platform='facebook', **kwargs)
        self.logger = logging.getLogger(__name__)
        self.client = ScrapflyClient(key=api_key)
        self.location_service = LocationService()
    
    async def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """Make a request to Facebook using Scrapfly and return BeautifulSoup object"""
        try:
            self.logger.debug(f"[facebook] Making request to: {url}")
            config = await self._get_request_config(url)
            self.logger.debug(f"[facebook] Request config: {config}")
            
            result = await self.client.async_scrape(ScrapeConfig(**config))
            self.logger.debug(f"[facebook] Response status: {result.status_code}")
            
            if not result.success:
                self.logger.error(f"[facebook] Request failed: {result.error}")
                raise RetryError(f"Facebook request failed: {result.error}")
            
            # Parse HTML response
            try:
                soup = BeautifulSoup(result.content, 'html.parser')
                self.logger.debug(f"[facebook] Successfully parsed HTML response")
                return soup
            except Exception as e:
                self.logger.error(f"[facebook] Failed to parse HTML response: {str(e)}")
                raise RetryError(f"Failed to parse HTML response: {str(e)}")
            
        except ScrapflyError as e:
            self.logger.error(f"[facebook] Scrapfly error: {str(e)}")
            raise RetryError(f"Scrapfly error: {str(e)}")
        except Exception as e:
            self.logger.error(f"[facebook] Request error: {str(e)}")
            raise RetryError(f"Request error: {str(e)}")
    
    async def _get_request_config(self, url: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get Facebook-specific Scrapfly configuration"""
        self.logger.debug(f"[facebook] Building request config for URL: {url}")
        
        base_config = await super()._get_request_config(url, config)
        
        # Add Facebook-specific settings
        facebook_config = {
            'url': url,
            'render_js': True,
            'asp': True,
            'country': 'us',
            'tags': ['facebook', 'events'],
            'method': 'GET',
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
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        }
        
        # Add retry configuration
        facebook_config.update({
            'retry': {
                'enabled': True,
                'max_retries': 3,
                'retry_delay': 1000,
                'exceptions': [
                    'ScrapflyAspError',
                    'ScrapflyProxyError',
                    'UpstreamHttpClientError'
                ]
            }
        })
        
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
        seen_event_ids = set()  # Track seen events to prevent duplicates
        next_url = None
        
        try:
            while page <= max_pages:
                self.logger.info(f"[facebook] Scraping page {page}")
                
                # Build URL for first page or use next_url for subsequent pages
                url = next_url if next_url else await self._build_search_url(
                    location, 
                    date_range['start'], 
                    date_range['end']
                )
                
                if not url:
                    self.logger.error("[facebook] Failed to build search URL")
                    break
                
                try:
                    # Make request with retries
                    soup = None
                    for attempt in range(3):  # 3 retries
                        soup = await self._make_request(url)
                        if soup:
                            break
                        self.logger.warning(f"[facebook] Request attempt {attempt + 1} failed, retrying...")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
                    if not soup:
                        self.logger.error("[facebook] All request attempts failed")
                        break
                    
                    # Parse events from HTML
                    new_events = self._parse_html(soup)
                    self.logger.debug(f"[facebook] Found {len(new_events)} events from HTML")
                    
                    # Process new events
                    for event in new_events:
                        event_id = event.get('platform_id')
                        if not event_id:
                            continue
                            
                        # Skip duplicates
                        if event_id in seen_event_ids:
                            continue
                            
                        seen_event_ids.add(event_id)
                        events.append(event)
                    
                    self.logger.info(f"[facebook] Total unique events found: {len(events)}")
                    
                    # Get next page URL if available
                    next_link = soup.find("a", {"aria-label": "Next"})
                    next_url = next_link.get("href") if next_link else None
                    if not next_url:
                        self.logger.info("[facebook] No more pages to scrape")
                        break
                    
                    # Rate limiting
                    await asyncio.sleep(random.uniform(1.0, 2.0))
                    page += 1
                    
                except Exception as e:
                    self.logger.error(f"[facebook] Error on page {page}: {str(e)}")
                    if "rate limit" in str(e).lower():
                        self.logger.warning("[facebook] Rate limit hit, waiting longer...")
                        await asyncio.sleep(60)  # Wait longer on rate limit
                        continue
                    break
            
            self.logger.info(f"[facebook] Scraping completed. Found {len(events)} unique events")
            return events
            
        except Exception as e:
            self.logger.error(f"[facebook] Failed to scrape events: {str(e)}")
            raise RetryError(f"Facebook scraping failed: {str(e)}")
    
    async def _get_page_id(self, location: dict) -> str:
        """Get the Facebook page ID for a location."""
        # Return default Louisville page ID
        return "104006346303593"
    
    async def _build_search_url(
        self,
        location: dict,
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None,
        page: int = 1
    ) -> str:
        """Build the Facebook events search URL."""
        base_url = "https://www.facebook.com/events/search"
        
        # Format location
        location_query = f"{location.get('city', '')} {location.get('state', '')}"
        
        # Build query parameters
        params = {
            "q": location_query.strip(),
            "source": "discovery",
            "display": "list",
            "filter": "upcoming",
            "sort": "time",
        }
        
        # Add distance if provided
        if location.get('radius'):
            params['distance'] = str(location['radius'])
            params['unit'] = 'mi'
        
        # Build the URL with parameters
        url = f"{base_url}?{urlencode(params)}"
        self.logger.debug(f"[facebook] Built API URL: {url}")
        return url
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from Facebook HTML."""
        try:
            # Remove any extra text after the time
            if " and " in date_str:
                date_str = date_str.split(" and ")[0]
            
            # Handle relative dates
            if date_str.lower().startswith("today"):
                return datetime.now()
            elif date_str.lower().startswith("tomorrow"):
                return datetime.now() + timedelta(days=1)
            
            # Parse standard date format: "Day, DD Mon at HH:MM"
            # Example: "Thurs, 6 Mar at 19:00"
            match = re.match(r"(?:\w+), (\d+) (\w+) at (\d+):(\d+)", date_str)
            if match:
                day, month, hour, minute = match.groups()
                # Convert month name to number
                month_num = {
                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                }[month]
                
                # Get the current year
                year = datetime.now().year
                
                # Create the datetime object
                dt = datetime(year, month_num, int(day), int(hour), int(minute))
                
                # If the date is in the past, add a year
                if dt < datetime.now():
                    dt = dt.replace(year=year + 1)
                
                return dt
            
            self.logger.warning(f"[facebook] Error parsing date '{date_str}': Unknown format")
            return datetime.now()
            
        except Exception as e:
            self.logger.warning(f"[facebook] Error parsing date '{date_str}': {str(e)}")
            return datetime.now()
    
    def _parse_html(self, soup: BeautifulSoup) -> List[dict]:
        """Parse events from Facebook HTML."""
        events = []
        event_cards = soup.find_all("div", {"role": "article"})
        self.logger.debug(f"[facebook] Found {len(event_cards)} event cards")

        for i, card in enumerate(event_cards, 1):
            try:
                self.logger.debug(f"[facebook] Processing card {i}")
                self.logger.debug(f"[facebook] Card HTML: {card.prettify()}")
                
                # Extract title from the span with class x4k7w5x
                title_elem = card.find("span", {"class": "x4k7w5x"})
                if not title_elem:
                    self.logger.debug(f"[facebook] No title element found in card {i}")
                    continue
                title = title_elem.get_text().strip()
                self.logger.debug(f"[facebook] Found title: {title}")
                
                # Extract date - look for span with class x193iq5w containing date pattern
                date_spans = card.find_all("span", {"class": "x193iq5w"})
                date_str = None
                for span in date_spans:
                    text = span.get_text().strip()
                    if any(day in text.lower() for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']):
                        date_str = text
                        self.logger.debug(f"[facebook] Found date string: {date_str}")
                        break
                if not date_str:
                    self.logger.debug(f"[facebook] No date string found in card {i}")
                    continue
                
                # Parse the date string
                try:
                    # Example format: "Thu, Mar 6 at 7:00 PM EST"
                    date_parts = date_str.split(' at ')
                    date_part = date_parts[0].strip()  # "Thu, Mar 6"
                    time_part = date_parts[1].strip()  # "7:00 PM EST"
                    
                    # Add year since Facebook doesn't include it
                    current_year = datetime.now().year
                    date_with_year = f"{date_part}, {current_year}"
                    dt_str = f"{date_with_year} {time_part}"
                    start_datetime = parser.parse(dt_str)
                    self.logger.debug(f"[facebook] Parsed date: {start_datetime}")
                except Exception as e:
                    self.logger.error(f"[facebook] Error parsing date: {str(e)}")
                    continue
                
                # Extract venue name
                venue_name = None
                venue_spans = card.find_all("span", {"class": "x193iq5w"})
                for span in venue_spans:
                    # Look for spans that contain location-like text
                    text = span.get_text().strip()
                    # Skip if it's a date string
                    if any(day in text.lower() for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']):
                        continue
                    # Skip if it's an RSVP count
                    if any(word in text.lower() for word in ['going', 'interested']):
                        continue
                    # If we find a span with class x6prxxf, this is likely the venue
                    if span.find("span", {"class": "x6prxxf"}):
                        venue_name = text
                        self.logger.debug(f"[facebook] Found venue: {venue_name}")
                        break
                    # Backup: look for text that might be an address
                    elif any(word in text for word in ['Street', 'Avenue', 'Road', 'Blvd', 'Center', 'Theatre', 'Arena']):
                        venue_name = text
                        self.logger.debug(f"[facebook] Found venue (backup method): {venue_name}")
                        break

                if not venue_name:
                    self.logger.debug(f"[facebook] No venue found in card {i}")
                    continue
                
                # Extract image URL
                img = card.find("img", {"class": "x1rg5ohu"})
                image_url = None
                if img and img.get("src"):
                    image_url = img["src"]
                    self.logger.debug(f"[facebook] Found image URL: {image_url}")
                
                # Extract RSVP count
                rsvp_count = 0
                interested_count = 0
                for span in card.find_all("span", {"class": "x1lliihq"}):
                    text = span.get_text().strip()
                    if "going" in text.lower():
                        try:
                            rsvp_count = int(text.split('·')[1].strip().split()[0])
                        except (AttributeError, ValueError, IndexError):
                            pass
                
                # Generate a unique platform ID based on title and date
                platform_id = hashlib.md5(f"{title}{start_datetime}".encode()).hexdigest()
                
                # Construct event URL
                event_url = f"https://www.facebook.com/events/{platform_id}"
                
                # Geocode the venue address
                venue_lat = None
                venue_lon = None
                if venue_name:
                    # Construct full address with city and state
                    full_address = f"{venue_name}, Louisville, KY, US"
                    try:
                        location = self.location_service.geocoder.geocode(full_address)
                        if location:
                            venue_lat = location.latitude
                            venue_lon = location.longitude
                            self.logger.debug(f"[facebook] Geocoded venue coordinates: {venue_lat}, {venue_lon}")
                    except Exception as e:
                        self.logger.error(f"[facebook] Error geocoding venue: {str(e)}")
                
                # Create event data
                event_data = {
                    "platform_id": platform_id,
                    "title": title,
                    "description": "",
                    "start_datetime": start_datetime,
                    "end_datetime": start_datetime + timedelta(hours=2),  # Default duration
                    "url": event_url,
                    "venue_name": venue_name,
                    "venue_lat": venue_lat,
                    "venue_lon": venue_lon,
                    "venue_city": "Louisville",
                    "venue_state": "KY",
                    "venue_country": "US",
                    "organizer_id": platform_id,
                    "organizer_name": title.split(" - ")[0] if " - " in title else title,  # Use first part of title or full title as organizer name
                    "platform": "facebook",
                    "is_online": False,
                    "rsvp_count": rsvp_count,
                    "price_info": "null",
                    "categories": [],
                    "image_url": image_url
                }
                
                events.append(event_data)
                self.logger.debug(f"[facebook] Successfully parsed event: {title}")
                
            except Exception as e:
                self.logger.error(f"[facebook] Error parsing event card {i}: {str(e)}")
                continue

        self.logger.debug(f"[facebook] Found {len(events)} events from HTML")
        return events
    
    def _has_next_page(self, response_data: Dict[str, Any]) -> bool:
        """Check if there's a next page of results in the Graph API response"""
        try:
            paging = response_data.get('paging', {})
            return bool(paging.get('next'))
        except Exception:
            return False
    
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
    
    def _get_scraper_config(self) -> dict:
        if not os.getenv("FACEBOOK_ACCESS_TOKEN"):
            raise ValueError("FACEBOOK_ACCESS_TOKEN environment variable is required")
        
        return {
            "asp": True,
            "country": "us",
            "render_js": False,
            "retry_attempts": 3,
            "retry_wait_seconds": 10,
            "headers": {
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "Pragma": "no-cache",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            }
        } 