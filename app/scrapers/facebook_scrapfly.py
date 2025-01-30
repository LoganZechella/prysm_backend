"""
Facebook scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
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

tzinfos = {
    'PST': -8 * 3600,  # Pacific Standard Time
    'PDT': -7 * 3600,  # Pacific Daylight Time
    'MST': -7 * 3600,  # Mountain Standard Time
    'MDT': -6 * 3600   # Mountain Daylight Time
}

class FacebookScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Facebook events using Scrapfly"""
    
    def __init__(self, api_key: str, **kwargs):
        """Initialize the Facebook scraper"""
        super().__init__(api_key=api_key, platform='facebook', **kwargs)
        self.logger = logging.getLogger(__name__)
        self.client = ScrapflyClient(key=api_key)
        self.location_service = LocationService()
        
        # Default location settings
        self.default_city = "Louisville"
        self.default_state = "KY"
        self.default_country = "US"
    
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
            self.logger.info(f"[facebook] Starting event scraping for {location['city']}, {location['state']}")
            self.logger.info(f"[facebook] Date range: {date_range['start']} to {date_range['end']}")
            
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
                
                self.logger.info(f"[facebook] Requesting URL: {url}")
                
                try:
                    # Make request with retries
                    soup = None
                    for attempt in range(3):  # 3 retries
                        try:
                            soup = await self._make_request(url)
                            if soup:
                                self.logger.info(f"[facebook] Successfully got response on attempt {attempt + 1}")
                                break
                            self.logger.warning(f"[facebook] Request attempt {attempt + 1} failed, retrying...")
                        except RetryError as e:
                            self.logger.error(f"[facebook] Retry error on attempt {attempt + 1}: {str(e)}")
                        except Exception as e:
                            self.logger.error(f"[facebook] Unexpected error on attempt {attempt + 1}: {str(e)}")
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
                    if not soup:
                        self.logger.error("[facebook] All request attempts failed")
                        break
                    
                    # Parse events from HTML
                    try:
                        new_events = self._parse_html(soup)
                        self.logger.info(f"[facebook] Found {len(new_events)} events on page {page}")
                    except Exception as e:
                        self.logger.error(f"[facebook] Error parsing HTML: {str(e)}")
                        if hasattr(e, '__traceback__'):
                            import traceback
                            self.logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
                        break
                    
                    # Process new events
                    for event in new_events:
                        event_id = event.get('platform_id')
                        if not event_id:
                            self.logger.warning("[facebook] Event missing platform_id, skipping")
                            continue
                            
                        # Skip duplicates
                        if event_id in seen_event_ids:
                            self.logger.debug(f"[facebook] Skipping duplicate event {event_id}")
                            continue
                            
                        seen_event_ids.add(event_id)
                        events.append(event)
                        self.logger.debug(f"[facebook] Added event: {event.get('title')} ({event_id})")
                    
                    self.logger.info(f"[facebook] Total unique events found so far: {len(events)}")
                    
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
                    if hasattr(e, '__traceback__'):
                        import traceback
                        self.logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
                    if "rate limit" in str(e).lower():
                        self.logger.warning("[facebook] Rate limit hit, waiting longer...")
                        await asyncio.sleep(60)  # Wait longer on rate limit
                        continue
                    break
            
            self.logger.info(f"[facebook] Scraping completed. Found {len(events)} unique events")
            return events
            
        except Exception as e:
            self.logger.error(f"[facebook] Failed to scrape events: {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                self.logger.error(f"Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            return []  # Return empty list instead of raising error
    
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
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string from Facebook event.
        
        Handles multiple date formats:
        - Old format: "Thu, Mar 6 at 7:00 PM EST"
        - New format: "Fri, Jan 31, 2025 10:30 AM EST"
        - Alternate format: "Tomorrow at 7:00 PM EST"
        - ISO format: "2024-01-31T19:00:00-0500"
        """
        if not date_str:
            self.logger.error("[facebook] Empty date string provided")
            return None
            
        self.logger.debug(f"[facebook] Attempting to parse date string: {date_str}")
        
        try:
            # First try parsing as ISO format
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                self.logger.debug(f"[facebook] Successfully parsed ISO date: {dt}")
                return dt
            except ValueError:
                self.logger.debug("[facebook] Not an ISO format date, trying other formats")
            
            # Try dateutil parser first as it's more flexible
            try:
                dt = parser.parse(date_str, tzinfos=tzinfos)
                self.logger.debug(f"[facebook] Successfully parsed with dateutil: {dt}")
                return dt
            except (parser.ParserError, ValueError):
                self.logger.debug("[facebook] dateutil parser failed, trying manual parsing")
            
            # Handle "Tomorrow" and "Today" cases
            if "tomorrow" in date_str.lower():
                base_date = datetime.now() + timedelta(days=1)
                self.logger.debug("[facebook] Found 'tomorrow' in date string")
            elif "today" in date_str.lower():
                base_date = datetime.now()
                self.logger.debug("[facebook] Found 'today' in date string")
            else:
                base_date = None
            
            if base_date:
                # Extract time part after "at"
                time_match = re.search(r'at\s+(\d{1,2}):(\d{2})\s*(AM|PM)?\s*([A-Z]{3})?', date_str, re.IGNORECASE)
                if time_match:
                    hour, minute, ampm, tz = time_match.groups()
                    hour = int(hour)
                    minute = int(minute)
                    if ampm and ampm.upper() == 'PM' and hour < 12:
                        hour += 12
                    elif ampm and ampm.upper() == 'AM' and hour == 12:
                        hour = 0
                    
                    base_date = base_date.replace(hour=hour, minute=minute)
                    self.logger.debug(f"[facebook] Parsed relative date: {base_date}")
                    return base_date
            
            # Try parsing standard formats
            formats_to_try = [
                # New format with year (no "at")
                (r'([A-Za-z]+),\s+([A-Za-z]+)\s+(\d{1,2})(?:,\s+(\d{4}))?\s+(\d{1,2}):(\d{2})\s+(AM|PM)\s+([A-Z]{3})',
                 lambda m: self._construct_datetime(m.groups(), has_year=True)),
                # New format with year and "at"
                (r'([A-Za-z]+),\s+([A-Za-z]+)\s+(\d{1,2})(?:,\s+(\d{4}))?\s+at\s+(\d{1,2}):(\d{2})\s+(AM|PM)\s+([A-Z]{3})',
                 lambda m: self._construct_datetime_with_at(m.groups())),
                # Old format with "at"
                (r'([A-Za-z]+),\s+([A-Za-z]+)\s+(\d{1,2})\s+at\s+(\d{1,2}):(\d{2})\s+(AM|PM)\s+([A-Z]{3})',
                 lambda m: self._construct_datetime(m.groups())),
                # Compact format
                (r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})\s*(AM|PM)?',
                 lambda m: self._construct_datetime_compact(m.groups()))
            ]
            
            for pattern, parser_func in formats_to_try:
                match = re.match(pattern, date_str, re.IGNORECASE)
                if match:
                    dt = parser_func(match)
                    if dt:
                        self.logger.debug(f"[facebook] Successfully parsed with pattern {pattern}: {dt}")
                        return dt
                    else:
                        self.logger.debug(f"[facebook] Parser function failed for pattern {pattern}")
            
            # If all else fails, try one more time with dateutil parser in fuzzy mode
            try:
                dt = parser.parse(date_str, fuzzy=True)
                self.logger.debug(f"[facebook] Successfully parsed with fuzzy dateutil: {dt}")
                return dt
            except (parser.ParserError, ValueError) as e:
                self.logger.debug(f"[facebook] Fuzzy parsing failed: {str(e)}")
            
            self.logger.error(f"[facebook] Failed to parse date string with any known format: {date_str}")
            return None
            
        except Exception as e:
            self.logger.error(f"[facebook] Error parsing date '{date_str}': {str(e)}")
            if hasattr(e, '__traceback__'):
                import traceback
                self.logger.error(f"[facebook] Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
            return None
    
    def _construct_datetime_with_at(self, groups: tuple) -> Optional[datetime]:
        """Helper method to construct datetime from regex groups with 'at' in the format"""
        try:
            _, month, day, year, hour, minute, ampm, tz = groups
            year = year or datetime.now().year  # Use current year if not provided
            
            # Convert month name to number
            try:
                month_num = datetime.strptime(month, '%B').month
            except ValueError:
                month_num = datetime.strptime(month[:3], '%b').month
            
            # Convert hour to 24-hour format
            hour = int(hour)
            if ampm.upper() == 'PM' and hour < 12:
                hour += 12
            elif ampm.upper() == 'AM' and hour == 12:
                hour = 0
            
            # Construct datetime
            dt = datetime(
                year=int(year),
                month=month_num,
                day=int(day),
                hour=hour,
                minute=int(minute)
            )
            
            # Adjust for timezone if provided
            if tz in tzinfos:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone(timedelta(seconds=tzinfos[tz])))
            
            return dt
            
        except Exception as e:
            self.logger.error(f"[facebook] Error constructing datetime with 'at': {str(e)}")
            return None
    
    def _construct_datetime(self, groups: tuple, has_year: bool = False) -> Optional[datetime]:
        """Helper method to construct datetime from regex groups"""
        try:
            if has_year:
                _, month, day, year, hour, minute, ampm, tz = groups
            else:
                _, month, day, hour, minute, ampm, tz = groups
                year = datetime.now().year
            
            # Convert month name to number
            try:
                month_num = datetime.strptime(month, '%B').month
            except ValueError:
                month_num = datetime.strptime(month[:3], '%b').month
            
            # Convert hour to 24-hour format
            hour = int(hour)
            if ampm.upper() == 'PM' and hour < 12:
                hour += 12
            elif ampm.upper() == 'AM' and hour == 12:
                hour = 0
            
            # Construct datetime
            dt = datetime(
                year=int(year),
                month=month_num,
                day=int(day),
                hour=hour,
                minute=int(minute)
            )
            
            # Adjust for timezone if provided
            if tz in tzinfos:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone(timedelta(seconds=tzinfos[tz])))
            
            return dt
            
        except Exception as e:
            self.logger.error(f"[facebook] Error constructing datetime: {str(e)}")
            return None
    
    def _construct_datetime_compact(self, groups: tuple) -> Optional[datetime]:
        """Helper method to construct datetime from compact format groups"""
        try:
            month, day, year, hour, minute, ampm = groups
            hour = int(hour)
            
            if ampm and ampm.upper() == 'PM' and hour < 12:
                hour += 12
            elif ampm and ampm.upper() == 'AM' and hour == 12:
                hour = 0
            
            return datetime(
                year=int(year),
                month=int(month),
                day=int(day),
                hour=hour,
                minute=int(minute)
            )
            
        except Exception as e:
            self.logger.error(f"[facebook] Error constructing datetime from compact format: {str(e)}")
            return None
    
    def _parse_html(self, soup: BeautifulSoup) -> List[dict]:
        """Parse events from Facebook HTML with enhanced error handling and logging."""
        events = []
        event_cards = soup.find_all("div", {"role": "article"})
        self.logger.info(f"[facebook] Found {len(event_cards)} event cards")
        
        # Log HTML structure for debugging
        self.logger.debug("[facebook] Page structure analysis:")
        self.logger.debug(f"[facebook] Title: {soup.title.string if soup.title else 'No title found'}")
        self.logger.debug(f"[facebook] Meta tags: {len(soup.find_all('meta'))}")
        self.logger.debug(f"[facebook] Article divs: {len(soup.find_all('div', {'role': 'article'}))}")
        
        for i, card in enumerate(event_cards, 1):
            try:
                self.logger.debug(f"\n[facebook] Processing card {i} {'='*40}")
                event_data = {}
                
                # Extract title with multiple fallback selectors
                title = None
                title_selectors = [
                    ("span", {"class": "x4k7w5x"}),
                    ("h2", {"class": "x1heor9g"}),
                    ("div", {"class": "x1e56ztr"}),
                    ("span", {"class": "xt0psk2"})
                ]
                
                for selector in title_selectors:
                    elem = card.find(*selector)
                    if elem and elem.get_text().strip():
                        title = elem.get_text().strip()
                        self.logger.debug(f"[facebook] Found title using selector {selector}: {title}")
                        break
                
                if not title:
                    self.logger.warning(f"[facebook] No title found in card {i}, skipping")
                    continue
                
                event_data["title"] = title
                
                # Extract date with enhanced logging
                date_str = None
                date_spans = card.find_all("span", {"class": "x193iq5w"})
                for span in date_spans:
                    text = span.get_text().strip()
                    if any(day in text.lower() for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']):
                        date_str = text
                        self.logger.debug(f"[facebook] Found date string: {date_str}")
                        break
                
                if not date_str:
                    self.logger.warning(f"[facebook] No date found in card {i}, skipping")
                    continue
                
                # Parse date with enhanced error handling
                start_datetime = self._parse_date(date_str)
                if not start_datetime:
                    self.logger.warning(f"[facebook] Could not parse date '{date_str}' in card {i}, skipping")
                    continue
                
                event_data["start_datetime"] = start_datetime
                event_data["end_datetime"] = start_datetime + timedelta(hours=2)  # Default duration
                
                # Extract venue information with enhanced parsing
                venue_info = self._extract_venue_info(card, i)
                event_data.update(venue_info)
                
                # Extract image URL with multiple fallback methods
                image_url = self._extract_image_url(card, i)
                event_data["image_url"] = image_url
                
                # Extract RSVP count with enhanced parsing
                rsvp_info = self._extract_rsvp_info(card, i)
                event_data.update(rsvp_info)
                
                # Generate unique platform ID
                platform_id = self._generate_platform_id(event_data)
                event_data["platform_id"] = platform_id
                
                # Construct event URL
                event_data["url"] = f"https://www.facebook.com/events/{platform_id}"
                
                # Add platform-specific fields
                event_data.update({
                    "platform": "facebook",
                    "is_online": venue_info.get("is_online", False),
                    "description": "",  # Default empty description
                    "categories": [],  # Default empty categories
                    "organizer_name": title.split(" - ")[0] if " - " in title else title,
                    "organizer_id": platform_id
                })
                
                # Validate required fields
                if self._validate_event_data(event_data, i):
                    events.append(event_data)
                    self.logger.debug(f"[facebook] Successfully parsed event: {title}")
                    self.logger.debug(f"[facebook] Event data: {json.dumps(event_data, default=str, indent=2)}")
                
            except Exception as e:
                self.logger.error(f"[facebook] Error parsing event card {i}: {str(e)}")
                if hasattr(e, '__traceback__'):
                    import traceback
                    self.logger.error(f"[facebook] Traceback: {''.join(traceback.format_tb(e.__traceback__))}")
                continue
        
        self.logger.info(f"[facebook] Successfully parsed {len(events)} events from {len(event_cards)} cards")
        return events
    
    def _extract_venue_info(self, card: BeautifulSoup, card_index: int) -> Dict[str, Any]:
        """Extract venue information from event card with enhanced error handling."""
        try:
            venue_name = None
            is_online = False
            
            # Try multiple selectors for venue information
            venue_selectors = [
                ("span", {"class": "x193iq5w"}),
                ("div", {"class": "x6prxxf"}),
                ("span", {"class": "xt0psk2"})
            ]
            
            for selector in venue_selectors:
                elements = card.find_all(*selector)
                for elem in elements:
                    text = elem.get_text().strip()
                    
                    # Skip if it's a date string or RSVP count
                    if any(day in text.lower() for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']):
                        continue
                    if any(word in text.lower() for word in ['going', 'interested']):
                        continue
                        
                    # Check for online event indicators
                    if any(indicator in text.lower() for indicator in ['online', 'virtual', 'zoom', 'webinar']):
                        is_online = True
                        venue_name = "Online Event"
                        break
                        
                    # Look for venue indicators
                    if any(word in text for word in ['Street', 'Avenue', 'Road', 'Blvd', 'Center', 'Theatre', 'Arena']):
                        venue_name = text
                        break
            
            if not venue_name:
                self.logger.debug(f"[facebook] No venue found in card {card_index}, using default")
                venue_name = "Unknown Venue"
            
            venue_info = {
                'venue_name': venue_name,
                'venue_city': self.default_city,
                'venue_state': self.default_state,
                'venue_country': self.default_country,
                'venue_lat': 0.0,
                'venue_lon': 0.0,
                'is_online': is_online
            }
            
            # Try to geocode the venue if it's not online
            if not is_online and venue_name != "Unknown Venue":
                try:
                    venue_key = f"{venue_name}|{self.default_city}|{self.default_state}|{self.default_country}".lower()
                    address = {
                        'city': self.default_city,
                        'state': self.default_state,
                        'country': self.default_country
                    }
                    
                    coordinates = self.location_service.geocode_venue(venue_key, json.dumps(address))
                    if coordinates and len(coordinates) == 2:
                        venue_info.update({
                            'venue_lat': coordinates[0],
                            'venue_lon': coordinates[1]
                        })
                except Exception as e:
                    self.logger.error(f"[facebook] Error geocoding venue: {str(e)}")
            
            return venue_info
            
        except Exception as e:
            self.logger.error(f"[facebook] Error extracting venue info from card {card_index}: {str(e)}")
            return {
                'venue_name': "Unknown Venue",
                'venue_city': self.default_city,
                'venue_state': self.default_state,
                'venue_country': self.default_country,
                'venue_lat': 0.0,
                'venue_lon': 0.0,
                'is_online': False
            }
    
    def _extract_image_url(self, card: BeautifulSoup, card_index: int) -> Optional[str]:
        """Extract image URL from event card with multiple fallback methods."""
        try:
            # Try multiple selectors for images
            image_selectors = [
                ("img", {"class": "x1rg5ohu"}),
                ("img", {"class": "x1ey2m1c"}),
                ("img", {"class": "xt7dq6l"}),
                ("img", {"role": "img"})
            ]
            
            for selector in image_selectors:
                img = card.find(*selector)
                if img and img.get("src"):
                    return img["src"]
            
            self.logger.debug(f"[facebook] No image found in card {card_index}")
            return None
            
        except Exception as e:
            self.logger.error(f"[facebook] Error extracting image URL from card {card_index}: {str(e)}")
            return None
    
    def _extract_rsvp_info(self, card: BeautifulSoup, card_index: int) -> Dict[str, Any]:
        """Extract RSVP information from event card with enhanced parsing."""
        try:
            rsvp_count = 0
            interested_count = 0
            
            # Try multiple selectors for RSVP counts
            rsvp_selectors = [
                ("span", {"class": "x1lliihq"}),
                ("span", {"class": "xt0psk2"}),
                ("div", {"class": "x6prxxf"})
            ]
            
            for selector in rsvp_selectors:
                elements = card.find_all(*selector)
                for elem in elements:
                    text = elem.get_text().strip().lower()
                    
                    # Extract numbers from text
                    if 'going' in text:
                        try:
                            rsvp_count = int(re.search(r'\d+', text).group())
                        except (AttributeError, ValueError):
                            pass
                    elif 'interested' in text:
                        try:
                            interested_count = int(re.search(r'\d+', text).group())
                        except (AttributeError, ValueError):
                            pass
            
            return {
                'rsvp_count': rsvp_count,
                'interested_count': interested_count
            }
            
        except Exception as e:
            self.logger.error(f"[facebook] Error extracting RSVP info from card {card_index}: {str(e)}")
            return {
                'rsvp_count': 0,
                'interested_count': 0
            }
    
    def _generate_platform_id(self, event_data: Dict[str, Any]) -> str:
        """Generate a unique platform ID for the event."""
        try:
            # Create a unique string combining multiple event attributes
            unique_string = f"{event_data['title']}{event_data['start_datetime']}{event_data['venue_name']}"
            # Generate MD5 hash
            return hashlib.md5(unique_string.encode()).hexdigest()
        except Exception as e:
            self.logger.error(f"[facebook] Error generating platform ID: {str(e)}")
            # Fallback to timestamp-based ID
            return hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()
    
    def _validate_event_data(self, event_data: Dict[str, Any], card_index: int) -> bool:
        """Validate required fields in event data."""
        required_fields = ['title', 'start_datetime', 'venue_name', 'platform_id']
        
        for field in required_fields:
            if not event_data.get(field):
                self.logger.warning(f"[facebook] Missing required field '{field}' in card {card_index}")
                return False
        
        return True
    
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
    
    async def _process_venue_info(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process venue information from event data"""
        try:
            # Extract location info from event data
            location = event_data.get('location', {})
            venue_name = location.get('name', '')
            city = location.get('city', '')
            state = location.get('state', '')
            country = location.get('country', 'US')
            
            # First try to use coordinates directly from Facebook if available
            lat = location.get('latitude') or location.get('lat')
            lon = location.get('longitude') or location.get('lon') or location.get('lng')
            
            if lat is not None and lon is not None:
                try:
                    lat = float(lat)
                    lon = float(lon)
                    return {
                        'venue_name': venue_name or 'Unknown Venue',
                        'venue_lat': lat,
                        'venue_lon': lon,
                        'venue_city': city or self.default_city,
                        'venue_state': state or self.default_state,
                        'venue_country': country or self.default_country
                    }
                except (ValueError, TypeError):
                    logger.warning(f"[facebook] Invalid coordinates for venue {venue_name}: lat={lat}, lon={lon}")
            
            # If no coordinates, try to geocode the venue
            if venue_name:
                try:
                    # Create a unique string key for geocoding cache
                    venue_key = f"{venue_name}|{city or self.default_city}|{state or self.default_state}|{country or self.default_country}".lower()
                    address = {
                        'city': city or self.default_city,
                        'state': state or self.default_state,
                        'country': country or self.default_country
                    }
                    
                    import json
                    address_str = json.dumps(address)
                    coordinates = self.location_service.geocode_venue(venue_key, address_str)
                    
                    if coordinates and len(coordinates) == 2:
                        return {
                            'venue_name': venue_name,
                            'venue_lat': coordinates[0],
                            'venue_lon': coordinates[1],
                            'venue_city': city or self.default_city,
                            'venue_state': state or self.default_state,
                            'venue_country': country or self.default_country
                        }
                except Exception as e:
                    logger.error(f"[facebook] Error geocoding venue: {str(e)}")
            
            # If geocoding fails or insufficient info, use default values
            logger.warning(f"[facebook] Could not get coordinates for venue: {venue_name}, {city}, {state}")
            return {
                'venue_name': venue_name or 'Unknown Venue',
                'venue_lat': 0.0,
                'venue_lon': 0.0,
                'venue_city': city or self.default_city,
                'venue_state': state or self.default_state,
                'venue_country': country or self.default_country
            }
            
        except Exception as e:
            logger.error(f"[facebook] Error processing venue info: {str(e)}")
            return {
                'venue_name': 'Unknown Venue',
                'venue_lat': 0.0,
                'venue_lon': 0.0,
                'venue_city': self.default_city,
                'venue_state': self.default_state,
                'venue_country': self.default_country
            }
    
    async def _process_event(self, event_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single event from Facebook"""
        try:
            # Extract basic event info
            event_id = event_data.get('id', '')
            if not event_id:
                return None
                
            # Process dates
            start_time = event_data.get('start_time')
            end_time = event_data.get('end_time')
            if not start_time:
                return None
                
            # Process venue information
            venue_info = await self._process_venue_info(event_data)
            
            # Build processed event
            processed_event = {
                'platform_id': event_id,
                'title': event_data.get('name', ''),
                'description': event_data.get('description', ''),
                'start_datetime': parser.parse(start_time),
                'end_datetime': parser.parse(end_time) if end_time else None,
                'url': f"https://www.facebook.com/events/{event_id}",
                'venue_name': venue_info['venue_name'],
                'venue_lat': venue_info['venue_lat'],
                'venue_lon': venue_info['venue_lon'],
                'venue_city': venue_info['venue_city'],
                'venue_state': venue_info['venue_state'],
                'venue_country': venue_info['venue_country'],
                'platform': 'facebook',
                'is_online': event_data.get('is_online', False),
                'rsvp_count': event_data.get('attending_count', 0),
                'price_info': event_data.get('ticket_uri', None),
                'categories': event_data.get('event_categories', []),
                'image_url': event_data.get('cover', {}).get('source', None)
            }
            
            return processed_event
            
        except Exception as e:
            self.logger.error(f"[facebook] Error processing event: {str(e)}")
            return None 