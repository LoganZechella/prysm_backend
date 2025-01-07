from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import re
import asyncio
from bs4 import BeautifulSoup
from scrapfly import ScrapeConfig, ScrapflyClient
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

class EventbriteScrapflyScraper(BaseScraper):
    """Scraper for Eventbrite using Scrapfly"""
    
    def __init__(self, scrapfly_key: str):
        """Initialize the scraper with Scrapfly API key"""
        super().__init__("eventbrite")
        self.scrapfly_key = scrapfly_key
        self.retry_count = 3
        self.retry_delay = 5  # seconds
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Scrape events from Eventbrite for a given location and date range"""
        try:
            # Construct search URL with date range
            city = location['city'].replace(' ', '-').lower()
            state = location['state'].lower()
            start_date = date_range['start'].strftime('%Y-%m-%d')
            end_date = date_range['end'].strftime('%Y-%m-%d')
            base_url = f"https://www.eventbrite.com/d/{state}--{city}/all-events/?start_date={start_date}&end_date={end_date}&page=1"
            logger.info(f"Scraping Eventbrite events from URL: {base_url}")
            
            # Configure Scrapfly request with additional options
            config = ScrapeConfig(
                url=base_url,
                asp=True,  # Enable anti-scraping protection
                country="US",
                render_js=True,  # Enable JavaScript rendering
                proxy_pool="public_residential_pool",  # Use residential proxies
                wait_for_selector="div[data-testid='search-results-feed'], div[data-testid='event-card']",  # Wait for events to load
                rendering_wait=5000  # Wait up to 5 seconds for rendering
            )
            
            # Make request with retries
            for attempt in range(self.retry_count):
                try:
                    html_content = await self.fetch_with_retry(config)
                    if html_content:
                        break
                except Exception as e:
                    if attempt == self.retry_count - 1:  # Last attempt
                        logger.error(f"Failed to fetch Eventbrite events after {self.retry_count} attempts: {str(e)}")
                        return []
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {self.retry_delay} seconds...")
                    await asyncio.sleep(self.retry_delay)
            
            if not html_content:
                logger.error("Failed to fetch Eventbrite events")
                return []
            
            # Parse HTML content
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Log the HTML structure for debugging
            logger.debug("HTML structure:")
            logger.debug(soup.prettify()[:2000])  # Log first 2000 chars for better context
            
            # Try multiple selectors to find events
            event_cards = soup.find_all('div', {'data-testid': 'event-card'})
            if not event_cards:
                event_cards = soup.find_all('article', {'data-testid': 'event-card'})
            if not event_cards:
                event_cards = soup.find_all('div', {'class': 'search-event-card-wrapper'})
            
            if not event_cards:
                logger.warning("Could not find any event cards")
                logger.debug("Available data-testid attributes:")
                for elem in soup.find_all(attrs={'data-testid': True}):
                    logger.debug(f"Found element with data-testid: {elem['data-testid']}")
                return []
            
            logger.info(f"Found {len(event_cards)} event cards")
            
            events = []
            for card in event_cards:
                try:
                    # Extract event data
                    event_data = await self._extract_event_data(card, location)
                    if event_data:
                        events.append(event_data)
                    
                except Exception as e:
                    logger.error(f"Error extracting event details: {str(e)}")
                    continue
            
            logger.info(f"Successfully extracted {len(events)} events")
            return events
            
        except Exception as e:
            logger.error(f"Error scraping Eventbrite: {str(e)}")
            return []
    
    async def _extract_event_data(self, card: BeautifulSoup, location: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract all event data from a card with error handling"""
        try:
            title = self._extract_title(card)
            if not title:  # Skip if no title found
                return None
                
            url = self._extract_url(card)
            if not url:  # Skip if no URL found
                return None
                
            return {
                'title': title,
                'description': self._extract_description(card),
                'start_time': self._extract_datetime(card),
                'end_time': None,  # Will be populated in get_event_details
                'location': {
                    'coordinates': {
                        'lat': location.get('lat', 0),
                        'lng': location.get('lng', 0)
                    },
                    'venue_name': self._extract_venue_name(card),
                    'address': self._extract_venue_address(card)
                },
                'categories': self._extract_categories(card),
                'price_info': self._extract_price_info(card),
                'image_url': self._extract_images(card)[0] if self._extract_images(card) else None,
                'tags': self._extract_tags(card),
                'source': self.platform,
                'source_id': self._extract_event_id(card),
                'url': url,
                'venue': {
                    'name': self._extract_venue_name(card),
                    'address': self._extract_venue_address(card)
                }
            }
        except Exception as e:
            logger.error(f"Error in _extract_event_data: {str(e)}")
            return None
    
    def _extract_title(self, card) -> str:
        """Extract event title"""
        selectors = [
            ('h2', {'data-testid': 'event-card-title'}),
            ('h2', {'class': 'event-card__title'}),
            ('div', {'class': 'event-card__formatted-name'}),
            ('h3', {'class': 'event-card__title'}),
            ('a', {'class': 'event-card__title-link'}),
            ('div', {'class': 'event-card__title-content'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_description(self, card) -> str:
        """Extract event description"""
        selectors = [
            ('p', {'data-testid': 'event-card-description'}),
            ('div', {'class': 'event-card__description'}),
            ('p', {'class': 'event-card__description'}),
            ('div', {'class': 'event-card__sub-content'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_datetime(self, card) -> Optional[datetime]:
        """Extract event datetime with multiple format support"""
        try:
            selectors = [
                ('time', {'data-testid': 'event-card-date'}),
                ('div', {'class': 'event-card__date'}),
                ('div', {'class': 'event-card__time'}),
                ('div', {'data-subcontent': 'true'})
            ]
            
            for tag, attrs in selectors:
                elem = card.find(tag, attrs)
                if elem:
                    if elem.get('datetime'):
                        return datetime.fromisoformat(elem['datetime'].replace('Z', '+00:00'))
                    
                    # Try parsing text content if datetime attribute not found
                    date_text = elem.text.strip()
                    try:
                        # Add more date format parsing as needed
                        return datetime.strptime(date_text, '%a, %b %d, %Y %I:%M %p')
                    except ValueError:
                        try:
                            return datetime.strptime(date_text, '%B %d, %Y %I:%M %p')
                        except ValueError:
                            continue
            
            return datetime.now()
        except Exception as e:
            logger.error(f"Error extracting datetime: {str(e)}")
            return datetime.now()
    
    def _extract_venue_name(self, card) -> str:
        """Extract venue name"""
        selectors = [
            ('p', {'data-testid': 'event-card-venue'}),
            ('div', {'class': 'event-card__venue'}),
            ('div', {'class': 'event-card__venue-name'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_venue_address(self, card) -> str:
        """Extract venue address"""
        selectors = [
            ('p', {'data-testid': 'event-card-location'}),
            ('div', {'class': 'event-card__location'}),
            ('div', {'class': 'event-card__venue-address'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_categories(self, card) -> List[str]:
        """Extract event categories"""
        cat_elem = card.find('p', {'class': 'eds-event-card__category'})
        return [cat_elem.text.strip()] if cat_elem else []
    
    def _extract_price_info(self, card) -> Dict[str, Any]:
        """Extract price information with support for ranges"""
        price_elem = card.find('div', {'data-testid': 'event-card-price'})
        if not price_elem:
            price_elem = card.find('div', {'class': 'eds-event-card-content__price'})
        
        if not price_elem:
            return {'is_free': True, 'currency': 'USD', 'min_price': 0, 'max_price': 0}
            
        price_text = price_elem.text.strip().lower()
        
        if 'free' in price_text:
            return {'is_free': True, 'currency': 'USD', 'min_price': 0, 'max_price': 0}
            
        # Extract all numbers from the price text
        numbers = re.findall(r'\d+(?:\.\d+)?', price_text)
        if not numbers:
            return {'is_free': False, 'currency': 'USD'}
            
        numbers = [float(n) for n in numbers]
        return {
            'is_free': False,
            'currency': 'USD',
            'min_price': min(numbers),
            'max_price': max(numbers)
        }
    
    def _extract_images(self, card) -> List[str]:
        """Extract event images"""
        img_elem = card.find('img', {'class': 'eds-event-card__image'})
        return [img_elem['src']] if img_elem and img_elem.get('src') else []
    
    def _extract_tags(self, card) -> List[str]:
        """Extract event tags"""
        tag_elems = card.find_all('p', {'class': 'eds-event-card__tag'})
        return [tag.text.strip() for tag in tag_elems]
    
    def _extract_url(self, card) -> str:
        """Extract event URL"""
        selectors = [
            ('a', {'class': 'eds-event-card-content__action-link'}),
            ('a', {'class': 'eds-event-card__formatted-name--link'}),
            ('a', {'class': 'eds-event-card-content__action'}),
            ('a', {'class': 'eds-media-card-content__action-link'}),
            ('a', {'class': 'eds-media-card-content__link'})
        ]
        
        for tag, attrs in selectors:
            link = card.find(tag, attrs)
            if link and link.get('href'):
                url = link['href']
                if not url.startswith('http'):
                    url = f"https://www.eventbrite.com{url}"
                return url
        
        # Try finding any link that contains event URL patterns
        for link in card.find_all('a'):
            href = link.get('href', '')
            if '/e/' in href or '/events/' in href:
                if not href.startswith('http'):
                    href = f"https://www.eventbrite.com{href}"
                return href
        return ""
    
    def _extract_event_id(self, card) -> str:
        """Extract event ID from URL"""
        url = self._extract_url(card)
        return url.split('-')[-1] if url else ""
    
    def _extract_full_description(self, soup) -> str:
        """Extract full event description from details page"""
        desc_elem = soup.find('div', {'class': 'eds-event__description'})
        return desc_elem.text.strip() if desc_elem else ""
    
    def _extract_end_datetime(self, soup) -> datetime:
        """Extract event end datetime from details page"""
        end_elem = soup.find('time', {'class': 'eds-event__end-date'})
        if end_elem and end_elem.get('datetime'):
            return datetime.fromisoformat(end_elem['datetime'].replace('Z', '+00:00'))
        return None
    
    def _extract_organizer(self, soup) -> Dict[str, Any]:
        """Extract organizer information from details page"""
        org_elem = soup.find('div', {'class': 'eds-event__organizer'})
        if org_elem:
            name_elem = org_elem.find('h3', {'class': 'eds-event__organizer-name'})
            return {
                'name': name_elem.text.strip() if name_elem else "",
                'description': ""  # Add organizer description if available
            }
        return {} 
    
    async def get_event_details(self, event_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific event"""
        try:
            config = ScrapeConfig(
                url=event_url,
                asp=True,
                country="US",
                render_js=True
            )
            
            result = await self.client.async_scrape(config)
            
            if not result.success:
                logger.error(f"Failed to get event details: {result.error}")
                return {}
            
            soup = BeautifulSoup(result.content, 'html.parser')
            
            return {
                'description': self._extract_full_description(soup),
                'end_datetime': self._extract_end_datetime(soup),
                'organizer': self._extract_organizer(soup),
                'venue': {
                    'name': self._extract_venue_name(soup),
                    'address': self._extract_venue_address(soup)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting event details: {str(e)}")
            return {} 