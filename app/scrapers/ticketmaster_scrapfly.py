from typing import Dict, Any, List
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from scrapfly import ScrapeConfig, ScrapflyClient
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

class TicketmasterScrapflyScraper(BaseScraper):
    """Scraper for Ticketmaster using Scrapfly"""
    
    def __init__(self, scrapfly_key: str):
        """Initialize the scraper with Scrapfly API key"""
        super().__init__("ticketmaster")
        self.scrapfly_key = scrapfly_key
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Scrape events from Ticketmaster for a given location and date range"""
        try:
            # Construct search URL
            city = location['city'].replace(' ', '+').lower()
            state = location['state'].lower()
            base_url = f"https://www.ticketmaster.com/events/{state}/{city}"
            
            # Configure Scrapfly request
            config = ScrapeConfig(
                url=base_url,
                asp=True,  # Enable anti-scraping protection
                country="US",
                render_js=True  # Enable JavaScript rendering
            )
            
            # Make request
            result = await self.client.async_scrape(config)
            
            if not result.success:
                logger.error(f"Failed to scrape Ticketmaster: {result.error}")
                return []
            
            # Parse response
            soup = BeautifulSoup(result.content, 'html.parser')
            
            # Log HTML structure for debugging
            logger.debug("HTML structure:")
            logger.debug(soup.prettify())
            
            events = []
            
            # Extract events using modern Ticketmaster selectors
            event_cards = soup.find_all('div', {'class': 'search-event'})
            if not event_cards:
                event_cards = soup.find_all('div', {'class': 'event-listing'})
            if not event_cards:
                event_cards = soup.find_all('div', {'data-tid': 'event-card'})
            
            logger.info(f"Found {len(event_cards)} event cards")
            
            for card in event_cards:
                try:
                    event_data = {
                        'title': self._extract_title(card),
                        'description': self._extract_description(card),
                        'start_datetime': self._extract_datetime(card),
                        'end_datetime': None,  # Will be populated in get_event_details
                        'location': {
                            'coordinates': {
                                'lat': location['lat'],
                                'lng': location['lng']
                            },
                            'venue_name': self._extract_venue_name(card),
                            'address': self._extract_venue_address(card)
                        },
                        'categories': self._extract_categories(card),
                        'price_info': self._extract_price_info(card),
                        'images': self._extract_images(card),
                        'tags': self._extract_tags(card),
                        'source': {
                            'platform': self.platform,
                            'url': self._extract_url(card)
                        },
                        'event_id': self._extract_event_id(card)
                    }
                    events.append(event_data)
                except Exception as e:
                    logger.error(f"Error extracting event data: {str(e)}")
                    continue
            
            return events
            
        except Exception as e:
            logger.error(f"Error scraping Ticketmaster: {str(e)}")
            return []
    
    async def get_event_details(self, event_url: str) -> Dict[str, Any]:
        """Get detailed information for a specific event"""
        try:
            config = ScrapeConfig(
                url=event_url,
                asp=True,
                country="US"
            )
            
            result = await self.client.async_scrape(config)
            
            if not result.success:
                logger.error(f"Failed to get event details: {result.error}")
                return {}
            
            soup = BeautifulSoup(result.content, 'html.parser')
            
            return {
                'description': self._extract_full_description(soup),
                'end_datetime': self._extract_end_datetime(soup),
                'organizer': self._extract_organizer(soup)
            }
            
        except Exception as e:
            logger.error(f"Error getting event details: {str(e)}")
            return {}
    
    def _extract_title(self, card) -> str:
        """Extract event title"""
        selectors = [
            ('h3', {'class': 'event-name'}),
            ('h3', {'class': 'event-title'}),
            ('div', {'class': 'event-name'}),
            ('div', {'class': 'event-title'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_description(self, card) -> str:
        """Extract event description"""
        desc_elem = card.find('p', {'class': 'event-description'})
        return desc_elem.text.strip() if desc_elem else ""
    
    def _extract_datetime(self, card) -> datetime:
        """Extract event datetime"""
        try:
            date_elem = card.find('time', {'class': 'event-datetime'})
            if date_elem and date_elem.get('datetime'):
                return datetime.fromisoformat(date_elem['datetime'].replace('Z', '+00:00'))
            
            # Try alternative selectors
            date_elem = card.find('time', {'data-testid': 'event-date'})
            if date_elem and date_elem.get('datetime'):
                return datetime.fromisoformat(date_elem['datetime'].replace('Z', '+00:00'))
            
            # If no datetime found, use a default future date
            return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        except Exception as e:
            logger.error(f"Error extracting datetime: {str(e)}")
            return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    def _extract_venue_name(self, card) -> str:
        """Extract venue name"""
        selectors = [
            ('div', {'class': 'venue-name'}),
            ('div', {'class': 'event-venue'}),
            ('div', {'data-tid': 'venue-name'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_venue_address(self, card) -> str:
        """Extract venue address"""
        addr_elem = card.find('div', {'class': 'venue-address'})
        return addr_elem.text.strip() if addr_elem else ""
    
    def _extract_categories(self, card) -> List[str]:
        """Extract event categories"""
        cat_elem = card.find('div', {'class': 'event-category'})
        return [cat_elem.text.strip()] if cat_elem else []
    
    def _extract_price_info(self, card) -> Dict[str, Any]:
        """Extract price information"""
        selectors = [
            ('div', {'class': 'event-price'}),
            ('div', {'class': 'price-range'}),
            ('div', {'data-tid': 'price-range'})
        ]
        
        for tag, attrs in selectors:
            price_elem = card.find(tag, attrs)
            if price_elem:
                price_text = price_elem.text.strip()
                try:
                    # Handle price ranges (e.g., "$25 - $100")
                    if '-' in price_text:
                        min_price, max_price = price_text.replace('$', '').split('-')
                        return {
                            'is_free': False,
                            'currency': 'USD',
                            'min_price': float(min_price.strip()),
                            'max_price': float(max_price.strip())
                        }
                    else:
                        price = float(price_text.replace('$', '').split()[0])
                        return {
                            'is_free': False,
                            'currency': 'USD',
                            'min_price': price,
                            'max_price': price
                        }
                except (ValueError, IndexError):
                    pass
        return {}
    
    def _extract_images(self, card) -> List[str]:
        """Extract event images"""
        img_elem = card.find('img', {'class': 'event-image'})
        return [img_elem['src']] if img_elem and img_elem.get('src') else []
    
    def _extract_tags(self, card) -> List[str]:
        """Extract event tags"""
        tag_elems = card.find_all('div', {'class': 'event-tag'})
        return [tag.text.strip() for tag in tag_elems]
    
    def _extract_url(self, card) -> str:
        """Extract event URL"""
        selectors = [
            ('a', {'class': 'event-link'}),
            ('a', {'class': 'event-listing__link'}),
            ('a', {'data-tid': 'event-link'})
        ]
        
        for tag, attrs in selectors:
            link = card.find(tag, attrs)
            if link and link.get('href'):
                url = link['href']
                if not url.startswith('http'):
                    url = f"https://www.ticketmaster.com{url}"
                return url
        return ""
    
    def _extract_event_id(self, card) -> str:
        """Extract event ID from URL"""
        url = self._extract_url(card)
        return url.split('/')[-1] if url else ""
    
    def _extract_full_description(self, soup) -> str:
        """Extract full event description from details page"""
        desc_elem = soup.find('div', {'class': 'event-details-description'})
        return desc_elem.text.strip() if desc_elem else ""
    
    def _extract_end_datetime(self, soup) -> datetime:
        """Extract event end datetime from details page"""
        end_elem = soup.find('time', {'class': 'event-end-time'})
        if end_elem and end_elem.get('datetime'):
            return datetime.fromisoformat(end_elem['datetime'].replace('Z', '+00:00'))
        return None
    
    def _extract_organizer(self, soup) -> Dict[str, Any]:
        """Extract organizer information from details page"""
        org_elem = soup.find('div', {'class': 'event-organizer'})
        if org_elem:
            name_elem = org_elem.find('h3', {'class': 'organizer-name'})
            return {
                'name': name_elem.text.strip() if name_elem else "",
                'description': ""  # Add organizer description if available
            }
        return {} 