from typing import Dict, Any, List
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from scrapfly import ScrapeConfig, ScrapflyClient
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

class StubhubScrapflyScraper(BaseScraper):
    """Scraper for StubHub using Scrapfly"""
    
    def __init__(self, scrapfly_key: str):
        """Initialize the scraper with Scrapfly API key"""
        super().__init__("stubhub")
        self.scrapfly_key = scrapfly_key
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Scrape events from StubHub for a given location and date range"""
        try:
            # Construct search URL
            city = location['city'].replace(' ', '+').lower()
            state = location['state'].lower()
            base_url = f"https://www.stubhub.com/find/s/?q={city}+{state}&sort=date%2Casc&dateLocal={date_range['start'].strftime('%Y-%m-%d')}"
            
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
                logger.error(f"Failed to scrape StubHub: {result.error}")
                return []
            
            # Parse response
            soup = BeautifulSoup(result.content, 'html.parser')
            events = []
            
            # Extract events
            event_cards = soup.find_all('div', {'class': 'EventItem'})
            
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
            logger.error(f"Error scraping StubHub: {str(e)}")
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
        title_elem = card.find('h3', {'class': 'EventItem__Title'})
        return title_elem.text.strip() if title_elem else ""
    
    def _extract_description(self, card) -> str:
        """Extract event description"""
        desc_elem = card.find('div', {'class': 'EventItem__Description'})
        return desc_elem.text.strip() if desc_elem else ""
    
    def _extract_datetime(self, card) -> datetime:
        """Extract event datetime"""
        try:
            date_elem = card.find('time', {'class': 'EventItem__DateTime'})
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
        venue_elem = card.find('div', {'class': 'EventItem__Venue'})
        return venue_elem.text.strip() if venue_elem else ""
    
    def _extract_venue_address(self, card) -> str:
        """Extract venue address"""
        addr_elem = card.find('div', {'class': 'EventItem__Address'})
        return addr_elem.text.strip() if addr_elem else ""
    
    def _extract_categories(self, card) -> List[str]:
        """Extract event categories"""
        cat_elem = card.find('div', {'class': 'EventItem__Category'})
        return [cat_elem.text.strip()] if cat_elem else []
    
    def _extract_price_info(self, card) -> Dict[str, Any]:
        """Extract price information"""
        price_elem = card.find('div', {'class': 'EventItem__Price'})
        if price_elem:
            price_text = price_elem.text.strip()
            try:
                price = float(price_text.replace('$', '').split()[0])
                return {
                    'is_free': False,
                    'currency': 'USD',
                    'amount': price
                }
            except (ValueError, IndexError):
                pass
        return {}
    
    def _extract_images(self, card) -> List[str]:
        """Extract event images"""
        img_elem = card.find('img', {'class': 'EventItem__Image'})
        return [img_elem['src']] if img_elem and img_elem.get('src') else []
    
    def _extract_tags(self, card) -> List[str]:
        """Extract event tags"""
        tag_elems = card.find_all('div', {'class': 'EventItem__Tag'})
        return [tag.text.strip() for tag in tag_elems]
    
    def _extract_url(self, card) -> str:
        """Extract event URL"""
        link = card.find('a', {'class': 'EventItem__Link'})
        return link['href'] if link else ""
    
    def _extract_event_id(self, card) -> str:
        """Extract event ID from URL"""
        url = self._extract_url(card)
        return url.split('/')[-1] if url else ""
    
    def _extract_full_description(self, soup) -> str:
        """Extract full event description from details page"""
        desc_elem = soup.find('div', {'class': 'EventDetails__Description'})
        return desc_elem.text.strip() if desc_elem else ""
    
    def _extract_end_datetime(self, soup) -> datetime:
        """Extract event end datetime from details page"""
        end_elem = soup.find('time', {'class': 'EventDetails__EndTime'})
        if end_elem and end_elem.get('datetime'):
            return datetime.fromisoformat(end_elem['datetime'].replace('Z', '+00:00'))
        return None
    
    def _extract_organizer(self, soup) -> Dict[str, Any]:
        """Extract organizer information from details page"""
        org_elem = soup.find('div', {'class': 'EventDetails__Organizer'})
        if org_elem:
            name_elem = org_elem.find('h3', {'class': 'EventDetails__OrganizerName'})
            return {
                'name': name_elem.text.strip() if name_elem else "",
                'description': ""  # Add organizer description if available
            }
        return {} 