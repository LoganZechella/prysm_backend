from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from scrapfly import ScrapeConfig, ScrapflyClient
from bs4 import BeautifulSoup
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

class StubhubScrapflyScraper(BaseScraper):
    """Scraper for StubHub events using Scrapfly"""
    
    def __init__(self, scrapfly_key: str):
        """Initialize the scraper with Scrapfly API key"""
        self.platform = "stubhub"
        super().__init__(platform=self.platform)
        self.client = ScrapflyClient(key=scrapfly_key)
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Scrape StubHub events for a given location and date range"""
        events = []
        
        try:
            # Construct the StubHub search URL
            base_url = f"https://www.stubhub.com/search/events?lat={location['lat']}&lon={location['lng']}&radius=10&sort=date,asc"
            
            # Configure Scrapfly request
            config = ScrapeConfig(
                url=base_url,
                render_js=True,  # Enable JavaScript rendering
                asp=True,  # Anti scraping protection
                country="US"
            )
            
            # Make the request
            result = await self.client.async_scrape(config)
            
            if result.success:
                logger.info(f"Successfully scraped StubHub search page: {base_url}")
                # Parse the HTML response
                soup = BeautifulSoup(result.content, 'html.parser')
                logger.debug(f"HTML content: {soup.prettify()}")
                
                # Extract event data from the page
                event_elements = soup.find_all('div', {'data-testid': 'event-card'})
                logger.info(f"Found {len(event_elements)} event cards")
                
                for element in event_elements:
                    try:
                        # Log the event card HTML for debugging
                        logger.debug(f"Event card HTML: {element.prettify()}")
                        
                        # Extract basic event info
                        title = element.find('h3').text.strip()
                        logger.info(f"Found event: {title}")
                        
                        event_url = f"https://www.stubhub.com{element.find('a')['href']}"
                        logger.info(f"Event URL: {event_url}")
                        
                        # Get event details page
                        event_config = ScrapeConfig(
                            url=event_url,
                            render_js=True,
                            asp=True,
                            country="US"
                        )
                        event_result = await self.client.async_scrape(event_config)
                        
                        if event_result.success:
                            logger.info(f"Successfully scraped event details: {event_url}")
                            event_soup = BeautifulSoup(event_result.content, 'html.parser')
                            logger.debug(f"Event details HTML: {event_soup.prettify()}")
                            
                            # Extract detailed event info
                            event_data = {
                                'title': title,
                                'description': self._extract_description(event_soup),
                                'event_id': event_url.split('/')[-1],
                                'source': {
                                    'platform': self.platform,
                                    'url': event_url
                                },
                                'start_datetime': self._extract_datetime(event_soup),
                                'end_datetime': None,  # StubHub doesn't provide end times
                                'location': {
                                    'coordinates': {
                                        'lat': location['lat'],
                                        'lng': location['lng']
                                    },
                                    'venue_name': self._extract_venue_name(event_soup),
                                    'address': self._extract_venue_address(event_soup)
                                },
                                'categories': self._extract_categories(event_soup),
                                'price_info': self._extract_price_info(event_soup),
                                'images': self._extract_images(event_soup),
                                'tags': self._extract_tags(event_soup)
                            }
                            
                            events.append(event_data)
                            
                    except Exception as e:
                        logger.error(f"Error extracting StubHub event data: {str(e)}")
                        continue
            
        except Exception as e:
            logger.error(f"Error scraping StubHub events: {str(e)}")
        
        return events
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract event description"""
        desc_element = soup.find('div', {'data-testid': 'event-description'})
        return desc_element.text.strip() if desc_element else ""
    
    def _extract_datetime(self, soup: BeautifulSoup) -> datetime:
        """Extract event datetime"""
        date_element = soup.find('time', {'data-testid': 'event-date'})
        if date_element and 'datetime' in date_element.attrs:
            return datetime.fromisoformat(date_element['datetime'].replace('Z', '+00:00'))
        return None
    
    def _extract_venue_name(self, soup: BeautifulSoup) -> str:
        """Extract venue name"""
        venue_element = soup.find('h2', {'data-testid': 'venue-name'})
        return venue_element.text.strip() if venue_element else ""
    
    def _extract_venue_address(self, soup: BeautifulSoup) -> str:
        """Extract venue address"""
        address_element = soup.find('div', {'data-testid': 'venue-address'})
        return address_element.text.strip() if address_element else ""
    
    def _extract_categories(self, soup: BeautifulSoup) -> List[str]:
        """Extract event categories"""
        category_elements = soup.find_all('a', {'data-testid': 'event-category'})
        return [element.text.strip() for element in category_elements]
    
    def _extract_price_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract event price information"""
        price_element = soup.find('div', {'data-testid': 'ticket-price'})
        if not price_element:
            return {'is_free': None}  # Price info not available
            
        price_text = price_element.text.strip()
        try:
            price = float(price_text.replace('$', '').split()[0])
            return {
                'is_free': False,
                'currency': 'USD',
                'amount': price
            }
        except (ValueError, IndexError):
            return {'is_free': None}
    
    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract event images"""
        image_elements = soup.find_all('img', {'data-testid': 'event-image'})
        return [element['src'] for element in image_elements if 'src' in element.attrs]
    
    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract event tags"""
        tag_elements = soup.find_all('span', {'data-testid': 'event-tag'})
        return [element.text.strip() for element in tag_elements]
    
    async def get_event_details(self, event_url: str) -> Optional[Dict[str, Any]]:
        """Get detailed information for a specific event"""
        try:
            config = ScrapeConfig(
                url=event_url,
                render_js=True,
                asp=True,
                country="US"
            )
            
            result = await self.client.async_scrape(config)
            
            if result.success:
                soup = BeautifulSoup(result.content, 'html.parser')
                
                return {
                    'title': soup.find('h1').text.strip(),
                    'description': self._extract_description(soup),
                    'event_id': event_url.split('/')[-1],
                    'source': {
                        'platform': self.platform,
                        'url': event_url
                    },
                    'start_datetime': self._extract_datetime(soup),
                    'end_datetime': None,  # StubHub doesn't provide end times
                    'location': {
                        'venue_name': self._extract_venue_name(soup),
                        'address': self._extract_venue_address(soup)
                    },
                    'categories': self._extract_categories(soup),
                    'price_info': self._extract_price_info(soup),
                    'images': self._extract_images(soup),
                    'tags': self._extract_tags(soup)
                }
        
        except Exception as e:
            logger.error(f"Error getting StubHub event details: {str(e)}")
            return None 