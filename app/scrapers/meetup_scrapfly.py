from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from scrapfly import ScrapeConfig, ScrapflyClient
from bs4 import BeautifulSoup
from app.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

class MeetupScrapflyScraper(BaseScraper):
    """Scraper for Meetup events using Scrapfly"""
    
    def __init__(self, scrapfly_key: str):
        """Initialize the scraper with Scrapfly API key"""
        self.platform = "meetup"
        super().__init__(platform=self.platform)
        self.client = ScrapflyClient(key=scrapfly_key)
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Scrape Meetup events for a given location and date range"""
        events = []
        
        try:
            # Construct the Meetup search URL
            base_url = f"https://www.meetup.com/find/events/?lat={location['lat']}&lon={location['lng']}&radius=10&eventType=inPerson"
            
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
                logger.info(f"Successfully scraped Meetup search page: {base_url}")
                # Parse the HTML response
                soup = BeautifulSoup(result.content, 'html.parser')
                logger.debug(f"HTML content: {soup.prettify()}")
                
                # Extract event data from the page
                event_elements = soup.find_all('div', {'data-element-name': 'eventCard'})
                logger.info(f"Found {len(event_elements)} event cards")
                
                for element in event_elements:
                    try:
                        # Log the event card HTML for debugging
                        logger.debug(f"Event card HTML: {element.prettify()}")
                        
                        # Extract basic event info
                        title_element = element.find('h2', {'data-element-name': 'eventCardTitle'})
                        title = title_element.text.strip() if title_element else ""
                        logger.info(f"Found event: {title}")
                        
                        description_element = element.find('p', {'data-element-name': 'eventCardDescription'})
                        description = description_element.text.strip() if description_element else ""
                        
                        link_element = element.find('a', {'data-element-name': 'eventCardLink'})
                        event_url = link_element['href'] if link_element else None
                        logger.info(f"Event URL: {event_url}")
                        
                        if not event_url:
                            logger.warning("No event URL found, skipping")
                            continue
                        
                        # Get event details page
                        event_config = ScrapeConfig(
                            url=event_url,
                            render_js=True,
                            asp=True,
                            country="US"
                        )
                        event_result = await self.client.async_scrape(event_config)
                        
                        if event_result.success:
                            event_soup = BeautifulSoup(event_result.content, 'html.parser')
                            
                            # Extract date and time
                            date_element = event_soup.find('time', {'data-element-name': 'eventDateTime'})
                            start_time = date_element['datetime'] if date_element else None
                            
                            if not start_time:
                                continue
                            
                            # Extract venue information
                            venue_element = event_soup.find('div', {'data-element-name': 'venueDisplay'})
                            venue_name = ""
                            venue_address = ""
                            
                            if venue_element:
                                name_element = venue_element.find('div', {'data-element-name': 'venueName'})
                                venue_name = name_element.text.strip() if name_element else ""
                                
                                address_element = venue_element.find('div', {'data-element-name': 'venueAddress'})
                                venue_address = address_element.text.strip() if address_element else ""
                            
                            # Extract event data
                            event_data = {
                                'title': title,
                                'description': description,
                                'event_id': event_url.split('/')[-1],
                                'source': {
                                    'platform': self.platform,
                                    'url': event_url
                                },
                                'start_datetime': self._parse_datetime(start_time),
                                'end_datetime': None,  # Meetup often doesn't show end times
                                'location': {
                                    'coordinates': {
                                        'lat': location['lat'],
                                        'lng': location['lng']
                                    },
                                    'venue_name': venue_name,
                                    'address': venue_address
                                },
                                'categories': self._extract_categories(event_soup),
                                'price_info': self._extract_price_info(event_soup),
                                'images': self._extract_images(event_soup),
                                'tags': self._extract_tags(event_soup)
                            }
                            
                            events.append(event_data)
                            
                    except Exception as e:
                        logger.error(f"Error extracting Meetup event data: {str(e)}")
                        continue
            
        except Exception as e:
            logger.error(f"Error scraping Meetup events: {str(e)}")
        
        return events
    
    def _parse_datetime(self, datetime_str: str) -> datetime:
        """Parse datetime string from Meetup"""
        return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
    
    def _extract_categories(self, soup: BeautifulSoup) -> List[str]:
        """Extract event categories"""
        category_elements = soup.find_all('a', {'data-element-name': 'groupCategory'})
        return [element.text.strip() for element in category_elements]
    
    def _extract_price_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract event price information"""
        price_element = soup.find('div', {'data-element-name': 'eventPrice'})
        if price_element and 'Free' in price_element.text:
            return {'is_free': True}
        
        price_text = price_element.text.strip() if price_element else ''
        if price_text:
            try:
                price = float(price_text.replace('$', '').split()[0])
                return {
                    'is_free': False,
                    'currency': 'USD',
                    'amount': price
                }
            except (ValueError, IndexError):
                pass
        
        return {'is_free': None}  # Price info not available
    
    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract event images"""
        image_elements = soup.find_all('img', {'data-element-name': 'eventImage'})
        return [element['src'] for element in image_elements if 'src' in element.attrs]
    
    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract event tags"""
        tag_elements = soup.find_all('a', {'data-element-name': 'groupTopic'})
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
                    'description': soup.find('div', {'data-testid': 'event-description'}).text.strip(),
                    'event_id': event_url.split('/')[-1],
                    'source': {
                        'platform': self.platform,
                        'url': event_url
                    },
                    'start_datetime': self._parse_datetime(
                        soup.find('time', {'data-testid': 'event-start-time'})['datetime']
                    ),
                    'end_datetime': self._parse_datetime(
                        soup.find('time', {'data-testid': 'event-end-time'})['datetime']
                    ),
                    'location': {
                        'venue_name': soup.find('p', {'data-testid': 'venue-name'}).text.strip(),
                        'address': soup.find('p', {'data-testid': 'venue-address'}).text.strip()
                    },
                    'categories': self._extract_categories(soup),
                    'price_info': self._extract_price_info(soup),
                    'images': self._extract_images(soup),
                    'tags': self._extract_tags(soup)
                }
        
        except Exception as e:
            logger.error(f"Error getting Meetup event details: {str(e)}")
            return None 