from typing import Dict, Any, List
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from scrapfly import ScrapeConfig, ScrapflyClient
from app.scrapers.base import BaseScraper
import re

logger = logging.getLogger(__name__)

class MeetupScrapflyScraper(BaseScraper):
    """Scraper for Meetup using Scrapfly"""
    
    def __init__(self, scrapfly_key: str):
        """Initialize the scraper with Scrapfly API key"""
        super().__init__("meetup")
        self.scrapfly_key = scrapfly_key
    
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime]
    ) -> List[Dict[str, Any]]:
        """Scrape events from Meetup for a given location and date range"""
        try:
            # Construct search URL
            city = location['city'].replace(' ', '%20').lower()
            state = location['state'].lower()
            base_url = f"https://www.meetup.com/find/?keywords=&location={city}%2C%20{state}&source=EVENTS"
            logger.info(f"Scraping Meetup events from URL: {base_url}")
            
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
                logger.error(f"Failed to scrape Meetup: {result.error}")
                return []
            
            # Parse response
            soup = BeautifulSoup(result.content, 'html.parser')
            events = []
            
            # Extract events using modern Meetup selectors
            event_cards = soup.find_all('div', {'class': 'group-content'})
            if not event_cards:
                event_cards = soup.find_all('div', {'data-testid': 'event-card'})
            if not event_cards:
                event_cards = soup.find_all('div', {'class': 'event-card'})
            
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
                    if event_data['title'] and event_data['url']:  # Only add if we have at least title and URL
                        events.append(event_data)
                except Exception as e:
                    logger.error(f"Error extracting event data: {str(e)}")
                    continue
            
            return events
            
        except Exception as e:
            logger.error(f"Error scraping Meetup: {str(e)}")
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
            ('a', {'class': 'group-card-link'}),
            ('h3', {'class': 'group-card-title'}),
            ('div', {'class': 'group-card-name'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_description(self, card) -> str:
        """Extract event description"""
        selectors = [
            ('p', {'class': 'text--sectionText'}),
            ('div', {'class': 'eventCardHead--description'}),
            ('div', {'data-testid': 'event-description'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_datetime(self, card) -> datetime:
        """Extract event datetime"""
        try:
            selectors = [
                ('time', {'class': 'group-card-next-event'}),
                ('div', {'class': 'group-card-event-time'}),
                ('span', {'class': 'group-card-event-date'})
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
            ('p', {'class': 'venueDisplay-venue-name'}),
            ('div', {'class': 'eventCardHead--venueName'}),
            ('div', {'data-testid': 'venue-name'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_venue_address(self, card) -> str:
        """Extract venue address"""
        selectors = [
            ('p', {'class': 'venueDisplay-venue-address'}),
            ('div', {'class': 'eventCardHead--venueAddress'}),
            ('div', {'data-testid': 'venue-address'})
        ]
        
        for tag, attrs in selectors:
            elem = card.find(tag, attrs)
            if elem:
                return elem.text.strip()
        return ""
    
    def _extract_categories(self, card) -> List[str]:
        """Extract event categories"""
        cat_elem = card.find('p', {'class': 'groupCard--category'})
        return [cat_elem.text.strip()] if cat_elem else []
    
    def _extract_price_info(self, card) -> Dict[str, Any]:
        """Extract price information"""
        selectors = [
            ('span', {'class': 'eventCard--price'}),
            ('div', {'class': 'eventCardHead--price'}),
            ('div', {'data-testid': 'event-price'})
        ]
        
        for tag, attrs in selectors:
            price_elem = card.find(tag, attrs)
            if price_elem:
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
        return {}
    
    def _extract_images(self, card) -> List[str]:
        """Extract event images"""
        img_elem = card.find('img', {'class': 'groupCard--photo'})
        return [img_elem['src']] if img_elem and img_elem.get('src') else []
    
    def _extract_tags(self, card) -> List[str]:
        """Extract event tags"""
        tag_elems = card.find_all('p', {'class': 'groupCard--tag'})
        return [tag.text.strip() for tag in tag_elems]
    
    def _extract_url(self, card) -> str:
        """Extract event URL"""
        selectors = [
            ('a', {'class': 'group-card-link'}),
            ('a', {'class': 'group-card-title-link'}),
            ('a', {'class': 'group-card-no-join'})
        ]
        
        for tag, attrs in selectors:
            link = card.find(tag, attrs)
            if link and link.get('href'):
                url = link['href']
                if not url.startswith('http'):
                    url = f"https://www.meetup.com{url}"
                return url
        return ""
    
    def _extract_event_id(self, card) -> str:
        """Extract event ID from URL"""
        url = self._extract_url(card)
        return url.split('/')[-1] if url else ""
    
    def _extract_full_description(self, soup) -> str:
        """Extract full event description from details page"""
        desc_elem = soup.find('div', {'class': 'event-description'})
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