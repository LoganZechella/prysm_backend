from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from .base import ScrapflyBaseScraper

logger = logging.getLogger(__name__)

class MeetupScrapflyScraper(ScrapflyBaseScraper):
    def __init__(self, scrapfly_key: str):
        super().__init__("meetup", scrapfly_key)
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Scrape events from Meetup"""
        city = location['city'].replace(' ', '%20').lower()
        state = location['state'].lower()
        url = f"https://www.meetup.com/find/?keywords=&location={city}%2C%20{state}&source=EVENTS"
        
        # Meetup-specific headers to mimic browser behavior
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'TE': 'trailers'
        }
        
        # Meetup uses client-side rendering, so we need JS rendering
        soup = await self._make_scrapfly_request(
            url,
            render_js=True,
            proxy_pool="residential",  # Meetup can be sensitive to proxy quality
            retry_attempts=4,
            wait_for_selector='div[data-element-name="eventCard"]',  # Wait for event cards
            rendering_wait=3000,  # Wait 3s for dynamic content
            headers=headers,
            asp=True,
            cache=True,
            debug=True
        )
        
        if not soup:
            return []
            
        events = []
        event_cards = soup.find_all('div', {'data-element-name': 'eventCard'})
        
        for card in event_cards:
            try:
                event_data = self._parse_meetup_card(card, location)
                if event_data and self._matches_filters(event_data, date_range, categories):
                    events.append(event_data)
            except Exception as e:
                logger.error(f"Error parsing Meetup event card: {str(e)}")
                continue
                
        return events
        
    def _parse_meetup_card(self, card: BeautifulSoup, location: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a Meetup event card into standardized event data"""
        try:
            # Extract basic event info
            title = card.find('h2').text.strip()
            description = card.find('p', {'data-element-name': 'eventDescription'}).text.strip()
            
            # Extract date and time
            datetime_elem = card.find('time')
            start_time = datetime_elem.get('datetime') if datetime_elem else None
            
            # Extract venue info
            venue_elem = card.find('div', {'data-element-name': 'venueDisplay'})
            venue = {
                'name': venue_elem.find('p').text.strip() if venue_elem else None,
                'address': venue_elem.find_all('p')[1].text.strip() if venue_elem and len(venue_elem.find_all('p')) > 1 else None
            }
            
            # Extract price info
            price_elem = card.find('span', {'data-element-name': 'price'})
            price_info = None
            if price_elem:
                price_text = price_elem.text.strip()
                if price_text != 'Free':
                    try:
                        price = float(price_text.replace('$', '').replace(',', ''))
                        price_info = {'min_price': price, 'max_price': price, 'currency': 'USD'}
                    except ValueError:
                        price_info = None
                        
            return {
                'title': title,
                'description': description,
                'start_time': start_time,
                'end_time': None,  # Meetup doesn't always show end time in cards
                'venue': venue,
                'price_info': price_info,
                'source': 'meetup',
                'url': card.find('a')['href'] if card.find('a') else None
            }
            
        except Exception as e:
            logger.error(f"Error parsing Meetup card details: {str(e)}")
            return None
            
    def _matches_filters(
        self,
        event: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> bool:
        """Check if event matches the given filters"""
        try:
            # Check date range
            if event['start_time']:
                event_date = datetime.fromisoformat(event['start_time'].replace('Z', '+00:00'))
                if event_date < date_range['start'] or event_date > date_range['end']:
                    return False
                    
            # Check categories if specified
            if categories and event.get('categories'):
                if not any(cat.lower() in [c.lower() for c in event['categories']] for cat in categories):
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error checking filters: {str(e)}")
            return False 