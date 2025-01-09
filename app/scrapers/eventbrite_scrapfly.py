from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from .base import ScrapflyBaseScraper

logger = logging.getLogger(__name__)

class EventbriteScrapflyScraper(ScrapflyBaseScraper):
    def __init__(self, scrapfly_key: str):
        super().__init__("eventbrite", scrapfly_key)
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Scrape events from Eventbrite"""
        city = location['city'].replace(' ', '-').lower()
        state = location['state'].replace(' ', '-').lower()
        url = f"https://www.eventbrite.com/d/{state}--{city}/events/"
        
        # Eventbrite-specific headers
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        
        # Eventbrite uses heavy client-side rendering
        soup = await self._make_scrapfly_request(
            url,
            render_js=True,
            proxy_pool="residential",  # Eventbrite can be sensitive to proxy quality
            retry_attempts=4,
            wait_for_selector='div[data-testid="event-card"]',  # Wait for event cards
            rendering_wait=4000,  # Wait 4s for dynamic content
            headers=headers,
            asp=True,
            cache=True,
            debug=True
        )
        
        if not soup:
            return []
            
        events = []
        event_cards = soup.find_all('div', {'data-testid': 'event-card'})
        
        for card in event_cards:
            try:
                event_data = self._parse_eventbrite_card(card, location)
                if event_data and self._matches_filters(event_data, date_range, categories):
                    events.append(event_data)
            except Exception as e:
                logger.error(f"Error parsing Eventbrite event card: {str(e)}")
                continue
                
        return events
        
    def _parse_eventbrite_card(self, card: BeautifulSoup, location: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse an Eventbrite event card into standardized event data"""
        try:
            # Extract basic event info
            title_elem = card.find('div', {'data-testid': 'event-card-title'})
            title = title_elem.text.strip() if title_elem else None
            
            # Extract date and time
            date_elem = card.find('div', {'data-testid': 'event-card-date'})
            start_time = None
            if date_elem:
                date_text = date_elem.text.strip()
                try:
                    # Eventbrite dates need special parsing based on format
                    start_time = self._parse_eventbrite_date(date_text)
                except:
                    logger.warning(f"Could not parse date: {date_text}")
            
            # Extract venue info
            venue_elem = card.find('div', {'data-testid': 'event-card-venue'})
            venue = {
                'name': venue_elem.find('div').text.strip() if venue_elem and venue_elem.find('div') else None,
                'address': venue_elem.find_all('div')[1].text.strip() if venue_elem and len(venue_elem.find_all('div')) > 1 else None
            }
            
            # Extract price info
            price_elem = card.find('div', {'data-testid': 'event-card-price'})
            price_info = None
            if price_elem:
                price_text = price_elem.text.strip()
                if price_text.lower() != 'free':
                    try:
                        # Handle price ranges
                        if ' - ' in price_text:
                            min_str, max_str = price_text.split(' - ')
                            min_price = float(min_str.replace('$', '').strip())
                            max_price = float(max_str.replace('$', '').strip())
                        else:
                            min_price = max_price = float(price_text.replace('$', '').strip())
                            
                        price_info = {
                            'min_price': min_price,
                            'max_price': max_price,
                            'currency': 'USD'
                        }
                    except ValueError:
                        logger.warning(f"Could not parse price: {price_text}")
                        
            # Get event URL
            url_elem = card.find('a', {'data-testid': 'event-card-link'})
            event_url = url_elem['href'] if url_elem else None
            
            return {
                'title': title,
                'description': None,  # Need to fetch event details for description
                'start_time': start_time.isoformat() if start_time else None,
                'end_time': None,  # Need to fetch event details for end time
                'venue': venue,
                'price_info': price_info,
                'source': 'eventbrite',
                'url': event_url
            }
            
        except Exception as e:
            logger.error(f"Error parsing Eventbrite card details: {str(e)}")
            return None
            
    def _parse_eventbrite_date(self, date_text: str) -> datetime:
        """Parse Eventbrite date formats into datetime object"""
        # Implementation depends on Eventbrite's date format
        # This is a placeholder - actual implementation would handle various formats
        raise NotImplementedError("Date parsing not implemented")
        
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