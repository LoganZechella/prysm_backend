from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
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
            title_elem = card.find('h1', {'data-testid': 'event-title'})
            title = title_elem.text.strip() if title_elem else None
            
            # Extract description
            desc_elem = card.find('div', {'data-testid': 'event-description'})
            description = desc_elem.text.strip() if desc_elem else None
            
            # Extract date and time
            date_elem = card.find('div', {'data-testid': 'event-card-date'})
            start_time = None
            if date_elem:
                date_text = date_elem.text.strip()
                try:
                    start_time = self._parse_eventbrite_date(date_text)
                except:
                    logger.warning(f"Could not parse date: {date_text}")
            
            # Extract venue info
            venue_elem = card.find('h2', {'data-testid': 'venue-name'})
            addr_elem = card.find('div', {'data-testid': 'venue-address'})
            venue = {
                'name': venue_elem.text.strip() if venue_elem else None,
                'address': addr_elem.text.strip() if addr_elem else None
            }
            
            # Extract price info
            price_elem = card.find('div', {'data-testid': 'ticket-price'})
            price_info = None
            if price_elem:
                price_text = price_elem.text.strip()
                if price_text.lower() == 'free':
                    price_info = {'amount': 0.0, 'currency': 'USD'}
                else:
                    try:
                        amount = float(price_text.replace('$', '').strip())
                        price_info = {'amount': amount, 'currency': 'USD'}
                    except ValueError:
                        logger.warning(f"Could not parse price: {price_text}")
                        
            # Get event URL
            url_elem = card.find('a', {'data-testid': 'event-card-link'})
            event_url = url_elem['href'] if url_elem else None
            
            return {
                'title': title,
                'description': description,
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
        try:
            if not date_text:
                raise ValueError("Empty date text")
            
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Extract time part first
            time_part = date_text.split(" ")[-2:]  # ['7:00', 'PM']
            time = datetime.strptime(" ".join(time_part), "%I:%M %p")
            
            if "Today" in date_text:
                date = today
            elif "Tomorrow" in date_text:
                date = today + timedelta(days=1)
            else:
                # Parse full date format
                if "," in date_text:
                    # Handle "Sat, January 13, 2024 7:00 PM" format
                    date = datetime.strptime(date_text, "%a, %B %d, %Y %I:%M %p")
                    return date  # Already has correct time
                else:
                    raise ValueError(f"Unsupported date format: {date_text}")
            
            # Set the correct time for relative dates
            return date.replace(hour=time.hour, minute=time.minute)
            
        except Exception as e:
            logger.error(f"Error parsing date '{date_text}': {str(e)}")
            raise ValueError(f"Could not parse date: {date_text}")
        
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

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract event title from HTML"""
        title_elem = soup.find('h1', {'data-testid': 'event-title'})
        return title_elem.text.strip() if title_elem else None

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract event description from HTML"""
        desc_elem = soup.find('div', {'data-testid': 'event-description'})
        return desc_elem.text.strip() if desc_elem else None

    def _extract_datetime(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Extract event start datetime from HTML"""
        date_elem = soup.find('time', {'data-testid': 'event-start-date'})
        if date_elem and date_elem.get('datetime'):
            return datetime.fromisoformat(date_elem['datetime'].replace('Z', '+00:00'))
        return None

    def _extract_end_datetime(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Extract event end datetime from HTML"""
        date_elem = soup.find('time', {'data-testid': 'event-end-date'})
        if date_elem and date_elem.get('datetime'):
            return datetime.fromisoformat(date_elem['datetime'].replace('Z', '+00:00'))
        return None

    def _extract_venue_name(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract venue name from HTML"""
        venue_elem = soup.find('h2', {'data-testid': 'venue-name'})
        return venue_elem.text.strip() if venue_elem else None

    def _extract_venue_address(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract venue address from HTML"""
        addr_elem = soup.find('div', {'data-testid': 'venue-address'})
        return addr_elem.text.strip() if addr_elem else None

    def _extract_categories(self, soup: BeautifulSoup) -> List[str]:
        """Extract event categories from HTML"""
        cat_elems = soup.find_all('a', {'data-testid': 'event-category'})
        return [elem.text.strip() for elem in cat_elems if elem.text.strip()]

    def _extract_price_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract price information from HTML"""
        price_elem = soup.find('div', {'data-testid': 'ticket-price'})
        if not price_elem:
            return {'is_free': True, 'currency': 'USD', 'amount': 0.0}
        
        price_text = price_elem.text.strip()
        if price_text.lower() == 'free':
            return {'is_free': True, 'currency': 'USD', 'amount': 0.0}
        
        try:
            amount = float(price_text.replace('$', '').strip())
            return {
                'is_free': False,
                'currency': 'USD',
                'amount': amount
            }
        except ValueError:
            logger.warning(f"Could not parse price: {price_text}")
            return {'is_free': False, 'currency': 'USD', 'amount': None}

    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract event images from HTML"""
        img_elems = soup.find_all('img', {'data-testid': 'event-image'})
        return [elem['src'] for elem in img_elems if elem.get('src')]

    def _extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract event tags from HTML"""
        tag_elems = soup.find_all('span', {'data-testid': 'event-tag'})
        return [elem.text.strip() for elem in tag_elems if elem.text.strip()]

    def _extract_event_id(self, event_url: str) -> Optional[str]:
        """Extract event ID from URL"""
        try:
            # URL format: https://eventbrite.com/e/event-title-123
            return event_url.split('-')[-1]
        except Exception:
            return None

    async def get_event_details(self, event_url: str) -> Optional[Dict[str, Any]]:
        """Get detailed event information from event URL"""
        soup = await self._make_scrapfly_request(
            event_url,
            render_js=True,
            proxy_pool="residential",
            retry_attempts=4,
            wait_for_selector='h1[data-testid="event-title"]',
            rendering_wait=4000,
            asp=True
        )
        
        if not soup:
            return None
        
        return {
            'title': self._extract_title(soup),
            'description': self._extract_description(soup),
            'start_time': self._extract_datetime(soup),
            'end_time': self._extract_end_datetime(soup),
            'event_id': self._extract_event_id(event_url),
            'location': {
                'venue_name': self._extract_venue_name(soup),
                'address': self._extract_venue_address(soup)
            },
            'price_info': self._extract_price_info(soup),
            'source': {
                'platform': 'eventbrite',
                'url': event_url
            },
            'images': self._extract_images(soup),
            'categories': self._extract_categories(soup),
            'tags': self._extract_tags(soup)
        } 