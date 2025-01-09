from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
from bs4 import BeautifulSoup
from .base import ScrapflyBaseScraper
import json

logger = logging.getLogger(__name__)

class FacebookScrapflyScraper(ScrapflyBaseScraper):
    def __init__(self, scrapfly_key: str):
        super().__init__("facebook", scrapfly_key)
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Scrape events from Facebook"""
        city = location['city'].replace(' ', '%20').lower()
        state = location['state'].lower()
        url = f"https://www.facebook.com/events/search/?q={city}%2C%20{state}"
        
        # Facebook-specific headers to appear more like a real browser
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'DNT': '1'
        }
        
        # Facebook requires JavaScript rendering and often needs residential proxies
        soup = await self._make_scrapfly_request(
            url,
            render_js=True,
            proxy_pool="residential",
            retry_attempts=5,  # Facebook can be more challenging to scrape
            wait_for_selector='div[role="article"]',  # Wait for event cards to load
            rendering_wait=5000,  # Wait 5s for dynamic content
            headers=headers,
            asp=True,  # Enable anti-scraping protection
            cache=True,  # Cache results during development
            debug=True  # Enable detailed logs
        )
        
        if not soup:
            return []
            
        events = []
        event_cards = soup.find_all('div', {'role': 'article'})
        
        for card in event_cards:
            try:
                event_data = self._parse_facebook_card(card, location)
                if event_data and self._matches_filters(event_data, date_range, categories):
                    events.append(event_data)
            except Exception as e:
                logger.error(f"Error parsing Facebook event card: {str(e)}")
                continue
                
        return events
        
    def _parse_facebook_card(self, card: BeautifulSoup, location: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a Facebook event card into standardized format"""
        try:
            # Extract event data from JSON-LD if available
            json_ld = card.find('script', {'type': 'application/ld+json'})
            if json_ld:
                data = json.loads(json_ld.string)
                return self._parse_json_ld(data, location)
            
            # Fallback to HTML parsing if JSON-LD not available
            title = card.find('h2').text.strip()
            description = card.find('div', {'data-testid': 'event-description'})
            description = description.text.strip() if description else ''
            
            # Extract date and time
            date_elem = card.find('span', {'class': 'x4k7w5x'})  # Facebook's date class
            start_time = self._parse_facebook_date(date_elem.text.strip()) if date_elem else None
            
            # Extract venue info
            venue_elem = card.find('div', {'class': 'x1q0g3np'})  # Facebook's location class
            venue = {
                'name': venue_elem.find('span').text.strip() if venue_elem else '',
                'address': venue_elem.find_all('span')[1].text.strip() if venue_elem and len(venue_elem.find_all('span')) > 1 else ''
            }
            
            # Get event URL and image
            url_elem = card.find('a', {'role': 'link', 'tabindex': '0'})
            event_url = f"https://www.facebook.com{url_elem['href']}" if url_elem else None
            
            image_elem = card.find('img')
            image_url = image_elem['src'] if image_elem else None
            
            return {
                'title': title,
                'description': description,
                'start_time': start_time.isoformat() if start_time else None,
                'end_time': None,
                'location': {
                    'lat': location.get('lat', 0),
                    'lng': location.get('lng', 0)
                },
                'venue': venue,
                'price_info': {
                    'min_price': 0,  # Facebook events often don't show price in search
                    'max_price': 0,
                    'currency': 'USD'
                },
                'source': 'facebook',
                'source_id': event_url.split('/')[-2] if event_url else None,
                'url': event_url,
                'categories': [],  # Would need to fetch event details to get categories
                'tags': [],
                'image_url': image_url,
                'view_count': 0,
                'like_count': 0,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error parsing Facebook card: {str(e)}")
            return None
            
    def _parse_json_ld(self, data: Dict[str, Any], location: Dict[str, Any]) -> Dict[str, Any]:
        """Parse event data from JSON-LD format"""
        try:
            return {
                'title': data.get('name', ''),
                'description': data.get('description', ''),
                'start_time': data.get('startDate'),
                'end_time': data.get('endDate'),
                'location': {
                    'lat': location.get('lat', 0),
                    'lng': location.get('lng', 0)
                },
                'venue': {
                    'name': data.get('location', {}).get('name', ''),
                    'address': data.get('location', {}).get('address', {}).get('streetAddress', '')
                },
                'price_info': {
                    'min_price': 0,
                    'max_price': 0,
                    'currency': 'USD'
                },
                'source': 'facebook',
                'source_id': data.get('identifier', ''),
                'url': data.get('url', ''),
                'categories': [],
                'tags': [],
                'image_url': data.get('image', ''),
                'view_count': 0,
                'like_count': 0,
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error parsing JSON-LD data: {str(e)}")
            return None
            
    def _parse_facebook_date(self, date_text: str) -> Optional[datetime]:
        """Parse Facebook's date formats"""
        try:
            # Facebook uses various date formats, we'll handle common ones
            formats = [
                '%b %d, %Y at %I:%M %p',  # "Mar 15, 2024 at 7:00 PM"
                '%B %d, %Y at %I:%M %p',  # "March 15, 2024 at 7:00 PM"
                '%A, %B %d at %I:%M %p',  # "Friday, March 15 at 7:00 PM"
                'Today at %I:%M %p',       # "Today at 7:00 PM"
                'Tomorrow at %I:%M %p'     # "Tomorrow at 7:00 PM"
            ]
            
            # Handle relative dates
            if 'Today at' in date_text:
                base_date = datetime.now()
                time_str = date_text.split('at')[1].strip()
                return datetime.strptime(f"{base_date.strftime('%Y-%m-%d')} {time_str}", '%Y-%m-%d %I:%M %p')
                
            if 'Tomorrow at' in date_text:
                base_date = datetime.now()
                time_str = date_text.split('at')[1].strip()
                next_day = base_date + timedelta(days=1)
                return datetime.strptime(f"{next_day.strftime('%Y-%m-%d')} {time_str}", '%Y-%m-%d %I:%M %p')
            
            # Try each format
            for fmt in formats:
                try:
                    return datetime.strptime(date_text, fmt)
                except ValueError:
                    continue
                    
            return None
            
        except Exception as e:
            logger.error(f"Error parsing Facebook date '{date_text}': {str(e)}")
            return None
            
    def _matches_filters(
        self,
        event: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]]
    ) -> bool:
        """Check if event matches the date range and category filters"""
        # Check date range
        if event['start_time']:
            event_date = datetime.fromisoformat(event['start_time'].replace('Z', '+00:00'))
            if event_date < date_range['start'] or event_date > date_range['end']:
                return False
                
        # Check categories if specified
        if categories and event['categories']:
            if not any(cat in event['categories'] for cat in categories):
                return False
                
        return True 