"""
Facebook scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from bs4 import BeautifulSoup

from app.scrapers.base import ScrapflyBaseScraper

logger = logging.getLogger(__name__)

class FacebookScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Facebook events using Scrapfly"""
    
    def __init__(self, api_key: str):
        """Initialize the Facebook scraper"""
        super().__init__(api_key)
        self.platform = "facebook"
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape events from Facebook.
        
        Args:
            location: Location parameters (city, state, lat, lng)
            date_range: Date range to search
            categories: Optional list of event categories
            
        Returns:
            List of scraped events
        """
        events = []
        page = 1
        
        while True:
            url = self._build_search_url(location, date_range, categories, page)
            try:
                soup = await self._make_request(url)
                if not soup:
                    break
                    
                new_events = self._parse_event_cards(soup)
                if not new_events:
                    break
                    
                events.extend(new_events)
                page += 1
                
            except Exception as e:
                logger.error(f"Error scraping Facebook page {page}: {str(e)}")
                break
                
        return events
        
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int
    ) -> str:
        """Build the Facebook search URL with parameters"""
        base_url = "https://www.facebook.com/events/search"
        
        # Format dates
        start_date = date_range['start'].strftime("%Y-%m-%d")
        end_date = date_range['end'].strftime("%Y-%m-%d")
        
        # Build query parameters
        params = [
            f"lat={location['lat']}",
            f"lng={location['lng']}",
            f"start_date={start_date}",
            f"end_date={end_date}",
            f"page={page}"
        ]
        
        if categories:
            params.append(f"category={','.join(categories)}")
            
        return f"{base_url}?{'&'.join(params)}"
        
    def _parse_event_cards(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse event cards from search results page"""
        events = []
        event_cards = soup.find_all("div", {"class": "event-card"})
        
        for card in event_cards:
            try:
                event = self._parse_event_card(card)
                if event:
                    events.append(event)
            except Exception as e:
                logger.error(f"Error parsing event card: {str(e)}")
                continue
                
        return events
        
    def _parse_event_card(self, card: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Parse a single event card"""
        try:
            # Extract basic event info
            title = card.find("h2").text.strip()
            description = card.find("div", {"class": "event-description"}).text.strip()
            
            # Extract date and time
            date_elem = card.find("time")
            if not date_elem or not date_elem.get("datetime"):
                return None
                
            start_time = datetime.fromisoformat(date_elem["datetime"].replace("Z", "+00:00"))
            
            # Extract location
            venue = card.find("div", {"class": "venue"})
            if not venue:
                return None
                
            location = {
                "venue_name": venue.find("span", {"class": "venue-name"}).text.strip(),
                "address": venue.find("span", {"class": "address"}).text.strip()
            }
            
            # Extract categories
            categories = []
            category_elem = card.find("div", {"class": "event-category"})
            if category_elem:
                categories = [cat.strip() for cat in category_elem.text.split(",")]
                
            # Get event URL and ID
            link = card.find("a", {"class": "event-link"})
            if not link or not link.get("href"):
                return None
                
            url = link["href"]
            event_id = url.split("/")[-2]
            
            return {
                "title": title,
                "description": description,
                "start_time": start_time,
                "location": location,
                "categories": categories,
                "source": self.platform,
                "event_id": event_id,
                "url": url
            }
            
        except Exception as e:
            logger.error(f"Error parsing event card: {str(e)}")
            return None 