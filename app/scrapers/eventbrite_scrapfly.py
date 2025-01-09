"""
Eventbrite scraper implementation using Scrapfly.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
from bs4 import BeautifulSoup

from app.scrapers.base import ScrapflyBaseScraper

logger = logging.getLogger(__name__)

class EventbriteScrapflyScraper(ScrapflyBaseScraper):
    """Scraper for Eventbrite events using Scrapfly"""
    
    def __init__(self, api_key: str):
        """Initialize the Eventbrite scraper"""
        super().__init__(api_key)
        self.platform = "eventbrite"
        
    async def scrape_events(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Scrape events from Eventbrite.
        
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
                logger.error(f"Error scraping Eventbrite page {page}: {str(e)}")
                break
                
        return events
        
    def _build_search_url(
        self,
        location: Dict[str, Any],
        date_range: Dict[str, datetime],
        categories: Optional[List[str]],
        page: int
    ) -> str:
        """Build the Eventbrite search URL with parameters"""
        base_url = "https://www.eventbrite.com/d"
        
        # Format location
        location_str = f"{location['city']}/{location['state']}"
        
        # Format dates
        start_date = date_range['start'].strftime("%Y-%m-%d")
        end_date = date_range['end'].strftime("%Y-%m-%d")
        
        # Build query parameters
        params = [
            f"--location-address={location_str}",
            f"--date={start_date}--{end_date}",
            f"--page={page}"
        ]
        
        if categories:
            params.append(f"--category={','.join(categories)}")
            
        return f"{base_url}{'/' if params else ''}{'/'.join(params)}"
        
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
            description = card.find("p", {"class": "description"}).text.strip()
            
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
            
            # Extract price info
            price_elem = card.find("div", {"class": "price"})
            price_info = {}
            if price_elem:
                price_text = price_elem.text.strip()
                if price_text == "Free":
                    price_info = {"min": 0, "max": 0, "currency": "USD"}
                elif "-" in price_text:
                    min_price, max_price = map(
                        lambda x: float(x.replace("$", "").strip()),
                        price_text.split("-")
                    )
                    price_info = {"min": min_price, "max": max_price, "currency": "USD"}
                    
            # Extract categories
            categories = []
            category_elem = card.find("div", {"class": "category"})
            if category_elem:
                categories = [cat.strip() for cat in category_elem.text.split(",")]
                
            # Get event URL and ID
            link = card.find("a", {"class": "event-link"})
            if not link or not link.get("href"):
                return None
                
            url = link["href"]
            event_id = url.split("-")[-1]
            
            return {
                "title": title,
                "description": description,
                "start_time": start_time,
                "location": location,
                "price_info": price_info,
                "categories": categories,
                "source": self.platform,
                "event_id": event_id,
                "url": url
            }
            
        except Exception as e:
            logger.error(f"Error parsing event card: {str(e)}")
            return None 