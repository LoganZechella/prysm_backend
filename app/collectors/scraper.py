import os
import logging
from typing import Dict, List, Any, Optional, AsyncGenerator, Union
from datetime import datetime, timedelta
from scrapfly import ScrapflyClient, ScrapeConfig, ScrapeApiResponse
from app.utils.storage import StorageManager
from app.utils.schema import Event, Location, PriceInfo, SourceInfo
from bs4 import BeautifulSoup
import json
import re

logger = logging.getLogger(__name__)

class EventScraper:
    """Base event scraping infrastructure using Scrapfly"""
    
    def __init__(self):
        self.api_key = os.getenv("SCRAPFLY_API_KEY")
        if not self.api_key:
            raise ValueError("SCRAPFLY_API_KEY environment variable is not set")
            
        self.client = ScrapflyClient(key=self.api_key)
        self.storage = StorageManager()
        logger.info("Initialized EventScraper")
        
    async def _get_scrape_configs(self, source: str, location: str, start_date: datetime) -> List[ScrapeConfig]:
        """Get scraping configurations for a specific source"""
        configs = []
        
        if source == "eventbrite":
            # Eventbrite search pages
            base_url = "https://www.eventbrite.com/d/united-states--{location}/events/"
            configs.append(
                ScrapeConfig(
                    url=base_url.format(location=location.lower().replace(" ", "-")),
                    asp=True,  # Anti scraping protection
                    country="US"
                )
            )
        elif source == "meetup":
            # Meetup search pages
            base_url = "https://www.meetup.com/find/?location={location}&source=EVENTS"
            configs.append(
                ScrapeConfig(
                    url=base_url.format(location=location),
                    asp=True,
                    country="US"
                )
            )
        
        return configs
        
    def _parse_events(self, result: Union[Dict[str, Any], ScrapeApiResponse], source: str) -> List[Event]:
        """Parse scraped HTML into Event objects"""
        events = []
        try:
            if source == "eventbrite":
                events.extend(self._parse_eventbrite(result))
            elif source == "meetup":
                events.extend(self._parse_meetup(result))
        except Exception as e:
            logger.error(f"Error parsing events from {source}: {str(e)}")
            
        return events
        
    def _parse_eventbrite(self, result: Union[Dict[str, Any], ScrapeApiResponse]) -> List[Event]:
        """Parse Eventbrite HTML into Event objects"""
        events = []
        try:
            # Get HTML content from Scrapfly response
            html_content = result.content if isinstance(result, ScrapeApiResponse) else result.get('content', '')
            # Get URL from the response
            result_url = result.context['url'] if isinstance(result, ScrapeApiResponse) else result.get('url', '')
            
            # Log the HTML content for debugging
            logger.debug(f"HTML content length: {len(html_content)}")
            
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Find all event cards
            event_cards = soup.find_all('div', {'class': 'event-card'})
            logger.info(f"Found {len(event_cards)} event cards")
            
            for card in event_cards:
                try:
                    # Extract basic event info
                    title_elem = card.find('h3', {'class': lambda x: x and 'Typography_body-lg__487rx' in x})
                    
                    if not title_elem:
                        logger.warning("Could not find title element")
                        continue
                        
                    title = title_elem.text.strip()
                    logger.info(f"Found event: {title}")
                    
                    # Extract date and time
                    date_elem = card.find('p', {'class': lambda x: x and 'Typography_body-md-bold__487rx' in x})
                    date_str = date_elem.text.strip() if date_elem else ""
                    time_str = ""  # Time is included in the date string
                    start_datetime = self._parse_eventbrite_datetime(date_str, time_str)
                    
                    # Extract location
                    location_elem = card.find('a', {'class': 'event-card-link'})
                    venue_name = ""
                    address = ""
                    city = ""
                    state = ""
                    country = "United States"
                    
                    if location_elem:
                        location_data = location_elem.get('data-event-location', '')
                        if location_data:
                            city = location_data
                    
                    # Extract price
                    price_elem = card.find('p', {'class': lambda x: x and '#716b7a' in x})
                    price_text = price_elem.text.strip() if price_elem else "Free"
                    min_price, max_price, price_tier = self._parse_eventbrite_price(price_text)
                    
                    # Extract image
                    image_elem = card.find('img', {'class': 'event-card-image'})
                    images = [image_elem['src']] if image_elem and 'src' in image_elem.attrs else []
                    
                    # Get event URL
                    event_link = card.find('a', {'class': 'event-card-link'})
                    event_url = event_link['href'] if event_link and 'href' in event_link.attrs else result_url
                    
                    # Create Event object
                    event = Event(
                        event_id=f"eventbrite_{hash(title + str(start_datetime))}",
                        title=title,
                        description="",  # Description not available in card view
                        short_description="",
                        start_datetime=start_datetime,
                        end_datetime=start_datetime + timedelta(hours=3),  # Default duration
                        location=Location(
                            venue_name=venue_name,
                            address=address,
                            city=city,
                            state=state,
                            country=country,
                            coordinates={"lat": 0.0, "lng": 0.0}  # Default coordinates
                        ),
                        categories=[],  # TODO: Extract categories
                        tags=[],
                        price_info=PriceInfo(
                            currency="USD",
                            min_price=min_price,
                            max_price=max_price,
                            price_tier=price_tier
                        ),
                        images=images,
                        source=SourceInfo(
                            platform="eventbrite",
                            url=event_url,
                            last_updated=datetime.utcnow()
                        )
                    )
                    
                    events.append(event)
                    logger.info(f"Successfully parsed event: {title}")
                    
                except Exception as e:
                    logger.error(f"Error parsing event card: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error parsing Eventbrite HTML: {str(e)}")
            
        return events
        
    def _parse_eventbrite_datetime(self, date_str: str, time_str: str) -> datetime:
        """Parse Eventbrite date and time strings into datetime object"""
        try:
            # Example: "Sat, Jan 20" and "7:00 PM"
            date_parts = date_str.replace(',', '').split()
            if len(date_parts) == 2:
                # Add current year if not provided
                date_parts.append(str(datetime.utcnow().year))
            
            date_str = ' '.join(date_parts)
            datetime_str = f"{date_str} {time_str}"
            
            # Try different format patterns
            formats = [
                "%a %b %d %Y %I:%M %p",
                "%a %b %d %Y %H:%M",
                "%Y-%m-%d %H:%M"
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(datetime_str, fmt)
                except ValueError:
                    continue
                    
            # If no format matches, use current time
            return datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Error parsing datetime: {str(e)}")
            return datetime.utcnow()
            
    def _parse_eventbrite_price(self, price_text: str) -> tuple[float, float, str]:
        """Parse Eventbrite price text into price components"""
        try:
            if price_text.lower() == "free":
                return 0.0, 0.0, "free"
                
            # Try to extract price using regex
            price_match = re.search(r'\$(\d+(?:\.\d{2})?)', price_text)
            if price_match:
                price = float(price_match.group(1))
                if price == 0:
                    return 0.0, 0.0, "free"
                elif price < 20:
                    return price, price, "budget"
                elif price < 50:
                    return price, price, "medium"
                else:
                    return price, price, "premium"
                    
            return 0.0, 0.0, "free"
            
        except Exception as e:
            logger.error(f"Error parsing price: {str(e)}")
            return 0.0, 0.0, "free"
        
    def _parse_meetup(self, result: Union[Dict[str, Any], ScrapeApiResponse]) -> List[Event]:
        """Parse Meetup HTML into Event objects"""
        events = []
        # TODO: Implement Meetup specific parsing
        return events
        
    async def scrape_events(
        self,
        sources: List[str],
        location: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> AsyncGenerator[Event, None]:
        """
        Scrape events from specified sources
        
        Args:
            sources: List of source platforms to scrape
            location: Location to search for events
            start_date: Start date for event search
            end_date: End date for event search
        """
        if not start_date:
            start_date = datetime.utcnow()
        if not end_date:
            end_date = start_date + timedelta(days=30)
            
        for source in sources:
            try:
                # Get scraping configurations for this source
                configs = await self._get_scrape_configs(source, location, start_date)
                
                # Scrape concurrently using Scrapfly
                async for result in self.client.concurrent_scrape(configs):
                    if isinstance(result, Exception):
                        logger.error(f"Error scraping {source}: {str(result)}")
                        continue
                        
                    # Parse events from the scraped data
                    events = self._parse_events(result, source)
                    
                    # Store and yield each event
                    for event in events:
                        try:
                            # Convert event to JSON and store raw data
                            event_data = json.loads(event.model_dump_json())
                            try:
                                # Store the event data and get the blob name
                                storage_result = await self.storage.store_raw_event(  # type: ignore[awaitable-is-async]
                                    source,
                                    event_data  # type: ignore[arg-type]
                                )
                                if storage_result:
                                    logger.debug(f"Stored event in {storage_result}")
                            except Exception as storage_error:
                                logger.error(f"Error storing event data: {str(storage_error)}")
                            # Yield the event regardless of storage success
                            yield event
                        except Exception as e:
                            logger.error(f"Error processing event from {source}: {str(e)}")
                            continue
                            
            except Exception as e:
                logger.error(f"Error processing source {source}: {str(e)}")
                continue
                
    async def get_account_info(self) -> Dict[str, Any]:
        """Get Scrapfly account information"""
        try:
            result = self.client.account()
            return result if isinstance(result, dict) else {}
        except Exception as e:
            logger.error(f"Error getting account info: {str(e)}")
            return {} 