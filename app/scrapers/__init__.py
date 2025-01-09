"""
Scraper module initialization.
"""

from app.scrapers.eventbrite_scrapfly import EventbriteScrapflyScraper
from app.scrapers.meetup_scrapfly import MeetupScrapflyScraper
from app.scrapers.facebook_scrapfly import FacebookScrapflyScraper

__all__ = [
    'EventbriteScrapflyScraper',
    'MeetupScrapflyScraper',
    'FacebookScrapflyScraper'
] 