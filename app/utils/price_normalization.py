import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
import statistics
from app.utils.schema import Event, PriceInfo

logger = logging.getLogger(__name__)

# Exchange rates could be fetched from an API in production
EXCHANGE_RATES = {
    'USD': 1.0,
    'EUR': 1.09,
    'GBP': 1.27,
    'CAD': 0.74,
    'AUD': 0.66,
    'JPY': 0.0067
}

@dataclass
class PriceStatistics:
    """Statistics about event prices in a category or region."""
    mean: float
    median: float
    percentile_25: float
    percentile_75: float
    min_price: float
    max_price: float
    sample_size: int

def normalize_price(
    amount: float,
    currency: str,
    target_currency: str = 'USD'
) -> float:
    """
    Convert price to target currency using exchange rates.
    Returns the normalized price in target currency.
    """
    if currency == target_currency:
        return amount
    
    try:
        rate = EXCHANGE_RATES[currency] / EXCHANGE_RATES[target_currency]
        return round(amount * rate, 2)
    except KeyError:
        logger.warning(f"Exchange rate not found for {currency} to {target_currency}")
        return amount

def calculate_price_tier(
    price: float,
    category_stats: Optional[PriceStatistics] = None
) -> str:
    """
    Determine price tier based on amount and optional category statistics.
    Returns 'free', 'budget', 'medium', or 'premium'.
    
    Default thresholds (USD):
    - free: $0
    - budget: $0.01-$40
    - medium: $40.01-$100
    - premium: >$100
    """
    if price == 0:
        return 'free'
    
    if category_stats and category_stats.sample_size > 1:
        # Use category-specific thresholds only if we have enough samples
        if price <= category_stats.percentile_25:
            return 'budget'
        elif price <= category_stats.percentile_75:
            return 'medium'
        else:
            return 'premium'
    else:
        # Use general thresholds based on common event price ranges
        if price <= 40:
            return 'budget'
        elif price <= 100:
            return 'medium'
        else:
            return 'premium'

def get_category_price_statistics(events: List[Event], category: str) -> PriceStatistics:
    """
    Calculate price statistics for events in a specific category.
    Returns statistics about price distribution.
    """
    # Get prices for events in this category
    prices = [
        normalize_price(event.price_info.max_price, event.price_info.currency)
        for event in events
        if category in event.categories and event.price_info.max_price > 0
    ]
    
    if not prices:
        return PriceStatistics(
            mean=0,
            median=0,
            percentile_25=0,
            percentile_75=0,
            min_price=0,
            max_price=0,
            sample_size=0
        )
    
    sorted_prices = sorted(prices)
    return PriceStatistics(
        mean=statistics.mean(prices),
        median=statistics.median(prices),
        percentile_25=sorted_prices[len(sorted_prices) // 4],
        percentile_75=sorted_prices[len(sorted_prices) * 3 // 4],
        min_price=min(prices),
        max_price=max(prices),
        sample_size=len(prices)
    )

def normalize_event_price(
    event: Event,
    category_stats: Optional[Dict[str, PriceStatistics]] = None
) -> Event:
    """
    Normalize price information for an event.
    Updates price_info with normalized values and appropriate price tier.
    Returns the updated event.
    """
    # Normalize min and max prices to USD
    normalized_min = normalize_price(
        event.price_info.min_price,
        event.price_info.currency
    )
    normalized_max = normalize_price(
        event.price_info.max_price,
        event.price_info.currency
    )
    
    # Get relevant category statistics if available
    relevant_stats = None
    if category_stats and event.categories:
        # Use statistics from the most specific category
        for category in event.categories:
            if category in category_stats:
                relevant_stats = category_stats[category]
                break
    
    # Determine price tier
    price_tier = calculate_price_tier(normalized_max, relevant_stats)
    
    # Update price info
    event.price_info.min_price = normalized_min
    event.price_info.max_price = normalized_max
    event.price_info.currency = 'USD'  # Now normalized to USD
    event.price_info.price_tier = price_tier
    
    return event

def batch_normalize_prices(events: List[Event]) -> List[Event]:
    """
    Normalize prices for a batch of events.
    Calculates category statistics and applies normalization to all events.
    Returns list of events with normalized prices.
    """
    logger.debug(f"\nProcessing {len(events)} events for price normalization")
    
    # First normalize all prices to USD
    for event in events:
        event.price_info.min_price = normalize_price(
            event.price_info.min_price,
            event.price_info.currency
        )
        event.price_info.max_price = normalize_price(
            event.price_info.max_price,
            event.price_info.currency
        )
        event.price_info.currency = 'USD'
        logger.debug(f"Normalized {event.title}: {event.price_info.max_price} USD")
    
    # Calculate statistics for each category
    category_stats = {}
    all_categories = set()
    for event in events:
        all_categories.update(event.categories)
    
    for category in all_categories:
        category_stats[category] = get_category_price_statistics(events, category)
        logger.debug(f"Price statistics for {category}:")
        logger.debug(f"  Sample size: {category_stats[category].sample_size}")
        logger.debug(f"  Mean: {category_stats[category].mean}")
        logger.debug(f"  25th percentile: {category_stats[category].percentile_25}")
        logger.debug(f"  75th percentile: {category_stats[category].percentile_75}")
    
    # Then determine price tiers using the category statistics
    normalized_events = []
    for event in events:
        try:
            # Find the most relevant category statistics
            relevant_stats = None
            max_samples = 0
            
            logger.debug(f"\nProcessing event: {event.title}")
            logger.debug(f"Price: {event.price_info.max_price} USD")
            logger.debug(f"Categories: {event.categories}")
            
            if event.categories:
                # Use statistics from the category with the most samples
                for category in event.categories:
                    if category in category_stats:
                        stats = category_stats[category]
                        logger.debug(f"Category {category} has {stats.sample_size} samples")
                        if stats.sample_size > max_samples:
                            max_samples = stats.sample_size
                            relevant_stats = stats
            
            if relevant_stats:
                logger.debug(f"Using statistics with {relevant_stats.sample_size} samples")
                logger.debug(f"25th percentile: {relevant_stats.percentile_25}")
                logger.debug(f"75th percentile: {relevant_stats.percentile_75}")
            else:
                logger.debug("No relevant category statistics found")
            
            # Calculate price tier
            event.price_info.price_tier = calculate_price_tier(
                event.price_info.max_price,
                relevant_stats
            )
            logger.debug(f"Assigned price tier: {event.price_info.price_tier}")
            
            normalized_events.append(event)
            
        except Exception as e:
            logger.error(f"Error normalizing prices for event {event.event_id}: {str(e)}")
            normalized_events.append(event)  # Keep original event if normalization fails
    
    return normalized_events 