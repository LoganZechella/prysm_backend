from typing import Dict, Optional
from app.schemas.event import Event, PriceInfo

# Exchange rates (to USD)
EXCHANGE_RATES = {
    "USD": 1.0,
    "EUR": 1.12,  # 1 EUR = 1.12 USD
    "GBP": 1.27,  # 1 GBP = 1.27 USD
}

def normalize_price(amount: float, currency: str) -> float:
    """
    Convert price to USD.
    
    Args:
        amount: Price amount
        currency: Currency code
        
    Returns:
        Price in USD
        
    Raises:
        ValueError: If currency is not supported
    """
    if currency not in EXCHANGE_RATES:
        raise ValueError(f"Unsupported currency: {currency}")
        
    return amount * EXCHANGE_RATES[currency]

def calculate_price_tier(price: float) -> str:
    """
    Calculate price tier based on USD amount.
    
    Args:
        price: Price in USD
        
    Returns:
        Price tier (free, low, medium, high)
    """
    if price == 0:
        return "free"
    elif price <= 20:
        return "low"
    elif price <= 50:
        return "medium"
    else:
        return "high"

def enrich_event_prices(event: Event) -> Event:
    """
    Enrich event with normalized prices and price tier.
    
    Args:
        event: Event to enrich
        
    Returns:
        Enriched event with normalized prices
    """
    if not event.price_info:
        # Set default price info
        event.price_info = PriceInfo(
            min_price=0.0,
            max_price=0.0,
            currency="USD",
            price_tier="free"
        )
        return event
        
    # Normalize prices to USD if needed
    if event.price_info.currency != "USD":
        event.price_info.min_price = normalize_price(
            event.price_info.min_price,
            event.price_info.currency
        )
        event.price_info.max_price = normalize_price(
            event.price_info.max_price,
            event.price_info.currency
        )
        event.price_info.currency = "USD"
    
    # Calculate price tier based on max price
    event.price_info.price_tier = calculate_price_tier(event.price_info.max_price)
    
    return event 