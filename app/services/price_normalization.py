"""Price normalization service for event prices."""

from typing import Dict, Any, List, Optional, Union
import logging
from forex_python.converter import CurrencyRates
from datetime import datetime

logger = logging.getLogger(__name__)

# Global currency converter instance
_currency_converter = CurrencyRates()

def normalize_price(amount: Union[float, int], currency: str = "USD") -> Optional[float]:
    """
    Normalize price to USD.
    
    Args:
        amount: Price amount
        currency: Currency code (default: USD)
        
    Returns:
        Normalized price in USD
    """
    try:
        if currency == "USD":
            return float(amount)
            
        rate = _currency_converter.get_rate(currency, "USD")
        return float(amount) * rate
        
    except Exception as e:
        logger.error(f"Error normalizing price: {str(e)}")
        return None

def calculate_price_tier(price: float, currency: str = "USD") -> str:
    """
    Calculate price tier based on normalized price.
    
    Args:
        price: Price amount
        currency: Currency code (default: USD)
        
    Returns:
        Price tier string ('free', 'budget', 'medium', 'premium', 'luxury')
    """
    try:
        # Convert to USD if needed
        if currency != "USD":
            normalized = normalize_price(price, currency)
            if normalized is None:
                return "unknown"
            price = normalized
        
        if price == 0:
            return "free"
        elif price <= 20:
            return "budget"
        elif price <= 50:
            return "medium"
        elif price <= 100:
            return "premium"
        else:
            return "luxury"
            
    except Exception as e:
        logger.error(f"Error calculating price tier: {str(e)}")
        return "unknown"

def enrich_event_prices(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Enrich event data with normalized prices and price tiers.
    
    Args:
        events: List of event dictionaries
        
    Returns:
        Events with enriched price information
    """
    try:
        enriched_events = []
        
        for event in events:
            # Get original price info
            price_info = event.get('price_info', {})
            
            if price_info:
                # Get base price and currency
                amount = price_info.get('amount')
                currency = price_info.get('currency', 'USD')
                
                if amount is not None:
                    # Add normalized price
                    normalized = normalize_price(amount, currency)
                    if normalized is not None:
                        price_info['normalized_price'] = round(normalized, 2)
                    
                    # Add price tier
                    price_info['tier'] = calculate_price_tier(amount, currency)
                    
                    # Update event with enriched price info
                    event['price_info'] = price_info
            
            enriched_events.append(event)
        
        return enriched_events
        
    except Exception as e:
        logger.error(f"Error enriching event prices: {str(e)}")
        return events

class PriceNormalizer:
    """Service for normalizing price information to a single comparable value."""
    
    def __init__(self):
        """Initialize price normalizer."""
        self.currency_converter = CurrencyRates()
        self.default_currency = 'USD'
        
        # Price tier thresholds in USD
        self.price_tiers = {
            'free': 0,
            'budget': 20,
            'medium': 50,
            'premium': 100,
            'luxury': float('inf')
        }
    
    def normalize_price(self, price_info: Dict[str, Any]) -> Optional[float]:
        """
        Normalize price information to a single comparable value in USD.
        
        Args:
            price_info: Dictionary containing price information
                - amount: Price amount
                - currency: Currency code (e.g., 'USD', 'EUR')
                - type: Price type ('fixed', 'range', 'starting_at')
                - min_price: Minimum price for range
                - max_price: Maximum price for range
            
        Returns:
            Normalized price in USD or None if invalid
        """
        try:
            if not price_info:
                return None
            
            price_type = price_info.get('type', 'fixed')
            currency = price_info.get('currency', self.default_currency)
            
            if price_type == 'fixed':
                amount = price_info.get('amount')
                if amount is None:
                    return None
                return self._convert_to_usd(amount, currency)
                
            elif price_type == 'range':
                min_price = price_info.get('min_price')
                max_price = price_info.get('max_price')
                if min_price is None or max_price is None:
                    return None
                # Use average of range
                avg_price = (min_price + max_price) / 2
                return self._convert_to_usd(avg_price, currency)
                
            elif price_type == 'starting_at':
                amount = price_info.get('amount')
                if amount is None:
                    return None
                # Add 20% to starting price as estimate
                estimated_price = amount * 1.2
                return self._convert_to_usd(estimated_price, currency)
            
            return None
            
        except Exception as e:
            logger.error(f"Error normalizing price: {str(e)}")
            return None
    
    def calculate_price_tier(self, price_info: Dict[str, Any]) -> str:
        """
        Calculate price tier based on normalized price.
        
        Args:
            price_info: Dictionary containing price information
            
        Returns:
            Price tier string ('free', 'budget', 'medium', 'premium', 'luxury')
        """
        try:
            normalized_price = self.normalize_price(price_info)
            
            if normalized_price is None:
                return 'unknown'
            
            if normalized_price == 0:
                return 'free'
            elif normalized_price <= self.price_tiers['budget']:
                return 'budget'
            elif normalized_price <= self.price_tiers['medium']:
                return 'medium'
            elif normalized_price <= self.price_tiers['premium']:
                return 'premium'
            else:
                return 'luxury'
                
        except Exception as e:
            logger.error(f"Error calculating price tier: {str(e)}")
            return 'unknown'
    
    def _convert_to_usd(self, amount: float, from_currency: str) -> float:
        """Convert amount from given currency to USD."""
        if from_currency == self.default_currency:
            return amount
            
        try:
            rate = self.currency_converter.get_rate(from_currency, self.default_currency)
            return amount * rate
        except Exception as e:
            logger.error(f"Error converting currency: {str(e)}")
            return amount  # Return original amount if conversion fails 