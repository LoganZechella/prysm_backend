from typing import Dict, Any, Optional, Union
import logging
from decimal import Decimal
from forex_python.converter import CurrencyRates

logger = logging.getLogger(__name__)

class PriceNormalizer:
    def __init__(self):
        self.currency_converter = CurrencyRates()
        self.base_currency = "USD"
        
    def normalize_price(self, price_info: Dict[str, Any]) -> Optional[float]:
        """
        Normalize price information to a single comparable value in USD.
        
        Args:
            price_info: Dictionary containing price information with fields:
                - amount: float or string
                - currency: string (ISO code)
                - type: string (fixed, range, starting_at, etc.)
                
        Returns:
            Normalized price in USD, or None if price cannot be normalized
        """
        try:
            if not price_info:
                return None
                
            # Extract price type and amount
            price_type = price_info.get('type', 'fixed')
            currency = price_info.get('currency', self.base_currency)
            
            # Handle different price types
            if price_type == 'fixed':
                amount = self._parse_amount(price_info.get('amount'))
            elif price_type == 'range':
                # For ranges, use the average of min and max
                min_amount = self._parse_amount(price_info.get('min_amount'))
                max_amount = self._parse_amount(price_info.get('max_amount'))
                if min_amount is None or max_amount is None:
                    return None
                amount = (min_amount + max_amount) / 2
            elif price_type == 'starting_at':
                amount = self._parse_amount(price_info.get('amount'))
            else:
                logger.warning(f"Unknown price type: {price_type}")
                return None
                
            if amount is None:
                return None
                
            # Convert to base currency if needed
            if currency != self.base_currency:
                try:
                    amount = self.currency_converter.convert(currency, self.base_currency, amount)
                except Exception as e:
                    logger.error(f"Currency conversion error: {str(e)}")
                    return None
                    
            return float(amount)
            
        except Exception as e:
            logger.error(f"Price normalization error: {str(e)}")
            return None
            
    def _parse_amount(self, amount: Union[str, float, int, None]) -> Optional[float]:
        """Parse price amount from various formats."""
        if amount is None:
            return None
            
        try:
            # If already a number, just convert to float
            if isinstance(amount, (int, float)):
                return float(amount)
                
            # If string, handle various formats
            if isinstance(amount, str):
                # Remove currency symbols and other non-numeric characters
                cleaned = ''.join(c for c in amount if c.isdigit() or c in '.,')
                if not cleaned:
                    return None
                    
                # Handle different decimal separators
                if ',' in cleaned and '.' in cleaned:
                    # Assume format like 1,234.56
                    cleaned = cleaned.replace(',', '')
                elif ',' in cleaned:
                    # Assume format like 1234,56
                    cleaned = cleaned.replace(',', '.')
                    
                return float(cleaned)
                
            return None
            
        except Exception as e:
            logger.error(f"Amount parsing error: {str(e)}")
            return None
            
    def get_price_range(self, price_info: Dict[str, Any]) -> Dict[str, float]:
        """
        Get the minimum and maximum prices from price information.
        
        Args:
            price_info: Dictionary containing price information
            
        Returns:
            Dictionary with min and max prices in base currency
        """
        try:
            if not price_info:
                return {'min': 0.0, 'max': float('inf')}
                
            price_type = price_info.get('type', 'fixed')
            currency = price_info.get('currency', self.base_currency)
            
            if price_type == 'fixed':
                amount = self._parse_amount(price_info.get('amount'))
                if amount is None:
                    return {'min': 0.0, 'max': float('inf')}
                    
                if currency != self.base_currency:
                    amount = float(self.currency_converter.convert(currency, self.base_currency, amount))
                    
                return {'min': float(amount), 'max': float(amount)}
                
            elif price_type == 'range':
                min_amount = self._parse_amount(price_info.get('min_amount'))
                max_amount = self._parse_amount(price_info.get('max_amount'))
                
                if min_amount is None:
                    min_amount = 0.0
                if max_amount is None:
                    max_amount = float('inf')
                    
                if currency != self.base_currency:
                    if min_amount != 0.0:
                        min_amount = float(self.currency_converter.convert(currency, self.base_currency, min_amount))
                    if max_amount != float('inf'):
                        max_amount = float(self.currency_converter.convert(currency, self.base_currency, max_amount))
                        
                return {'min': float(min_amount), 'max': float(max_amount)}
                
            elif price_type == 'starting_at':
                amount = self._parse_amount(price_info.get('amount'))
                if amount is None:
                    return {'min': 0.0, 'max': float('inf')}
                    
                if currency != self.base_currency:
                    amount = float(self.currency_converter.convert(currency, self.base_currency, amount))
                    
                return {'min': float(amount), 'max': float('inf')}
                
            return {'min': 0.0, 'max': float('inf')}
            
        except Exception as e:
            logger.error(f"Error getting price range: {str(e)}")
            return {'min': 0.0, 'max': float('inf')} 