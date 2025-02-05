"""Service for monitoring Personalize data quality and transformations."""
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

class PersonalizeMonitor:
    """Monitor data quality and transformations for Amazon Personalize."""

    def __init__(self):
        """Initialize the monitoring service."""
        self.reset_metrics()

    def reset_metrics(self):
        """Reset all monitoring metrics."""
        self.metrics = {
            'total_events': 0,
            'successful_transforms': 0,
            'failed_transforms': 0,
            'validation_errors': defaultdict(int),
            'field_stats': defaultdict(lambda: {
                'null_count': 0,
                'total_count': 0,
                'min_length': float('inf'),
                'max_length': 0
            }),
            'category_distribution': defaultdict(int),
            'price_distribution': {
                'free': 0,
                'low': 0,
                'medium': 0,
                'high': 0
            },
            'start_time': datetime.now()
        }

    def track_transform(self, event_id: str, success: bool, error: Optional[str] = None):
        """Track the success/failure of an event transformation.
        
        Args:
            event_id: ID of the event
            success: Whether the transformation succeeded
            error: Optional error message
        """
        self.metrics['total_events'] += 1
        if success:
            self.metrics['successful_transforms'] += 1
        else:
            self.metrics['failed_transforms'] += 1
            if error:
                self.metrics['validation_errors'][error] += 1

    def track_field_stats(self, field_name: str, value: Any):
        """Track statistics for a field value.
        
        Args:
            field_name: Name of the field
            value: Field value
        """
        stats = self.metrics['field_stats'][field_name]
        stats['total_count'] += 1
        
        if value is None or value == "":
            stats['null_count'] += 1
            return
            
        # Track length statistics for string fields
        if isinstance(value, str):
            length = len(value)
            stats['min_length'] = min(stats['min_length'], length)
            stats['max_length'] = max(stats['max_length'], length)

    def track_category(self, category: str):
        """Track category distribution.
        
        Args:
            category: Category value
        """
        self.metrics['category_distribution'][category] += 1

    def track_price(self, price: float):
        """Track price distribution.
        
        Args:
            price: Price value
        """
        if price == 0:
            self.metrics['price_distribution']['free'] += 1
        elif price < 20:
            self.metrics['price_distribution']['low'] += 1
        elif price < 50:
            self.metrics['price_distribution']['medium'] += 1
        else:
            self.metrics['price_distribution']['high'] += 1

    def get_report(self) -> Dict[str, Any]:
        """Generate a monitoring report.
        
        Returns:
            Dictionary containing monitoring metrics and statistics
        """
        duration = (datetime.now() - self.metrics['start_time']).total_seconds()
        
        success_rate = 0
        if self.metrics['total_events'] > 0:
            success_rate = (self.metrics['successful_transforms'] / self.metrics['total_events']) * 100
            
        report = {
            'summary': {
                'total_events': self.metrics['total_events'],
                'successful_transforms': self.metrics['successful_transforms'],
                'failed_transforms': self.metrics['failed_transforms'],
                'success_rate': f"{success_rate:.2f}%",
                'duration_seconds': duration
            },
            'validation_errors': dict(self.metrics['validation_errors']),
            'field_stats': {
                field: {
                    'null_rate': (stats['null_count'] / stats['total_count'] * 100) if stats['total_count'] > 0 else 0,
                    'total_count': stats['total_count'],
                    'min_length': stats['min_length'] if stats['min_length'] != float('inf') else 0,
                    'max_length': stats['max_length']
                }
                for field, stats in self.metrics['field_stats'].items()
            },
            'category_distribution': dict(self.metrics['category_distribution']),
            'price_distribution': self.metrics['price_distribution']
        }
        
        return report

    def log_report(self):
        """Log the current monitoring report."""
        report = self.get_report()
        logger.info("Personalize Transformation Report:")
        logger.info(json.dumps(report, indent=2)) 