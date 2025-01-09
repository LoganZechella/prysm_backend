"""
Monitoring utilities for tracking scraper performance and errors.

This module provides:
- Structured logging setup
- Error rate tracking
- Performance metrics
- Health status monitoring
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict
import json

logger = logging.getLogger(__name__)

class ScraperMonitor:
    """Monitor scraper health and performance"""
    
    def __init__(self, error_threshold: float = 0.3, window_minutes: int = 15):
        """
        Initialize the scraper monitor.
        
        Args:
            error_threshold: Maximum acceptable error rate (0.0 to 1.0)
            window_minutes: Time window for calculating error rates
        """
        self.error_threshold = error_threshold
        self.window_minutes = window_minutes
        
        # Track errors by scraper
        self.errors: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Track success/failure counts
        self.requests: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Track performance metrics
        self.response_times: Dict[str, List[float]] = defaultdict(list)
        
        # Track scraper health status
        self.health_status: Dict[str, bool] = {}
        
    def record_error(
        self,
        scraper_name: str,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record a scraper error.
        
        Args:
            scraper_name: Name of the scraper
            error: The exception that occurred
            context: Optional context about the error
        """
        error_data = {
            'timestamp': datetime.utcnow(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context or {}
        }
        
        self.errors[scraper_name].append(error_data)
        
        # Trim old errors outside the window
        self._trim_old_data(self.errors[scraper_name])
        
        # Update health status
        self._update_health_status(scraper_name)
        
        # Log the error
        logger.error(
            f"Scraper error: {scraper_name}",
            extra={
                'scraper': scraper_name,
                'error_type': error_data['error_type'],
                'error_message': error_data['error_message'],
                'context': json.dumps(context) if context else None
            }
        )
        
    def record_request(
        self,
        scraper_name: str,
        success: bool,
        response_time: float,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record a scraper request result.
        
        Args:
            scraper_name: Name of the scraper
            success: Whether the request succeeded
            response_time: Request duration in seconds
            context: Optional request context
        """
        request_data = {
            'timestamp': datetime.utcnow(),
            'success': success,
            'response_time': response_time,
            'context': context or {}
        }
        
        self.requests[scraper_name].append(request_data)
        self.response_times[scraper_name].append(response_time)
        
        # If request failed, record error
        if not success:
            self.record_error(
                scraper_name,
                Exception("Request failed"),
                context=request_data['context']
            )
        
        # Trim old data
        self._trim_old_data(self.requests[scraper_name])
        self._trim_old_data(self.response_times[scraper_name])
        
        # Update health status
        self._update_health_status(scraper_name)
        
        # Log metrics
        logger.info(
            f"Scraper request: {scraper_name}",
            extra={
                'scraper': scraper_name,
                'success': success,
                'response_time': response_time,
                'context': json.dumps(context) if context else None
            }
        )
        
    def get_error_rate(self, scraper_name: str) -> float:
        """
        Calculate error rate for a scraper in the current window.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            Error rate as a float between 0.0 and 1.0
        """
        requests = self.requests[scraper_name]
        if not requests:
            return 0.0
            
        total = len(requests)
        failures = sum(1 for r in requests if not r['success'])
        return failures / total if total > 0 else 0.0
        
    def get_health_status(self, scraper_name: str) -> bool:
        """
        Get current health status of a scraper.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            True if scraper is healthy, False otherwise
        """
        return self.health_status.get(scraper_name, True)
        
    def get_metrics(self, scraper_name: str) -> Dict[str, Any]:
        """
        Get current metrics for a scraper.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            Dictionary of current metrics
        """
        # Trim old data before calculating metrics
        if scraper_name in self.errors:
            self._trim_old_data(self.errors[scraper_name])
        if scraper_name in self.requests:
            self._trim_old_data(self.requests[scraper_name])
        if scraper_name in self.response_times:
            self._trim_old_data(self.response_times[scraper_name])
            
        response_times = self.response_times[scraper_name]
        requests = self.requests[scraper_name]
        errors = self.errors[scraper_name]
        
        if not requests:
            return {
                'error_rate': 0.0,
                'avg_response_time': 0.0,
                'request_count': 0,
                'error_count': len(errors),
                'success_rate': 100.0
            }
            
        return {
            'error_rate': self.get_error_rate(scraper_name),
            'avg_response_time': sum(response_times) / len(response_times),
            'request_count': len(requests),
            'error_count': len(errors),
            'success_rate': (
                sum(1 for r in requests if r['success']) / len(requests) * 100
            )
        }
        
    def _update_health_status(self, scraper_name: str) -> None:
        """Update health status based on current metrics"""
        error_rate = self.get_error_rate(scraper_name)
        self.health_status[scraper_name] = error_rate < self.error_threshold
        
        if not self.health_status[scraper_name]:
            logger.warning(
                f"Scraper health degraded: {scraper_name}",
                extra={
                    'scraper': scraper_name,
                    'error_rate': error_rate,
                    'threshold': self.error_threshold
                }
            )
            
    def _trim_old_data(self, data_list: List[Any]) -> None:
        """Remove data points outside the current window"""
        if not data_list:
            return
            
        cutoff = datetime.utcnow() - timedelta(minutes=self.window_minutes)
        
        # If the data has timestamps, use those
        if isinstance(data_list[0], dict) and 'timestamp' in data_list[0]:
            data_list[:] = [
                d for d in data_list
                if d['timestamp'] >= cutoff
            ]
        # Otherwise just keep the last N points that would fit in the window
        else:
            max_points = self.window_minutes * 60  # Assume 1 request per second max
            if len(data_list) > max_points:
                data_list[:] = data_list[-max_points:] 