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
from typing import Dict, List, Optional, Any, Set
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
        
        # Track scraper health status (store as strings)
        self.health_status: Dict[str, str] = {}
        
        # Track degraded scrapers
        self.degraded_scrapers: Set[str] = set()
        
    def clear_history(self) -> None:
        """Clear all monitoring history"""
        self.errors.clear()
        self.requests.clear()
        self.response_times.clear()
        self.health_status.clear()
        self.degraded_scrapers.clear()
        logger.info("Monitoring history cleared")
        
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
            'context': context or {},
            'traceback': getattr(error, '__traceback__', None)
        }
        
        self.errors[scraper_name].append(error_data)
        
        # Trim old errors outside the window
        self._trim_old_data(self.errors[scraper_name])
        
        # Update health status
        self._update_health_status(scraper_name)
        
        # Log the error with context
        logger.error(
            f"Scraper error: {scraper_name}",
            extra={
                'scraper': scraper_name,
                'error_type': error_data['error_type'],
                'error_message': error_data['error_message'],
                'context': json.dumps(context) if context else None,
                'health_status': self.health_status.get(scraper_name, 'true'),
                'error_rate': self.get_error_rate(scraper_name)
            }
        )
        
    def record_request(
        self,
        scraper_name: str,
        success: str,  # Changed to accept string
        response_time: float,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record a scraper request result.
        
        Args:
            scraper_name: Name of the scraper
            success: Whether the request succeeded ('true' or 'false')
            response_time: Request duration in seconds
            context: Optional request context
        """
        request_data = {
            'timestamp': datetime.utcnow(),
            'success': success,  # Already a string
            'response_time': response_time,
            'context': context or {}
        }
        
        self.requests[scraper_name].append(request_data)
        self.response_times[scraper_name].append(response_time)
        
        # If request failed, record error
        if success == 'false':
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
        
        # Log metrics with health status
        log_level = logging.INFO if success == 'true' else logging.WARNING
        logger.log(
            log_level,
            f"Scraper request: {scraper_name}",
            extra={
                'scraper': scraper_name,
                'success': success,
                'response_time': response_time,
                'context': json.dumps(context) if context else None,
                'health_status': self.health_status.get(scraper_name, 'true'),
                'error_rate': self.get_error_rate(scraper_name)
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
            
        # Trim old data before calculating
        self._trim_old_data(requests)
        
        total = len(requests)
        failures = sum(1 for r in requests if r['success'] == 'false')
        return failures / total if total > 0 else 0.0
        
    def get_health_status(self, scraper_name: str) -> str:
        """
        Get current health status of a scraper.
        
        Args:
            scraper_name: Name of the scraper
            
        Returns:
            'true' if scraper is healthy, 'false' otherwise
        """
        # Update health status before returning
        self._update_health_status(scraper_name)
        return self.health_status.get(scraper_name, 'true')
        
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
                'success_rate': 100.0,
                'health_status': self.health_status.get(scraper_name, 'true'),
                'window_minutes': self.window_minutes,
                'last_request': None,
                'last_error': (
                    errors[-1]['timestamp'].isoformat()
                    if errors else None
                )
            }
            
        return {
            'error_rate': self.get_error_rate(scraper_name),
            'avg_response_time': sum(response_times) / len(response_times),
            'request_count': len(requests),
            'error_count': len(errors),
            'success_rate': (
                sum(1 for r in requests if r['success'] == 'true') / len(requests) * 100
            ),
            'health_status': self.health_status.get(scraper_name, 'true'),
            'window_minutes': self.window_minutes,
            'last_request': requests[-1]['timestamp'].isoformat(),
            'last_error': (
                errors[-1]['timestamp'].isoformat()
                if errors else None
            )
        }
        
    def _update_health_status(self, scraper_name: str) -> None:
        """Update health status based on current metrics"""
        error_rate = self.get_error_rate(scraper_name)
        was_healthy = self.health_status.get(scraper_name, 'true') == 'true'
        is_healthy = error_rate < self.error_threshold
        
        if was_healthy and not is_healthy:
            # Scraper just became unhealthy
            self.degraded_scrapers.add(scraper_name)
            logger.warning(
                f"Scraper health degraded: {scraper_name}",
                extra={
                    'scraper': scraper_name,
                    'error_rate': error_rate,
                    'threshold': self.error_threshold,
                    'window_minutes': self.window_minutes,
                    'health_status': 'false'
                }
            )
        elif not was_healthy and is_healthy:
            # Scraper recovered
            self.degraded_scrapers.discard(scraper_name)
            logger.info(
                f"Scraper health recovered: {scraper_name}",
                extra={
                    'scraper': scraper_name,
                    'error_rate': error_rate,
                    'threshold': self.error_threshold,
                    'health_status': 'true'
                }
            )
            
        self.health_status[scraper_name] = 'true' if is_healthy else 'false'
            
    def _trim_old_data(self, data_list: List[Any]) -> None:
        """Remove data points outside the current window"""
        if not data_list:
            return
            
        cutoff = datetime.utcnow() - timedelta(minutes=self.window_minutes)
        
        # If the data has timestamps, use those
        if isinstance(data_list[0], dict) and 'timestamp' in data_list[0]:
            original_len = len(data_list)
            data_list[:] = [
                d for d in data_list
                if d['timestamp'] >= cutoff
            ]
            if len(data_list) < original_len:
                logger.debug(
                    f"Trimmed {original_len - len(data_list)} old data points"
                )
        # Otherwise just keep the last N points that would fit in the window
        else:
            max_points = self.window_minutes * 60  # Assume 1 request per second max
            if len(data_list) > max_points:
                original_len = len(data_list)
                data_list[:] = data_list[-max_points:]
                logger.debug(
                    f"Trimmed {original_len - len(data_list)} old data points"
                ) 