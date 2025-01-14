"""
Performance monitoring utilities for tracking API calls and system metrics.
"""

import time
import logging
from contextlib import contextmanager
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """Monitor performance metrics for API calls and system operations"""
    
    def __init__(self):
        self.metrics: Dict[str, Any] = {
            'api_calls': {},
            'errors': [],
            'last_error_time': None
        }
    
    @contextmanager
    def monitor_api_call(self, operation_name: str):
        """
        Context manager to monitor API call duration and status
        
        Args:
            operation_name: Name of the API operation being monitored
        """
        start_time = time.time()
        error = None
        
        try:
            yield
        except Exception as e:
            error = e
            raise
        finally:
            duration = time.time() - start_time
            
            if operation_name not in self.metrics['api_calls']:
                self.metrics['api_calls'][operation_name] = {
                    'count': 0,
                    'total_duration': 0,
                    'errors': 0,
                    'last_call_time': None
                }
            
            metrics = self.metrics['api_calls'][operation_name]
            metrics['count'] += 1
            metrics['total_duration'] += duration
            metrics['last_call_time'] = datetime.utcnow().isoformat()
            
            if error:
                metrics['errors'] += 1
            
            # Log performance data
            logger.info(
                f"API Call: {operation_name} - Duration: {duration:.2f}s - "
                f"Status: {'error' if error else 'success'}"
            )
    
    def record_error(self, operation: str, error_message: str):
        """
        Record an error occurrence
        
        Args:
            operation: Name of the operation where error occurred
            error_message: Description of the error
        """
        error_time = datetime.utcnow()
        error_entry = {
            'operation': operation,
            'message': error_message,
            'timestamp': error_time.isoformat()
        }
        
        self.metrics['errors'].append(error_entry)
        self.metrics['last_error_time'] = error_time
        
        logger.error(f"Operation '{operation}' failed: {error_message}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current performance metrics
        
        Returns:
            Dictionary containing collected metrics
        """
        return self.metrics
    
    def reset_metrics(self):
        """Reset all collected metrics"""
        self.metrics = {
            'api_calls': {},
            'errors': [],
            'last_error_time': None
        } 