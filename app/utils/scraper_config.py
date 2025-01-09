"""
Scraper configuration management.

This module provides platform-specific configurations for different event sites,
optimizing settings for each platform's anti-bot measures and requirements.
"""

from typing import Dict, Any

class ScraperConfig:
    """Platform-specific scraper configurations"""
    
    # Eventbrite specific settings
    EVENTBRITE_CONFIG = {
        'country': 'US',
        'asp': True,
        'render_js': True,
        'proxy_pool': 'residential',  # Use residential proxies for better success
        'session': True,
        'timeout': 30,
        'headers': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'TE': 'trailers'
        },
        'browser_emulator': 'chrome',  # Emulate Chrome browser
        'browser_settings': {
            'viewport': {'width': 1920, 'height': 1080},
            'platform': 'Windows',
            'mobile': False
        }
    }
    
    # Meetup specific settings
    MEETUP_CONFIG = {
        'country': 'US',
        'asp': True,
        'render_js': True,
        'proxy_pool': 'residential',
        'session': True,
        'timeout': 30,
        'headers': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        },
        'browser_emulator': 'firefox',  # Meetup seems to work better with Firefox
        'browser_settings': {
            'viewport': {'width': 1366, 'height': 768},
            'platform': 'MacOS',
            'mobile': False
        }
    }
    
    # Facebook specific settings
    FACEBOOK_CONFIG = {
        'country': 'US',
        'asp': True,
        'render_js': True,
        'proxy_pool': 'residential',
        'session': True,
        'timeout': 45,  # Longer timeout for Facebook's complex loading
        'headers': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        },
        'browser_emulator': 'chrome',
        'browser_settings': {
            'viewport': {'width': 1920, 'height': 1080},
            'platform': 'Windows',
            'mobile': False
        },
        'wait_for': '.event-card',  # Wait for event cards to load
        'wait_timeout': 10
    }
    
    @classmethod
    def get_config(cls, platform: str) -> Dict[str, Any]:
        """
        Get configuration for specific platform.
        
        Args:
            platform: Platform name (eventbrite, meetup, facebook)
            
        Returns:
            Platform-specific configuration dictionary
        """
        configs = {
            'eventbrite': cls.EVENTBRITE_CONFIG,
            'meetup': cls.MEETUP_CONFIG,
            'facebook': cls.FACEBOOK_CONFIG
        }
        
        base_config = {
            'retry_enabled': True,
            'retry_count': 2,
            'cookies_enabled': True,
            'js_scenario': {
                'steps': [
                    {'wait': 2000},  # Initial wait for page load
                    {'scroll_y': 800},  # Scroll down
                    {'wait': 1000},  # Wait for dynamic content
                    {'scroll_y': 1600},  # Scroll more
                    {'wait': 1000}  # Final wait
                ]
            }
        }
        
        platform_config = configs.get(platform.lower(), {})
        return {**base_config, **platform_config}
        
    @classmethod
    def get_proxy_config(cls, platform: str) -> Dict[str, Any]:
        """
        Get proxy configuration for specific platform.
        
        Args:
            platform: Platform name (eventbrite, meetup, facebook)
            
        Returns:
            Proxy configuration dictionary
        """
        return {
            'proxy_pool': 'residential',
            'proxy_country': 'US',
            'proxy_session': True,  # Maintain session with same IP
            'proxy_timeout': 30,
            'proxy_retry': True
        } 