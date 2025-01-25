"""
Scraper configuration management.

This module provides platform-specific configurations for different event sites,
optimizing settings for each platform's anti-bot measures and requirements.
"""

from typing import Dict, Any
import json
import base64

class ScraperConfig:
    """Platform-specific scraper configurations"""
    
    # Eventbrite specific settings
    EVENTBRITE_CONFIG = {
        'country': 'US',
        'asp': 'true',
        'render_js': 'true',
        'proxy_pool': 'residential',  # Use residential proxies for better success
        'session': 'true',
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
            'mobile': 'false'
        }
    }
    
    # Meetup specific settings
    MEETUP_CONFIG = {
        'country': 'US',
        'asp': 'true',
        'render_js': 'true',
        'proxy_pool': 'residential',
        'session': 'true',
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
            'mobile': 'false'
        }
    }
    
    # Facebook specific settings
    FACEBOOK_CONFIG = {
        'country': 'US',
        'asp': 'true',
        'render_js': 'true',
        'proxy_pool': 'residential',
        'session': 'true',
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
            'mobile': 'false'
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
        
        # Create JavaScript scenario as a list of actions
        js_scenario = [
            {'wait': 2000},  # Initial wait for page load
            {'execute': 'window.scrollTo(0, 800);'},  # Scroll down
            {'wait': 1000},  # Wait for dynamic content
            {'execute': 'window.scrollTo(0, 1600);'},  # Scroll more
            {'wait': 1000}  # Final wait
        ]
        
        # Base64 encode the JavaScript scenario
        js_scenario_json = json.dumps(js_scenario)
        js_scenario_base64 = base64.b64encode(js_scenario_json.encode()).decode()
        
        base_config = {
            'retry_enabled': 'true',
            'retry_count': 2,
            'cookies_enabled': 'true',
            'js_scenario': js_scenario_base64
        }
        
        platform_config = configs.get(platform.lower(), {})
        config = {**base_config, **platform_config}
        
        # Convert any remaining boolean values to strings
        for key, value in config.items():
            if key == 'headers' and isinstance(value, dict):
                config[key] = json.dumps(value)
            elif key == 'browser_settings' and isinstance(value, dict):
                cls._convert_bools_to_strings(value)
                config[key] = json.dumps(value)
            elif isinstance(value, bool):
                config[key] = str(value).lower()
                    
        return config
        
    @classmethod
    def _convert_bools_to_strings(cls, data: Dict[str, Any]) -> None:
        """Convert boolean values to strings in a dictionary recursively"""
        for key, value in data.items():
            if isinstance(value, bool):
                data[key] = str(value).lower()
            elif isinstance(value, dict):
                cls._convert_bools_to_strings(value)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, bool):
                        value[i] = str(item).lower()
                    elif isinstance(item, dict):
                        cls._convert_bools_to_strings(item)
        
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
            'proxy_session': 'true',  # Maintain session with same IP
            'proxy_timeout': 30,
            'proxy_retry': 'true'
        } 