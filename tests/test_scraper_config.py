"""
Tests for scraper configuration management.
"""

import pytest
from app.utils.scraper_config import ScraperConfig

def test_get_config_eventbrite():
    """Test getting Eventbrite configuration"""
    config = ScraperConfig.get_config('eventbrite')
    
    # Check basic settings
    assert config['country'] == 'US'
    assert config['asp'] is True
    assert config['render_js'] is True
    assert config['proxy_pool'] == 'residential'
    
    # Check browser settings
    assert config['browser_emulator'] == 'chrome'
    assert config['browser_settings']['platform'] == 'Windows'
    assert config['browser_settings']['mobile'] is False
    
    # Check headers
    assert 'Accept' in config['headers']
    assert 'Accept-Language' in config['headers']
    assert config['headers']['DNT'] == '1'
    
    # Check JS scenario
    assert 'js_scenario' in config
    assert len(config['js_scenario']['steps']) > 0
    
def test_get_config_meetup():
    """Test getting Meetup configuration"""
    config = ScraperConfig.get_config('meetup')
    
    # Check basic settings
    assert config['country'] == 'US'
    assert config['timeout'] == 30
    assert config['browser_emulator'] == 'firefox'
    
    # Check browser settings
    assert config['browser_settings']['platform'] == 'MacOS'
    assert config['browser_settings']['viewport']['width'] == 1366
    
def test_get_config_facebook():
    """Test getting Facebook configuration"""
    config = ScraperConfig.get_config('facebook')
    
    # Check basic settings
    assert config['timeout'] == 45  # Facebook has longer timeout
    assert config['wait_for'] == '.event-card'
    assert config['wait_timeout'] == 10
    
    # Check browser settings
    assert config['browser_settings']['viewport']['width'] == 1920
    assert config['browser_settings']['viewport']['height'] == 1080
    
def test_get_config_unknown_platform():
    """Test getting configuration for unknown platform"""
    config = ScraperConfig.get_config('unknown_platform')
    
    # Should still get base config
    assert config['retry_enabled'] is True
    assert config['cookies_enabled'] is True
    assert 'js_scenario' in config
    
def test_get_proxy_config():
    """Test getting proxy configuration"""
    config = ScraperConfig.get_proxy_config('eventbrite')
    
    assert config['proxy_pool'] == 'residential'
    assert config['proxy_country'] == 'US'
    assert config['proxy_session'] is True
    assert config['proxy_timeout'] == 30
    assert config['proxy_retry'] is True
    
def test_config_inheritance():
    """Test configuration inheritance and overrides"""
    config = ScraperConfig.get_config('eventbrite')
    
    # Base config settings should be present
    assert config['retry_enabled'] is True
    assert config['cookies_enabled'] is True
    assert 'js_scenario' in config
    
    # Platform specific settings should override base settings
    assert config['proxy_pool'] == 'residential'  # From Eventbrite config
    assert config['browser_emulator'] == 'chrome'  # From Eventbrite config
    
def test_js_scenario_steps():
    """Test JS scenario steps in configuration"""
    config = ScraperConfig.get_config('eventbrite')
    steps = config['js_scenario']['steps']
    
    # Verify step sequence
    assert len(steps) == 5
    assert steps[0]['wait'] == 2000  # Initial wait
    assert steps[1]['scroll_y'] == 800  # First scroll
    assert steps[2]['wait'] == 1000  # Wait after scroll
    assert steps[3]['scroll_y'] == 1600  # Second scroll
    assert steps[4]['wait'] == 1000  # Final wait
    
def test_browser_settings():
    """Test browser emulation settings"""
    eventbrite_config = ScraperConfig.get_config('eventbrite')
    meetup_config = ScraperConfig.get_config('meetup')
    
    # Different platforms should have different browser settings
    assert eventbrite_config['browser_emulator'] != meetup_config['browser_emulator']
    assert eventbrite_config['browser_settings']['platform'] != meetup_config['browser_settings']['platform']
    
    # But both should have viewport settings
    assert 'viewport' in eventbrite_config['browser_settings']
    assert 'viewport' in meetup_config['browser_settings'] 