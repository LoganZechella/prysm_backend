from typing import Dict, Any
import os
import yaml
from pathlib import Path

class TestConfig:
    def __init__(self):
        self.config = self._load_test_config()
        self.test_data = self._load_test_data()
    
    def _load_test_config(self) -> Dict[str, Any]:
        """Load test environment configuration"""
        config_path = Path(__file__).parent / 'test_config.yaml'
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def _load_test_data(self) -> Dict[str, Any]:
        """Load test data sets"""
        data = {}
        for data_type, path in self.config['test_data'].items():
            if os.path.exists(path):
                with open(path) as f:
                    data[data_type] = yaml.safe_load(f)
        return data

    def get_database_config(self) -> Dict[str, str]:
        """Get database configuration"""
        return self.config['database']

    def get_mock_service_config(self, service_name: str) -> Dict[str, Any]:
        """Get configuration for a specific mock service"""
        return self.config['mock_services'].get(service_name, {})

    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration"""
        return self.config['monitoring'] 