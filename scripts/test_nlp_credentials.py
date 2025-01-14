#!/usr/bin/env python3
"""
Test script to verify Google Cloud Natural Language API credentials and access.
"""

import sys
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from app.services.nlp_service import NLPService

def test_nlp_connection():
    """Test NLP service connection and basic functionality"""
    try:
        # Initialize service
        service = NLPService()
        
        # Test text to analyze
        test_text = "Google Cloud Platform is amazing! I love using their services."
        
        print("Testing NLP service connection...")
        print(f"Analyzing text: '{test_text}'")
        
        # Analyze text
        result = service.analyze_text(test_text)
        
        print("\nAnalysis successful! Results:")
        print("\nEntities found:")
        for entity in result['entities']:
            print(f"- {entity['name']} ({entity['type']}): {entity.get('salience', 'N/A')}")
        
        print("\nSentiment analysis:")
        sentiment = result['sentiment']['document_sentiment']
        print(f"- Score: {sentiment['score']:.2f}")
        print(f"- Magnitude: {sentiment['magnitude']:.2f}")
        
        print("\nCredentials and API access working correctly!")
        return True
        
    except Exception as e:
        print(f"\nError testing NLP service: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_nlp_connection()
    sys.exit(0 if success else 1) 