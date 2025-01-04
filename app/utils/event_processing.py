import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from app.utils.schema import Event, Location, PriceInfo, SourceInfo, ImageAnalysis
from app.utils.category_hierarchy import (
    create_default_hierarchy,
    extract_categories,
    map_platform_categories
)
from app.utils.location_services import geocode_address
from app.utils.image_processing import ImageProcessor
import re
from collections import Counter
import spacy
from transformers import pipeline
import torch
import subprocess
import os

logger = logging.getLogger(__name__)

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    logger.warning("Downloading spaCy model...")
    subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
    nlp = spacy.load("en_core_web_sm")

# Initialize sentiment analysis pipeline
sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    device=0 if torch.cuda.is_available() else -1
)

# Initialize category hierarchy
category_hierarchy = create_default_hierarchy()

# Initialize image processor
image_processor = ImageProcessor(os.getenv("GCS_PROCESSED_BUCKET", "prysm-processed-data"))

async def process_images(event: Event) -> Event:
    """Process and analyze event images"""
    if not event.images:
        return event
    
    # Process each image
    for i, image_url in enumerate(event.images):
        try:
            # Download image
            image_data = await image_processor.download_image(image_url)
            if not image_data:
                continue
            
            # Store image
            stored_url = await image_processor.store_image(image_data, event.event_id, i)
            
            # Analyze image
            analysis_results = await image_processor.analyze_image(image_data)
            
            # Create ImageAnalysis object
            image_analysis = ImageAnalysis(
                url=image_url,
                stored_url=stored_url,
                scene_classification=analysis_results.get("scene_classification", {}),
                crowd_density=analysis_results.get("crowd_density", {}),
                objects=analysis_results.get("objects", []),
                safe_search=analysis_results.get("safe_search", {})
            )
            
            event.image_analysis.append(image_analysis)
            
        except Exception as e:
            logger.error(f"Error processing image {image_url}: {str(e)}")
            continue
    
    return event

def clean_event_data(event: Event) -> Event:
    """Clean and standardize event data"""
    # Clean title and description
    event.title = event.title.strip()
    event.description = event.description.strip()
    
    # Generate short description if not present
    if not event.short_description:
        event.short_description = event.description[:200] + "..." if len(event.description) > 200 else event.description
    
    # Standardize location data
    event.location.city = event.location.city.strip().title()
    event.location.state = event.location.state.strip().upper()
    event.location.country = event.location.country.strip().title()
    
    # Geocode address if coordinates are not set
    if event.location.coordinates['lat'] == 0.0 and event.location.coordinates['lng'] == 0.0:
        coords = geocode_address(
            event.location.address,
            event.location.city,
            event.location.state,
            event.location.country
        )
        if coords:
            event.location.coordinates = coords
    
    # Ensure end_datetime is set
    if not event.end_datetime:
        event.end_datetime = event.start_datetime + timedelta(hours=3)
    
    return event

def extract_topics(text: str) -> List[str]:
    """Extract topics from event text using spaCy NLP"""
    # Use new category extraction
    categories = extract_categories(text, category_hierarchy)
    return list(categories)

def calculate_sentiment_scores(text: str) -> Dict[str, float]:
    """Calculate sentiment scores for text"""
    try:
        # Split text into chunks if it's too long (model max length is typically 512 tokens)
        max_chunk_length = 450  # Leave some room for special tokens
        chunks = [text[i:i + max_chunk_length] for i in range(0, len(text), max_chunk_length)]
        
        # Get sentiment for each chunk
        sentiments = []
        for chunk in chunks:
            if chunk.strip():  # Only process non-empty chunks
                result = sentiment_analyzer(chunk)[0] # type: ignore
                sentiments.append(result)
        
        if not sentiments:
            return {'positive': 0.5, 'negative': 0.5}
        
        # Calculate average sentiment
        positive_score = sum(
            s['score'] for s in sentiments if s['label'] == 'POSITIVE'
        ) / len(sentiments)
        negative_score = sum(
            s['score'] for s in sentiments if s['label'] == 'NEGATIVE'
        ) / len(sentiments)
        
        # Normalize scores to sum to 1.0
        total = positive_score + negative_score
        if total == 0:
            return {'positive': 0.5, 'negative': 0.5}
            
        return {
            'positive': round(positive_score / total, 3),
            'negative': round(negative_score / total, 3)
        }
        
    except Exception as e:
        logger.error(f"Error calculating sentiment scores: {str(e)}")
        return {'positive': 0.5, 'negative': 0.5}

async def enrich_event(event: Event) -> Event:
    """Enrich event with additional data"""
    # Process images first as it might provide useful context
    event = await process_images(event)
    
    # Extract topics and categories from title and description
    text = f"{event.title} {event.description}"
    
    # Extract categories from text
    text_categories = extract_categories(text, category_hierarchy)
    
    # Add categories from image analysis if available
    for analysis in event.image_analysis:
        # Add scene categories if confidence is high enough
        for scene, confidence in analysis.scene_classification.items():
            if confidence > 0.7:  # Only use high-confidence scenes
                text_categories.add(scene.lower())
        
        # Add object categories
        for obj in analysis.objects:
            if obj["confidence"] > 0.7:  # Only use high-confidence objects
                text_categories.add(obj["name"].lower())
    
    # Map platform categories if available
    platform_categories = []
    if hasattr(event, 'raw_categories') and event.raw_categories:
        platform_categories = map_platform_categories(
            event.source.platform,
            event.raw_categories,
            category_hierarchy
        )
    
    # Combine categories from all sources
    all_categories = text_categories.union(platform_categories)
    
    # Update event with hierarchical categories
    for category in all_categories:
        event.add_category(category, category_hierarchy)
    
    # Set extracted topics (can include things not in our hierarchy)
    event.extracted_topics = list(text_categories)
    
    # Calculate sentiment scores
    event.sentiment_scores = calculate_sentiment_scores(text)
    
    # Process text with spaCy for additional insights
    doc = nlp(text)
    
    # Set indoor/outdoor attribute using both text and image analysis
    outdoor_indicators = ['outdoor', 'outside', 'park', 'garden', 'field', 'beach']
    indoor_indicators = ['indoor', 'inside', 'hall', 'room', 'venue', 'theater']
    
    outdoor_score = sum(1 for token in doc if any(ind in token.text.lower() for ind in outdoor_indicators))
    indoor_score = sum(1 for token in doc if any(ind in token.text.lower() for ind in indoor_indicators))
    
    # Add scores from image analysis
    for analysis in event.image_analysis:
        scenes = analysis.scene_classification
        if any(scene.lower() in outdoor_indicators for scene in scenes):
            outdoor_score += 2
        if any(scene.lower() in indoor_indicators for scene in scenes):
            indoor_score += 2
    
    if outdoor_score > indoor_score:
        event.attributes.indoor_outdoor = 'outdoor'
    elif indoor_score > outdoor_score:
        event.attributes.indoor_outdoor = 'indoor'
    elif outdoor_score > 0 and indoor_score > 0:
        event.attributes.indoor_outdoor = 'both'
    else:
        event.attributes.indoor_outdoor = 'indoor'  # Default to indoor
    
    # Extract age restrictions
    age_patterns = [
        (r'\b21\+\b|\b21\s*and\s*over\b|\btwenty-one\s*and\s*over\b', '21+'),
        (r'\b18\+\b|\b18\s*and\s*over\b|\beighteen\s*and\s*over\b', '18+'),
        (r'\ball\s*ages\b|\bfamily\s*friendly\b', 'all')
    ]
    
    for pattern, restriction in age_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            event.attributes.age_restriction = restriction
            break
    
    # Extract accessibility features
    accessibility_patterns = {
        'wheelchair': [r'wheelchair\s*accessible', r'wheelchair\s*friendly', r'ada\s*compliant'],
        'hearing_assistance': [r'hearing\s*loop', r'audio\s*assistance', r'hearing\s*aid'],
        'sign_language': [r'sign\s*language', r'asl', r'interpreter'],
        'visual_assistance': [r'visual\s*aid', r'braille', r'sight\s*assistance']
    }
    
    for feature, patterns in accessibility_patterns.items():
        if any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns):
            if feature not in event.attributes.accessibility_features:
                event.attributes.accessibility_features.append(feature)
    
    return event

def classify_event(event: Event) -> Event:
    """Classify event based on various attributes"""
    # Classify price tier
    avg_price = (event.price_info.min_price + event.price_info.max_price) / 2
    
    if event.price_info.min_price == 0:
        event.price_info.price_tier = 'free'
    elif avg_price < 20:
        event.price_info.price_tier = 'budget'
    elif avg_price < 50:
        event.price_info.price_tier = 'medium'
    else:
        event.price_info.price_tier = 'premium'
    
    # Extract dress code using NLP
    text = event.description.lower()
    
    formal_indicators = ['formal', 'black tie', 'evening wear', 'cocktail attire']
    business_indicators = ['business', 'smart casual', 'business casual', 'semi-formal']
    
    if any(indicator in text for indicator in formal_indicators):
        event.attributes.dress_code = 'formal'
    elif any(indicator in text for indicator in business_indicators):
        event.attributes.dress_code = 'business'
    else:
        event.attributes.dress_code = 'casual'
    
    return event

async def process_event(event: Event) -> Event:
    """Process an event through the complete pipeline"""
    event = clean_event_data(event)
    event = await enrich_event(event)
    event = classify_event(event)
    return event 