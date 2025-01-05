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
    # Process text with spaCy
    doc = nlp(text.lower())
    
    # Extract topics using NER and noun chunks
    topics = set()
    
    # Add named entities
    for ent in doc.ents:
        if ent.label_ in ["ORG", "PRODUCT", "EVENT", "WORK_OF_ART"]:
            topics.add(ent.text)
    
    # Add noun chunks
    for chunk in doc.noun_chunks:
        topics.add(chunk.root.text)
    
    # Define topic mappings
    topic_mappings = {
        "tech": "technology",
        "ai": "artificial intelligence",
        "ml": "machine learning",
        "ds": "data science"
    }
    
    # Add technology-related keywords
    tech_keywords = {
        "technology", "workshop", "coding", "programming",
        "software", "hardware", "digital", "computer", "ai",
        "artificial intelligence", "machine learning", "data science"
    }
    
    # Check for tech-related words and apply mappings
    text_words = set(text.lower().split())
    for word in text_words:
        if word in topic_mappings:
            topics.add(topic_mappings[word])
        if word in tech_keywords:
            topics.add(word)
    
    # Use category extraction for additional topics
    categories = extract_categories(text, category_hierarchy)
    topics.update(categories)
    
    return list(topics)

def calculate_sentiment_scores(text: str) -> Dict[str, float]:
    """Calculate sentiment scores for text with improved chunk processing and error handling"""
    if not text or not text.strip():
        logger.warning("Empty text provided for sentiment analysis")
        return {'positive': 0.5, 'negative': 0.5}
    
    try:
        # Clean and normalize text
        text = ' '.join(text.split())  # Normalize whitespace
        
        # Split text into chunks if it's too long (model max length is typically 512 tokens)
        max_chunk_length = 450  # Leave room for special tokens
        chunks = [text[i:i + max_chunk_length] for i in range(0, len(text), max_chunk_length)]
        
        # Get sentiment for each chunk
        chunk_sentiments = []
        for i, chunk in enumerate(chunks):
            if not chunk.strip():
                continue
                
            try:
                result = sentiment_analyzer(chunk)
                if not result or not isinstance(result, list) or not result[0]:
                    continue
                    
                sentiment = result[0]
                if not isinstance(sentiment, dict) or 'score' not in sentiment or 'label' not in sentiment:
                    continue
                    
                chunk_sentiments.append({
                    'score': float(sentiment['score']),
                    'label': str(sentiment['label']),
                    'weight': len(chunk) / len(text)  # Weight by chunk length
                })
            except Exception as e:
                logger.error(f"Error analyzing chunk {i}: {str(e)}")
                continue
        
        if not chunk_sentiments:
            logger.warning("No valid sentiment results obtained")
            return {'positive': 0.5, 'negative': 0.5}
        
        # Calculate weighted average sentiment
        total_positive = 0.0
        total_weight = 0.0
        
        for sentiment in chunk_sentiments:
            weight = sentiment['weight']
            if sentiment['label'] == 'POSITIVE':
                total_positive += sentiment['score'] * weight
            total_weight += weight
        
        if total_weight == 0:
            return {'positive': 0.5, 'negative': 0.5}
        
        # Calculate final scores
        positive_score = total_positive / total_weight
        
        # Round to 3 decimal places
        return {
            'positive': round(positive_score, 3),
            'negative': round(1 - positive_score, 3)
        }
        
    except Exception as e:
        logger.error(f"Error in sentiment analysis: {str(e)}")
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
        (r'\b21\+\b|\b21\s*and\s*over\b|\btwenty-one\s*and\s*over\b|\b21\s*only\b', '21+'),
        (r'\b18\+\b|\b18\s*and\s*over\b|\beighteen\s*and\s*over\b', '18+'),
        (r'\ball\s*ages\b|\bfamily\s*friendly\b', 'all')
    ]
    
    # Default to 'all' unless a restriction is found
    event.attributes.age_restriction = 'all'
    
    # Check for age restrictions in both title and description
    text = f"{event.title} {event.description}".lower()
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
    """Classify event based on various attributes including price tier, venue type, and target audience"""
    # Normalize and classify price tier based on event type and location
    if event.price_info.min_price == 0 and event.price_info.max_price == 0:
        event.price_info.price_tier = 'free'
    else:
        # Calculate average price
        avg_price = (event.price_info.min_price + event.price_info.max_price) / 2
        
        # Adjust price thresholds based on event category
        price_thresholds = {
            'concert': {'budget': 30, 'medium': 75},
            'conference': {'budget': 100, 'medium': 500},
            'workshop': {'budget': 50, 'medium': 200},
            'sports': {'budget': 40, 'medium': 100},
            'theater': {'budget': 35, 'medium': 80},
            'default': {'budget': 25, 'medium': 75}
        }
        
        # Determine event type from categories
        event_type = 'default'
        for category in event.categories:
            if category.lower() in price_thresholds:
                event_type = category.lower()
                break
        
        thresholds = price_thresholds[event_type]
        
        # Apply location-based adjustment
        major_cities = {'new york', 'san francisco', 'los angeles', 'chicago', 'miami', 'boston'}
        if event.location.city.lower() in major_cities:
            thresholds = {k: v * 1.5 for k, v in thresholds.items()}
        
        # Classify price tier
        if avg_price <= thresholds['budget']:
            event.price_info.price_tier = 'budget'
        elif avg_price <= thresholds['medium']:
            event.price_info.price_tier = 'medium'
        else:
            event.price_info.price_tier = 'premium'
    
    # Extract dress code using NLP and event type
    text = f"{event.title} {event.description}".lower()
    
    formal_indicators = {
        'black tie', 'formal attire', 'evening wear', 'cocktail dress',
        'formal dress', 'tuxedo', 'gala', 'ball', 'formal dinner'
    }
    business_indicators = {
        'business casual', 'smart casual', 'business attire', 'semi-formal',
        'professional attire', 'business dress', 'conference', 'networking'
    }
    
    if any(indicator in text for indicator in formal_indicators):
        event.attributes.dress_code = 'formal'
    elif any(indicator in text for indicator in business_indicators):
        event.attributes.dress_code = 'business'
    else:
        event.attributes.dress_code = 'casual'
    
    # Set target audience based on content analysis
    audience_indicators = {
        'family': {'family friendly', 'all ages', 'kids', 'children', 'parent'},
        'professional': {'networking', 'business', 'career', 'professional', 'industry'},
        'academic': {'student', 'academic', 'research', 'university', 'college'},
        'adult': {'21+', 'adult only', 'mature'}
    }
    
    event.attributes.target_audience = []
    for audience, indicators in audience_indicators.items():
        if any(indicator in text for indicator in indicators):
            event.attributes.target_audience.append(audience)
    
    if not event.attributes.target_audience:
        event.attributes.target_audience = ['general']
    
    return event

async def process_event(event: Event) -> Event:
    """Process and enrich an event with all available data"""
    if not event:
        raise ValueError("Event cannot be None")
    
    try:
        logger.info(f"Starting event processing for event_id: {event.event_id}")
        
        # Clean data first
        try:
            event = clean_event_data(event)
            logger.debug(f"Event {event.event_id} cleaned successfully")
        except Exception as e:
            logger.error(f"Error cleaning event {event.event_id}: {str(e)}")
            # Continue processing even if cleaning fails
        
        # Enrich with additional data
        try:
            event = await enrich_event(event)
            logger.debug(f"Event {event.event_id} enriched successfully")
        except Exception as e:
            logger.error(f"Error enriching event {event.event_id}: {str(e)}")
            # Continue processing even if enrichment partially fails
        
        # Final validation
        if not event.title or not event.description:
            logger.warning(f"Event {event.event_id} missing critical fields after processing")
        
        logger.info(f"Completed event processing for event_id: {event.event_id}")
        return event
        
    except Exception as e:
        logger.error(f"Critical error processing event {event.event_id}: {str(e)}")
        raise 