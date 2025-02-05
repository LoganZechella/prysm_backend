"""Service for transforming user, event, and interaction data for Amazon Personalize."""
import json
import hashlib
import re
import html
import unicodedata
from datetime import datetime
from typing import Dict, Any, Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.traits import Traits
from app.models.event import EventModel
from app.models.feedback import ImplicitFeedback, UserFeedback

import logging
logger = logging.getLogger(__name__)

class PersonalizeTransformer:
    """Service for transforming data into Amazon Personalize format."""

    def __init__(self, db: AsyncSession):
        """Initialize the transformer service."""
        self.db = db
        self.required_fields = {
            'id': 'Event ID is required',
            'title': 'Event title is required'
        }
        
        # Standard category mappings
        self.category_keywords = {
            'music': ['concert', 'festival', 'band', 'dj', 'live music', 'performance'],
            'tech': ['hackathon', 'workshop', 'conference', 'meetup', 'coding'],
            'sports': ['game', 'match', 'tournament', 'competition', 'race'],
            'arts': ['exhibition', 'gallery', 'museum', 'theater', 'performance'],
            'food': ['tasting', 'dinner', 'culinary', 'restaurant', 'food'],
            'education': ['class', 'workshop', 'seminar', 'lecture', 'training'],
            'business': ['networking', 'conference', 'meetup', 'summit', 'expo'],
            'social': ['party', 'meetup', 'gathering', 'social', 'mixer'],
            'outdoor': ['hiking', 'camping', 'adventure', 'outdoor', 'nature']
        }

    def _generate_safe_item_id(self, event_id: int, title: str = "") -> str:
        """Generate a safe ITEM_ID that is guaranteed to be under 256 characters.
        
        If the event_id string is short enough, use it directly.
        Otherwise, generate a deterministic hash using both id and title.
        
        Args:
            event_id: The numeric ID of the event
            title: Optional title to use in hash generation for better uniqueness
            
        Returns:
            A string ID that is guaranteed to be under 256 characters
        """
        # Convert event_id to string
        str_id = str(event_id)
        
        # If the ID is already short enough, just use it
        if len(str_id) < 256:
            return str_id
            
        # Otherwise, create a hash using both id and title
        # This ensures uniqueness even if two events have the same ID
        hash_input = f"{event_id}:{title}"
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()[:64]  # 64-char hex string

    def _infer_categories(self, title: str, description: str = "") -> List[str]:
        """Infer categories from event title and description.
        
        Args:
            title: Event title
            description: Event description
            
        Returns:
            List of inferred categories
        """
        # Combine title and description for better inference
        text = f"{title} {description}".lower()
        
        # Find matching categories
        matches = []
        for category, keywords in self.category_keywords.items():
            if any(keyword in text for keyword in keywords):
                matches.append(category)
                
        # If no matches found, use a default category
        if not matches:
            matches = ['other']
            
        return matches

    def _clean_text_field(self, text: str, default: str = "") -> str:
        """Clean and normalize text fields for Personalize.
        
        Args:
            text: Raw text to clean
            default: Default value if text is empty
            
        Returns:
            Cleaned and normalized text
        """
        if not text:
            return default
            
        # Convert to string if not already
        text = str(text)
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove markdown
        text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)  # Remove links
        text = re.sub(r'[*_~]{1,2}([^*_~]+)[*_~]{1,2}', r'\1', text)  # Remove bold/italic
        text = re.sub(r'#{1,6}\s+', '', text)  # Remove headers
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        # Normalize unicode characters
        text = unicodedata.normalize('NFKC', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text if text else default

    def _validate_event_data(self, event: EventModel) -> None:
        """Validate required fields are present and properly formatted.
        
        Args:
            event: Event model to validate
            
        Raises:
            ValueError: If required fields are missing or invalid
        """
        errors = []
        
        for field, message in self.required_fields.items():
            if not getattr(event, field, None):
                errors.append(message)
                
        if errors:
            raise ValueError(f"Event validation failed: {'; '.join(errors)}")

    def _process_categories(self, event: EventModel) -> tuple[str, str]:
        """Process categories into primary and secondary categories.
        
        Args:
            event: Event model to process
            
        Returns:
            Tuple of (primary_category, secondary_category)
        """
        # Start with existing categories if available
        categories = []
        if event.categories and isinstance(event.categories, list):
            categories = [cat.strip().lower() for cat in event.categories if cat and cat.strip()]
            
        # If no valid categories, try to infer them
        if not categories:
            categories = self._infer_categories(
                event.title or "",
                event.description or ""
            )
            
        # Ensure we have at least one category
        if not categories:
            categories = ['other']
            
        # Map to primary and secondary
        primary = categories[0]
        secondary = categories[1] if len(categories) > 1 else primary
        
        return primary.capitalize(), secondary.capitalize()

    def _process_price(self, price_info: Any) -> float:
        """Process price information into a normalized float value.
        
        Args:
            price_info: Raw price information
            
        Returns:
            Normalized price as float
        """
        try:
            if isinstance(price_info, dict):
                return float(price_info.get("min_price", 0))
            elif isinstance(price_info, str):
                price_data = json.loads(price_info)
                return float(price_data.get("min_price", 0))
            elif isinstance(price_info, (int, float)):
                return float(price_info)
            return 0.0
        except (json.JSONDecodeError, ValueError, TypeError):
            return 0.0

    async def transform_user_data(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Transform user data into Personalize format.
        
        Required fields for ECOMMERCE domain:
        - USER_ID (string)
        - GENDER (string, categorical)
        - AGE_GROUP (string, categorical)
        - INTERESTS (string, metadata)
        - PROFESSIONAL_FIELD (string, metadata)
        - BEHAVIOR_PATTERNS (string, metadata)
        """
        try:
            # Get user traits
            result = await self.db.execute(
                select(Traits).where(Traits.user_id == user_id)
            )
            traits = result.scalar_one_or_none()
            
            if not traits:
                logger.warning(f"No traits found for user {user_id}")
                return None

            # Extract traits
            music_traits = traits.music_traits or {}
            social_traits = traits.social_traits or {}
            professional_traits = traits.professional_traits or {}
            behavior_traits = traits.behavior_traits or {}

            # Process professional traits
            professional_field = {}
            if professional_traits:
                # Combine LinkedIn and Google professional data
                linkedin_data = professional_traits.get('linkedin', {})
                google_data = professional_traits.get('google', {})
                
                professional_field = {
                    "industries": list(set(
                        linkedin_data.get('industries', []) +
                        [org.get('name', '') for org in google_data.get('organizations', [])]
                    )),
                    "job_titles": linkedin_data.get('job_titles', []),
                    "skills": google_data.get('skills', []),
                    "experience_years": linkedin_data.get('experience_years')
                }

            # Process interests
            interests = {
                "music": list(music_traits.get('genres', {}).keys()),
                "professional": professional_field.get('industries', []),
                "general": google_data.get('interests', []) if 'google' in professional_traits else []
            }

            # Process behavior patterns
            behavior_data = {
                "listening_patterns": behavior_traits.get('listening_times', {}),
                "discovery_ratio": behavior_traits.get('discovery_ratio', 0.0),
                "engagement_level": behavior_traits.get('engagement_level', 0.0),
                "social_score": social_traits.get('social_score', 0.0)
            }

            # Map gender (if available)
            gender = "Unknown"
            if 'google' in professional_traits:
                google_gender = professional_traits['google'].get('genders', [{}])[0].get('value', '').lower()
                if google_gender in ['male', 'female', 'other']:
                    gender = google_gender.capitalize()

            # Map age group
            age_group = "Unknown"
            if 'google' in professional_traits and 'birthdays' in professional_traits['google']:
                try:
                    birth_year = professional_traits['google']['birthdays'][0].get('date', {}).get('year')
                    if birth_year:
                        age = datetime.now().year - birth_year
                        if age < 18:
                            age_group = "Under 18"
                        elif age < 25:
                            age_group = "18-24"
                        elif age < 35:
                            age_group = "25-34"
                        elif age < 45:
                            age_group = "35-44"
                        elif age < 55:
                            age_group = "45-54"
                        else:
                            age_group = "55+"
                except (KeyError, IndexError, TypeError):
                    pass

            # Ensure all fields are properly formatted for Personalize
            return {
                "USER_ID": user_id,
                "GENDER": gender,
                "AGE_GROUP": age_group,
                "INTERESTS": json.dumps(interests) if interests else "{}",
                "PROFESSIONAL_FIELD": json.dumps(professional_field) if professional_field else "{}",
                "BEHAVIOR_PATTERNS": json.dumps(behavior_data) if behavior_data else "{}"
            }

        except Exception as e:
            logger.error(f"Error transforming user data for {user_id}: {str(e)}")
            return None

    async def transform_event_data(self, event: EventModel) -> Optional[Dict[str, Any]]:
        """Transform event data into Personalize format."""
        try:
            # Validate required fields
            self._validate_event_data(event)
            
            # Generate safe ITEM_ID
            item_id = self._generate_safe_item_id(event.id, event.title or "")
            
            # Process categories with inference
            category_l1, category_l2 = self._process_categories(event)
            
            # Clean description with default
            description = self._clean_text_field(
                event.description,
                default=f"Event: {self._clean_text_field(event.title)}"
            )
            if len(description) > 1000:
                description = description[:997] + "..."
                
            # Process price
            price = self._process_price(event.price_info)
            
            # Clean location data
            venue_city = self._clean_text_field(event.venue_city, "Unknown")
            venue_state = self._clean_text_field(event.venue_state, "Unknown")
            
            # Handle nullable fields with safe defaults
            is_online = bool(event.is_online) if event.is_online is not None else False
            technical_level = float(event.technical_level or 0.0)
            creation_timestamp = int(event.created_at.timestamp()) if event.created_at else int(datetime.now().timestamp())
            
            return {
                "ITEM_ID": item_id,
                "CATEGORY_L1": category_l1,
                "CATEGORY_L2": category_l2,
                "PRICE": price,
                "DESCRIPTION": description,
                "VENUE_CITY": venue_city,
                "VENUE_STATE": venue_state,
                "IS_ONLINE": is_online,
                "TECHNICAL_LEVEL": technical_level,
                "CREATION_TIMESTAMP": creation_timestamp
            }

        except ValueError as ve:
            logger.error(f"Validation error for event {getattr(event, 'id', 'unknown')}: {str(ve)}")
            return None
        except Exception as e:
            logger.error(f"Error transforming event data for event {getattr(event, 'id', 'unknown')}: {str(e)}")
            return None

    async def transform_interaction_data(
        self,
        user_id: str,
        event_id: str,
        event_type: str,
        event_value: Optional[float] = None,
        timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Transform interaction data into Personalize format.
        
        Required fields:
        - USER_ID (string)
        - ITEM_ID (string)
        - EVENT_TYPE (string)
        - EVENT_VALUE (float, optional)
        - TIMESTAMP (long)
        """
        return {
            "USER_ID": user_id,
            "ITEM_ID": str(event_id),
            "EVENT_TYPE": event_type,
            "EVENT_VALUE": event_value,
            "TIMESTAMP": int((timestamp or datetime.utcnow()).timestamp())
        }

    async def get_all_user_data(self) -> List[Dict[str, Any]]:
        """Get transformed data for all users."""
        result = await self.db.execute(select(Traits))
        traits = result.scalars().all()
        
        transformed_data = []
        for trait in traits:
            if user_data := await self.transform_user_data(trait.user_id):
                transformed_data.append(user_data)
        
        return transformed_data

    async def get_all_event_data(self) -> List[Dict[str, Any]]:
        """Get transformed data for all events."""
        result = await self.db.execute(select(EventModel))
        events = result.scalars().all()
        
        transformed_data = []
        for event in events:
            if event_data := await self.transform_event_data(event):
                transformed_data.append(event_data)
        
        return transformed_data

    async def get_all_interaction_data(self) -> List[Dict[str, Any]]:
        """Get transformed data for all interactions."""
        # Get implicit feedback
        implicit_result = await self.db.execute(select(ImplicitFeedback))
        implicit_feedback = implicit_result.scalars().all()
        
        # Get explicit feedback
        explicit_result = await self.db.execute(select(UserFeedback))
        explicit_feedback = explicit_result.scalars().all()
        
        transformed_data = []
        
        # Transform implicit feedback
        for feedback in implicit_feedback:
            if feedback.view_count > 0:
                transformed_data.append(
                    await self.transform_interaction_data(
                        feedback.user_id,
                        str(feedback.event_id),
                        "view",
                        float(feedback.view_duration or 0.0),
                        feedback.created_at
                    )
                )
            if feedback.click_count > 0:
                transformed_data.append(
                    await self.transform_interaction_data(
                        feedback.user_id,
                        str(feedback.event_id),
                        "click",
                        float(feedback.click_count),
                        feedback.created_at
                    )
                )
            if feedback.share_count > 0:
                transformed_data.append(
                    await self.transform_interaction_data(
                        feedback.user_id,
                        str(feedback.event_id),
                        "share",
                        float(feedback.share_count),
                        feedback.created_at
                    )
                )
        
        # Transform explicit feedback
        for feedback in explicit_feedback:
            transformed_data.append(
                await self.transform_interaction_data(
                    feedback.user_id,
                    str(feedback.event_id),
                    "rating",
                    float(feedback.rating),
                    feedback.created_at
                )
            )
        
        return transformed_data 