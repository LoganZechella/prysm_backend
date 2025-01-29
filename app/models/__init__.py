"""Models package initialization."""

from app.database import Base
from .event import EventModel

__all__ = ['Base', 'EventModel']
