from sqlalchemy import Column, Integer, String, JSON
from app.db.base_class import Base


class GoogleProfile(Base):
    __tablename__ = 'google_profiles'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True, nullable=False)
    profile_data = Column(JSON, nullable=True) 