from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import uvicorn
from .recommendation_engine import RecommendationEngine

app = FastAPI(
    title="Prysm Recommendation Engine",
    description="AI-powered recommendation engine processing data from multiple sources",
    version="1.0.0"
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Initialize recommendation engine
engine = RecommendationEngine(
    project_id="prysm-backend",  # Replace with your project ID
    dataset_id="prysm_recommendations"
)

class Recommendation(BaseModel):
    id: str
    type: str
    title: str
    description: str
    confidence: float
    source: str
    metadata: Dict[str, Any]

class RecommendationResponse(BaseModel):
    recommendations: Dict[str, List[Dict[str, Any]]]
    user_id: str
    timestamp: str

class FeedbackRequest(BaseModel):
    recommendation_id: str
    feedback_type: str
    feedback_text: Optional[str] = None

@app.get("/")
async def root():
    return {"message": "Welcome to Prysm Recommendation Engine"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/recommendations", response_model=RecommendationResponse)
async def get_recommendations(
    user_id: str,
    category: Optional[str] = None,
    limit: int = 10,
    token: str = Depends(oauth2_scheme)
):
    try:
        recommendations = engine.get_recommendations(user_id, category, limit)
        return recommendations
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/feedback")
async def submit_feedback(
    feedback: FeedbackRequest,
    user_id: str,
    token: str = Depends(oauth2_scheme)
):
    try:
        engine.record_feedback(
            user_id=user_id,
            recommendation_id=feedback.recommendation_id,
            feedback_type=feedback.feedback_type,
            feedback_text=feedback.feedback_text
        )
        
        # Update user preferences based on feedback
        engine.update_user_preferences(user_id)
        
        return {"status": "success", "message": "Feedback recorded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080) 