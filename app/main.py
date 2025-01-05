from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import preferences, auth, recommendations
import os
from supertokens_python import init, InputAppInfo
from supertokens_python.recipe import session
from supertokens_python.recipe.session.framework.fastapi import verify_session
from supertokens_python.framework.fastapi import get_middleware
from supertokens_python.supertokens import SupertokensConfig

app = FastAPI()

# Initialize SuperTokens
init(
    app_info=InputAppInfo(
        app_name="Prysm",
        api_domain=os.getenv("API_DOMAIN", "http://localhost:8000"),
        website_domain=os.getenv("WEBSITE_DOMAIN", "http://localhost:3001"),
        api_base_path="/api/auth"
    ),
    supertokens_config=SupertokensConfig(
        connection_uri=os.getenv("SUPERTOKENS_CONNECTION_URI", "http://localhost:3567"),
        api_key=os.getenv("SUPERTOKENS_API_KEY")
    ),
    framework='fastapi',
    recipe_list=[
        session.init()
    ],
    mode='asgi'
)

# Add SuperTokens middleware
app.add_middleware(get_middleware())

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3001",  # Frontend development server
        os.getenv("WEBSITE_DOMAIN", "http://localhost:3001")
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=["*"],  # Allow all headers for development
    expose_headers=["Content-Type", "Authorization", "anti-csrf"]
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(preferences.router, prefix="/api/preferences", tags=["preferences"])
app.include_router(recommendations.router, prefix="/api/recommendations", tags=["recommendations"]) 