# Prysm Backend

Event collection and processing backend for Prysm.

## Setup

1. Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

2. Set up environment variables in `.env`:
```
EVENTBRITE_API_KEY=your_key_here
GOOGLE_MAPS_API_KEY=your_key_here  # Required for geocoding
```

3. Run the development server:
```bash
uvicorn app.main:app --reload
```

## Features

- Event collection from multiple sources (Eventbrite, etc.)
- Advanced event processing:
  - NLP-based topic extraction
  - Sentiment analysis
  - Location geocoding
  - Event classification
  - Price tier analysis
- RESTful API for event data access
- Google Cloud Storage integration

## API Documentation

Once the server is running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc