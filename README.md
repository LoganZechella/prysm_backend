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
META_ACCESS_TOKEN=your_token_here
GOOGLE_MAPS_API_KEY=your_key_here  # Required for geocoding
GOOGLE_CLOUD_PROJECT=your_project_id  # Required for Google Cloud services
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json  # Required for Google Cloud services
GCS_RAW_BUCKET=prysm-raw-data  # Google Cloud Storage bucket for raw data
GCS_PROCESSED_BUCKET=prysm-processed-data  # Google Cloud Storage bucket for processed data
```

3. Set up Google Cloud services:
   - Create a Google Cloud project
   - Enable the following APIs:
     - Google Cloud Storage
     - Google Cloud Vision API
   - Create a service account with access to these services
   - Download the service account key JSON file

4. Run the development server:

```bash
uvicorn app.main:app --reload
```

## Features

- Event collection from multiple sources (Meta, Eventbrite, etc.)
- Advanced event processing:
  - NLP-based topic extraction
  - Sentiment analysis
  - Location geocoding
  - Event classification
  - Price tier analysis
  - Image processing and analysis:
    - Scene classification
    - Crowd density estimation
    - Object detection
    - Safe search analysis
- RESTful API for event data access
- Google Cloud Storage integration

## Image Processing

The system includes advanced image processing capabilities powered by machine learning:

- **Scene Classification**: Identifies the type of venue or setting (e.g., concert hall, stadium, conference room)
- **Crowd Density**: Estimates crowd size and density for better capacity planning
- **Object Detection**: Identifies relevant objects and features in event images
- **Safe Search**: Ensures image content is appropriate and safe

Images are processed using:

- ResNet-50 for scene classification
- Google Cloud Vision API for object detection and safe search
- Custom crowd density estimation

Processed images and their analysis results are stored in Google Cloud Storage for efficient access.

## API Documentation

Once the server is running, visit:

- Swagger UI: <http://localhost:8000/docs>
- ReDoc: <http://localhost:8000/redoc>
