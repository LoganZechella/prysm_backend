# Event Recommendation System

A FastAPI-based backend service that provides personalized event recommendations based on user preferences and event data.

## Features

- Personalized event recommendations based on:
  - User preferences (categories, location, price range, etc.)
  - Event attributes (indoor/outdoor, accessibility, age restrictions)
  - Time and date preferences
  - Location proximity
- Caching system for improved performance
- JWT-based authentication
- RESTful API endpoints
- Comprehensive test coverage

## Tech Stack

- FastAPI: Modern, fast web framework for building APIs
- Pydantic: Data validation using Python type annotations
- Redis: Caching layer for improved performance
- PostgreSQL: Primary database (through SQLAlchemy)
- JWT: Token-based authentication
- pytest: Testing framework

## Getting Started

### Prerequisites

- Python 3.9+
- Redis server
- PostgreSQL database

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/event-recommendation-system.git
cd event-recommendation-system
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Run the application:
```bash
uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

### Running Tests

```bash
pytest
```

## API Documentation

Once the server is running, you can access the API documentation at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Main Endpoints

- `GET /api/recommendations`: Get personalized event recommendations
- `GET /api/health`: Health check endpoint

## Project Structure

```
app/
├── api/                 # API routes
│   ├── __init__.py
│   └── recommendations.py
├── schemas/            # Pydantic models
│   ├── __init__.py
│   ├── event.py
│   └── user.py
├── services/          # Business logic
│   ├── __init__.py
│   └── recommendation.py
├── auth.py           # Authentication
├── storage.py        # Data storage
└── main.py          # Application entry point

tests/               # Test files
├── __init__.py
├── conftest.py
└── test_recommendations.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
