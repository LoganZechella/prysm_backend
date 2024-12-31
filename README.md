# Prysm Backend

A data ingestion and processing pipeline for collecting and analyzing music industry trends using Spotify, Google Trends, and LinkedIn data.

## Setup

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
Create a `.env` file in the root directory with the following variables:
```
# Spotify API
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret

# Google Cloud
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/credentials.json

# LinkedIn API
LINKEDIN_CLIENT_ID=your_linkedin_client_id
LINKEDIN_CLIENT_SECRET=your_linkedin_client_secret

# Airflow
AIRFLOW_HOME=./airflow
```

4. Initialize Airflow database:
```bash
airflow db init
```

5. Create Airflow admin user:
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

## Running the Pipeline

1. Start the Airflow webserver:
```bash
airflow webserver -p 8080
```

2. In a new terminal, start the Airflow scheduler:
```bash
airflow scheduler
```

3. Access the Airflow web interface at http://localhost:8080

## Project Structure

```
prysm_backend/
├── airflow/
│   ├── dags/           # Airflow DAG definitions
│   ├── plugins/        # Custom Airflow plugins
│   └── logs/          # Airflow logs
├── tests/             # Test files
├── utils/             # Utility functions
├── requirements.txt   # Project dependencies
└── README.md         # Project documentation
```

## Testing

Run tests using pytest:
```bash
pytest tests/
```

## Contributing

1. Create a new branch for your feature
2. Make your changes
3. Run tests
4. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.