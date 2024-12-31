import os
import sys

# Add airflow/dags directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags')) 