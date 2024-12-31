import os
import sys

# Get the absolute path of the project root directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Add airflow/dags directory to Python path
dags_dir = os.path.join(project_root, 'airflow', 'dags')
sys.path.insert(0, dags_dir)

# Add project root to Python path
sys.path.insert(0, project_root) 