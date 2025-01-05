from setuptools import setup, find_packages

setup(
    name="prysm_backend",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "uvicorn",
        "sqlalchemy",
        "pydantic",
        "redis",
        "numpy",
        "scikit-learn",
        "geopy",
        "supertokens-python",
        "python-dotenv",
    ],
    python_requires=">=3.9",
) 