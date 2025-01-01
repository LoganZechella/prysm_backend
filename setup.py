from setuptools import setup, find_packages

setup(
    name="prysm_backend",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "uvicorn",
        "python-dotenv",
        "httpx",
        "python-jose[cryptography]",
        "python-multipart",
        "pydantic",
    ],
    python_requires=">=3.8",
) 