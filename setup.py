"""
Setup script for Acuity-Airtable SDK
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

setup(
    name="acuity-airtable-sdk",
    version="1.0.0",
    author="Your Name",
    author_email="yj276",
    description="Python SDK for connecting Acuity Scheduling to Airtable",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/acuity-airtable-sdk",
    packages=find_packages(exclude=["test", "venv", "*.tests", "*.tests.*", "tests.*", "tests"]),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Office/Business",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pyairtable>=2.3.0",
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "python-dateutil>=2.8.0",
        "pytz>=2024.1",
    ],
    extras_require={
        "streamlit": [
            "streamlit>=1.28.0",
            "pandas>=2.0.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)

