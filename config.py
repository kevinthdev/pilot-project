"""
Configuration settings
Supports environment variables and .env file
"""

import os
from dotenv import load_dotenv

load_dotenv()

MAX_TRADEMARKS = int(os.getenv("MAX_TRADEMARKS", "100"))

# Output Settings
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
OUTPUT_JSON_FILE = os.getenv("OUTPUT_JSON_FILE", "trademarks_cleaned.json")
OUTPUT_CSV_FILE = os.getenv("OUTPUT_CSV_FILE", "trademarks_cleaned.csv")

# Logging Settings
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "wipo_trademark_pipeline.log")

DATE_FORMAT = os.getenv("DATE_FORMAT", "%Y-%m-%d")
