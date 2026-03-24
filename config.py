from pathlib import Path
import os

HOME_DIR = Path.home()
BASE_DIR = HOME_DIR / "database_ws" / "nist_response_db"
CSV_DIR = BASE_DIR / "csv"

DATABASE_URL = os.getenv(
    # DATABASE_URL",
    # "postgresql+psycopg2://postgres:your_real_password@localhost:5432/nist_response_db"
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/nist_response_db"
)