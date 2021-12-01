from pathlib import Path

from typing import Literal


# Access token for Social Media API
# https://api.data365.co/v1.1/docs
ACCESS_TOKEN: str

# Location for the cache
# Cache allows to stop and start the script at any arbitrary moment without losing the progress
TASK_CACHE_DB: Path = Path('./current_task_cache.db')

OUTPUT_FORMAT: Literal['postgres', 'csv'] = 'csv'

# DSN (connection string) for the database
# https://stackoverflow.com/questions/3582552/what-is-the-format-for-the-postgresql-connection-string-url
POSTGRES_DSN: str = 'postgresql://postgres:postgres@localhost:5432/postgres'
POSTGRES_SCHEMA: str = 'data'

CSV_PATH: Path = Path('./data')

# Do not change this values
API_BASE_URL: str = 'https://api.data365.co'
API_VERSION: str = 'v1.1'
API_URL: str = f"{API_BASE_URL.rstrip('/')}/{API_VERSION}"

# Do not change this values
REQUEST_TIMEOUT_S: float = 3.0
UPDATE_CHECK_PERIOD_S: float = 3.0
MAX_THREADS: int = 10
MAX_QUEUE_SIZE: int = 5
TASKS_BATCH_SIZE: int = 5
