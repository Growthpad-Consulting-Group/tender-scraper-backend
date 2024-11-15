import os
import pg8000
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    connection = pg8000.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", 5432))  # Defaults to 5432 if not set
    )
    return connection