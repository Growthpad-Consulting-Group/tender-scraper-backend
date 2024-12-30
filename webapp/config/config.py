import os
import pg8000
from dotenv import load_dotenv
import time
import logging

# Load environment variables from .env file
load_dotenv()

def get_db_connection(retries=3, delay=5):
    """Establish a database connection with retry logic."""
    attempt = 0
    while attempt < retries:
        try:
            connection = pg8000.connect(
                host=os.getenv("DB_HOST"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                port=int(os.getenv("DB_PORT", 5432))  # Defaults to 5432 if not set
            )
            logging.info("Successfully connected to the database.")
            return connection
        except Exception as e:
            attempt += 1
            logging.error(f"Error connecting to the database (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise Exception("Unable to connect to the database after several attempts.")
