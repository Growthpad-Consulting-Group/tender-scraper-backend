import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import pool
import logging
import time

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the connection pool (will be created once at app startup)
db_pool = None

def init_db_pool():
    """Initialize the database connection pool."""
    global db_pool
    try:
        # Construct the connection string with SSL for Supabase
        connection_string = (
            f"host={os.getenv('DB_HOST')} "
            f"dbname={os.getenv('DB_NAME')} "
            f"user={os.getenv('DB_USER')} "
            f"password={os.getenv('DB_PASSWORD')} "
            f"port={os.getenv('DB_PORT', '6543')} "  # Default to transaction mode (6543)
            f"sslmode=require "  # Enforce SSL for Supabase
            f"connect_timeout=10"
        )

        # Close existing pool if it exists
        if db_pool is not None:
            try:
                db_pool.closeall()
                logging.info("Closed existing database connection pool.")
            except Exception as e:
                logging.warning(f"Error closing existing pool: {str(e)}")

        db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,  # Minimum number of connections to keep open
            maxconn=10,  # Increased to 10 to handle more concurrent requests
            dsn=connection_string
        )

        # Test a connection to ensure the pool is usable
        test_conn = db_pool.getconn()
        db_pool.putconn(test_conn)
        logging.info("Database connection pool initialized successfully.")
    except Exception as e:
        logging.error(f"Error initializing database pool: {str(e)}")
        db_pool = None  # Reset pool to None on failure
        raise

def get_db_connection(retries=3, delay=5):
    """Get a database connection from the pool with retry logic."""
    global db_pool
    if db_pool is None:
        init_db_pool()

    attempt = 0
    while attempt < retries:
        try:
            # Log pool usage before attempting to get a connection
            logging.debug(f"Connection pool status: used={db_pool._used}, total={db_pool.maxconn}")
            conn = db_pool.getconn()
            # Test the connection to ensure it's usable
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            logging.info("Successfully obtained database connection from pool.")
            return conn
        except Exception as e:
            attempt += 1
            logging.error(f"Error getting database connection from pool (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                # If the error indicates a connection issue, try reinitializing the pool
                if isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.pool.PoolError)):
                    logging.warning("Connection error detected, attempting to reinitialize pool...")
                    try:
                        init_db_pool()
                    except Exception as reinit_e:
                        logging.error(f"Failed to reinitialize pool: {str(reinit_e)}")
                time.sleep(delay)
            else:
                # Reset pool to None to force reinitialization on next attempt
                db_pool = None
                raise Exception("Unable to get database connection from pool after several attempts.")

def close_db_connection(conn):
    """Return the database connection to the pool."""
    if conn and db_pool:
        try:
            if conn.closed:
                logging.warning("Connection is already closed, cannot return to pool.")
                return
            db_pool.putconn(conn)
            logging.info("Returned database connection to pool.")
            logging.debug(f"Connection pool status after return: used={db_pool._used}, total={db_pool.maxconn}")
        except Exception as e:
            logging.error(f"Error returning connection to pool: {e}")
            # If the connection is unusable, reinitialize the pool
            if isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError)):
                logging.warning("Unusable connection detected, reinitializing pool...")
                try:
                    init_db_pool()
                except Exception as reinit_e:
                    logging.error(f"Failed to reinitialize pool: {str(reinit_e)}")
    else:
        logging.warning("No connection or pool to close.")