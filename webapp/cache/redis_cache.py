import os
import redis
import json
import logging
from retrying import retry
import certifi

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize Redis client
redis_client = None
try:
    redis_client = redis.Redis(
        host=os.getenv('REDIS_URL'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD'),
        ssl=True,
        ssl_cert_reqs="required",
        ssl_ca_certs=certifi.where(),
        decode_responses=True
    )
    # Test the connection
    redis_client.ping()
    logging.info("Successfully connected to Redis")
except Exception as e:
    logging.error(f"Failed to connect to Redis: {str(e)}")
    redis_client = None

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def get_cache(key):
    """Retrieve data from Redis cache."""
    if redis_client is None:
        logging.warning("Redis client not initialized, skipping cache")
        return None
    try:
        cached_data = redis_client.get(key)
        if cached_data:
            logging.info(f"Cache hit for key: {key}")
            return json.loads(cached_data)
        logging.info(f"Cache miss for key: {key}")
        return None
    except Exception as e:
        logging.error(f"Error getting cache for key {key}: {str(e)}")
        return None

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def set_cache(key, value, expiry=3600):
    """Store data in Redis cache with an optional expiry time (in seconds)."""
    if redis_client is None:
        logging.warning("Redis client not initialized, skipping cache set")
        return
    try:
        redis_client.setex(key, expiry, json.dumps(value))
        logging.info(f"Cache set for key: {key} with expiry: {expiry} seconds")
    except Exception as e:
        logging.error(f"Error setting cache for key {key}: {str(e)}")

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def delete_cache(key):
    """Delete a key from Redis cache."""
    if redis_client is None:
        logging.warning("Redis client not initialized, skipping cache delete")
        return
    try:
        redis_client.delete(key)
        logging.info(f"Cache deleted for key: {key}")
    except Exception as e:
        logging.error(f"Error deleting cache for key {key}: {str(e)}")