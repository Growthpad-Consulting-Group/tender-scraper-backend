# redis_cache.py

import os
import redis
import json
from typing import Any, Optional

# Initialize Redis connection
REDIS_URL = os.getenv('REDIS_URL')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# Ensure Redis connection parameters are set
if not REDIS_URL or not REDIS_PASSWORD:
    raise ValueError("Redis connection parameters are not set in the environment variables.")

# Create a Redis client
redis_client = redis.Redis(
    host=REDIS_URL,
    port=6379,
    password=REDIS_PASSWORD,
    ssl=True,
    decode_responses=True
)


def set_cache(key: str, value: Any, expiry: int = 300) -> None:
    """Set a value in cache with an expiration time."""
    try:
        redis_client.set(key, json.dumps(value), ex=expiry)
    except redis.RedisError as e:
        print(f"Redis error while setting cache for key {key}: {e}")


def get_cache(key: str) -> Optional[Any]:
    """Get a value from cache, returning None if it does not exist."""
    try:
        value = redis_client.get(key)
        return json.loads(value) if value else None
    except redis.RedisError as e:
        print(f"Redis error while getting cache for key {key}: {e}")
        return None


def delete_cache(key: str) -> None:
    """Delete a value from cache."""
    try:
        redis_client.delete(key)
    except redis.RedisError as e:
        print(f"Redis error while deleting cache for key {key}: {e}")