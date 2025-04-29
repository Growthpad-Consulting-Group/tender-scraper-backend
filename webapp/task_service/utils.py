import json
import logging
from datetime import datetime
from dateutil import parser
from flask import g
from webapp.cache.redis_cache import redis_client, get_cache, set_cache, delete_cache
from .constants import FREQUENCY_INTERVALS
from .exceptions import TaskNotFoundError

logger = logging.getLogger(__name__)

# --- Redis Utilities ---

def set_task_state(task_id, state, expiry=3600):
    """
    Set the state of a scraping task in Redis with an expiration time.
    
    Args:
        task_id (str): The ID of the scraping task.
        state (dict): The state to set.
        expiry (int): Expiration time in seconds (default: 3600).
    """
    try:
        existing_state = get_task_state(task_id) or {}
        if "startTime" in existing_state and "startTime" not in state:
            state["startTime"] = existing_state["startTime"]
        redis_client.setex(f"scraping_task:{task_id}", expiry, json.dumps(state))
    except Exception as e:
        logger.error(f"Error setting task state in Redis for task_id {task_id}: {str(e)}")

def get_task_state(task_id):
    """
    Retrieve the state of a scraping task from Redis.
    
    Args:
        task_id (str): The ID of the scraping task.
    
    Returns:
        dict: The task state, or None if not found.
    """
    try:
        state = redis_client.get(f"scraping_task:{task_id}")
        return json.loads(state) if state else None
    except Exception as e:
        logger.error(f"Error getting task state from Redis for task_id {task_id}: {str(e)}")
        return None

def delete_task_state(task_id):
    """
    Delete the state of a scraping task from Redis.
    
    Args:
        task_id (str): The ID of the scraping task.
    """
    try:
        redis_client.delete(f"scraping_task:{task_id}")
    except Exception as e:
        logger.error(f"Error deleting task state from Redis for task_id {task_id}: {str(e)}")

# --- Database Utilities ---

def fetch_task_details(task_id, user_id, fields="*"):
    """
    Fetch task details for a given task_id and user_id.
    
    Args:
        task_id (int): The ID of the task.
        user_id (str): The ID of the user.
        fields (str): The fields to select (default: "*").
    
    Returns:
        tuple: The task details.
    
    Raises:
        TaskNotFoundError: If the task is not found or the user lacks permission.
    """
    g.cur.execute(f"SELECT {fields} FROM scheduled_tasks WHERE task_id = %s AND user_id = %s", (task_id, user_id))
    task = g.cur.fetchone()
    if not task:
        logger.warning(f"Task {task_id} not found for user {user_id}")
        raise TaskNotFoundError("Task not found or access denied.")
    return task

def get_search_terms(task_id):
    """
    Fetch search terms for a given task.
    
    Args:
        task_id (int): The ID of the task.
    
    Returns:
        list: List of search terms.
    """
    g.cur.execute("SELECT term FROM task_search_terms WHERE task_id = %s", (task_id,))
    return [row[0] for row in g.cur.fetchall()]

# --- Task Utilities ---

def format_task_response(task, search_terms=None, calculate_next=True):
    """
    Format a task response for API output.
    
    Args:
        task (tuple): The task data from the database.
        search_terms (list, optional): List of search terms.
        calculate_next (bool): Whether to calculate the next schedule (default: True).
    
    Returns:
        dict: Formatted task response.
    """
    task_dict = {
        "task_id": task[0],
        "name": task[1],
        "frequency": task[2],
        "start_time": task[3].isoformat() if task[3] else None,
        "end_time": task[4].isoformat() if task[4] else None,
        "priority": task[5],
        "is_enabled": task[6],
        "tender_type": task[7],
        "last_run": task[8].isoformat() if task[8] else None,
        "email_notifications_enabled": task[9] if len(task) > 9 else False,
        "sms_notifications_enabled": task[10] if len(task) > 10 else False,
        "slack_notifications_enabled": task[11] if len(task) > 11 else False,
        "custom_emails": task[12] if len(task) > 12 else "",
        "search_terms": search_terms if search_terms is not None else (task[13] if len(task) > 13 and task[13] else []),
        "engines": task[14] if isinstance(task[14], list) else (task[14].split(',') if task[14] else []),
    }
    if calculate_next:
        task_dict["next_schedule"] = calculate_next_schedule(task[3], task[2], task[6])
    return task_dict

def calculate_next_schedule(start_time, frequency, is_enabled):
    """
    Calculate the next scheduled time for a task based on its frequency.
    
    Args:
        start_time (datetime or str): The start time of the task.
        frequency (str): The frequency of the task (e.g., 'Daily').
        is_enabled (bool): Whether the task is enabled.
    
    Returns:
        str: The next scheduled time in ISO format, or "N/A" if not applicable.
    """
    logger.info(f"Calculating next schedule: start_time={start_time}, frequency={frequency}, is_enabled={is_enabled}")
    
    if not is_enabled or not start_time:
        logger.info(f"Returning 'N/A' because is_enabled={is_enabled}, start_time={start_time}")
        return "N/A"

    now = datetime.now()

    if isinstance(start_time, str):
        try:
            start = parser.parse(start_time)
        except ValueError as e:
            logger.error(f"Failed to parse start_time '{start_time}': {str(e)}")
            return "N/A"
    else:
        start = start_time

    frequency = frequency.strip().title()
    logger.info(f"Normalized frequency: '{frequency}'")

    interval = FREQUENCY_INTERVALS.get(frequency)
    if not interval:
        logger.warning(f"Frequency '{frequency}' not found in intervals. Returning 'N/A'")
        return "N/A"

    if start > now:
        return start.isoformat()

    time_diff = now - start
    intervals_passed = int(time_diff.total_seconds() // interval.total_seconds()) + 1
    next_schedule = start + (interval * intervals_passed)

    while next_schedule < now:
        next_schedule += interval

    logger.info(f"Calculated next_schedule: {next_schedule.isoformat()}")
    return next_schedule.isoformat()