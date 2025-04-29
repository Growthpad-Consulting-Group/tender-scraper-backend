import logging
from datetime import datetime
from webapp.config import get_db_connection, close_db_connection

logger = logging.getLogger(__name__)

def add_notification(user_id, message):
    """
    Add a notification for a user.
    
    Args:
        user_id (str): The ID of the user.
        message (str): The notification message.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO notifications (user_id, message, created_at, read) VALUES (%s, %s, %s, %s)",
            (user_id, message, datetime.now(), False)
        )
        conn.commit()
        logger.info(f"Notification added for user_id {user_id}: {message}")
    except Exception as e:
        logger.error(f"Error adding notification for user_id {user_id}: {str(e)}")
    finally:
        cur.close()
        close_db_connection(conn)