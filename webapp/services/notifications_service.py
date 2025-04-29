from flask import Blueprint, jsonify, g
from flask_jwt_extended import jwt_required, get_jwt_identity
from webapp.config import get_db_connection, close_db_connection
import logging
from datetime import datetime

notifications_service_bp = Blueprint('notifications_service', __name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@notifications_service_bp.route('/api/notifications', methods=['GET'])
@jwt_required()
def get_notifications():
    """
    Fetch notifications for the authenticated user.
    Returns unread notifications and recent read notifications (up to 10 total).
    """
    user_id = get_jwt_identity()
    logger.info(f"Fetching notifications for user_id: {user_id}")

    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # Fetch notifications: prioritize unread, then recent read, limit to 10
        query = """
            SELECT id, user_id, message, created_at, read
            FROM notifications
            WHERE user_id = %s
            ORDER BY read ASC, created_at DESC
            LIMIT 10
        """
        cur.execute(query, (user_id,))
        notifications = cur.fetchall()

        # Format the response
        notifications_list = [
            {
                "id": n[0],
                "user_id": n[1],
                "message": n[2],
                "created_at": n[3].isoformat(),
                "read": n[4]
            }
            for n in notifications
        ]

        logger.info(f"Successfully fetched {len(notifications_list)} notifications for user_id: {user_id}")
        return jsonify({"notifications": notifications_list}), 200

    except Exception as e:
        logger.error(f"Error fetching notifications for user_id {user_id}: {str(e)}")
        return jsonify({"msg": "Failed to fetch notifications", "error": str(e)}), 500

    finally:
        cur.close()
        close_db_connection(conn)

@notifications_service_bp.route('/api/notifications/<int:notification_id>/read', methods=['PATCH'])
@jwt_required()
def mark_notification_as_read(notification_id):
    """
    Mark a specific notification as read for the authenticated user.
    """
    user_id = get_jwt_identity()
    logger.info(f"Marking notification {notification_id} as read for user_id: {user_id}")

    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # Check if the notification exists and belongs to the user
        cur.execute(
            "SELECT user_id, read FROM notifications WHERE id = %s",
            (notification_id,)
        )
        notification = cur.fetchone()

        if not notification:
            logger.warning(f"Notification {notification_id} not found for user_id: {user_id}")
            return jsonify({"msg": "Notification not found"}), 404

        if notification[0] != user_id:
            logger.warning(f"Unauthorized attempt to mark notification {notification_id} as read by user_id: {user_id}")
            return jsonify({"msg": "Unauthorized"}), 403

        if notification[1]:
            logger.info(f"Notification {notification_id} already marked as read for user_id: {user_id}")
            return jsonify({"msg": "Notification already marked as read"}), 200

        # Mark the notification as read
        cur.execute(
            "UPDATE notifications SET read = TRUE WHERE id = %s",
            (notification_id,)
        )
        conn.commit()

        logger.info(f"Successfully marked notification {notification_id} as read for user_id: {user_id}")
        return jsonify({"msg": "Notification marked as read"}), 200

    except Exception as e:
        logger.error(f"Error marking notification {notification_id} as read for user_id {user_id}: {str(e)}")
        return jsonify({"msg": "Failed to mark notification as read", "error": str(e)}), 500

    finally:
        cur.close()
        close_db_connection(conn)