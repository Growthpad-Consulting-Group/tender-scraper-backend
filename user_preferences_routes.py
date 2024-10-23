# user_preferences_routes.py
from flask import Blueprint, request, jsonify
from config import get_db_connection
from flask_jwt_extended import jwt_required, get_jwt_identity

user_preferences_bp = Blueprint('user_preferences_bp', __name__)

# Get user preferences
@user_preferences_bp.route('/api/preferences', methods=['GET'])
@jwt_required()
def get_user_preferences():
    current_user = get_jwt_identity()
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT preferred_search_engine, automatic_scraping, notification_enabled FROM user_preferences WHERE user_id = (SELECT id FROM users WHERE username = %s)", (current_user,))
        preferences = cur.fetchone()

        if preferences:
            return jsonify({
                "preferred_search_engine": preferences[0],
                "automatic_scraping": preferences[1],
                "notification_enabled": preferences[2]
            }), 200
        else:
            return jsonify({"msg": "Preferences not found"}), 404

    except Exception as e:
        return jsonify({"msg": "Error fetching preferences", "error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

# Update user preferences
@user_preferences_bp.route('/api/preferences', methods=['PUT'])
@jwt_required()
def update_user_preferences():
    current_user = get_jwt_identity()
    data = request.json
    preferred_search_engine = data.get('preferred_search_engine')
    automatic_scraping = data.get('automatic_scraping')
    notification_enabled = data.get('notification_enabled')

    if not preferred_search_engine:
        return jsonify({"msg": "Please provide a preferred search engine"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Update the user's preferences
        cur.execute("""
            INSERT INTO user_preferences (user_id, preferred_search_engine, automatic_scraping, notification_enabled)
            VALUES ((SELECT id FROM users WHERE username = %s), %s, %s, %s)
            ON CONFLICT (user_id)
            DO UPDATE SET preferred_search_engine = EXCLUDED.preferred_search_engine,
                          automatic_scraping = EXCLUDED.automatic_scraping,
                          notification_enabled = EXCLUDED.notification_enabled
        """, (current_user, preferred_search_engine, automatic_scraping, notification_enabled))
        conn.commit()

        return jsonify({"msg": "Preferences updated successfully"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Error updating preferences", "error": str(e)}), 500
    finally:
        cur.close()
        conn.close()
