from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required
from webapp.cache.redis_cache import set_cache, get_cache, delete_cache
from webapp.config import get_db_connection
from datetime import datetime

relevant_keywords_bp = Blueprint('relevant_keywords_bp', __name__)

# Cache key for relevant keywords
CACHE_KEY = 'relevant_keywords_list'

def serialize_keyword(keyword):
    return {
        'id': keyword['id'],
        'keyword': keyword['keyword'],
        'created_at': keyword['created_at'].strftime('%a, %d %b %Y %H:%M:%S GMT') if isinstance(keyword['created_at'], datetime) else keyword['created_at']
    }

@relevant_keywords_bp.route('/api/relevant_keywords', methods=['GET'])
@jwt_required()
def get_keywords():
    cached_keywords = get_cache(CACHE_KEY)
    if cached_keywords:
        current_app.logger.info("Serving relevant keywords from cache.")
        return jsonify(cached_keywords), 200

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, keyword, created_at FROM relevant_keywords")
        keywords = cursor.fetchall()

        keyword_list = [{'id': row[0], 'keyword': row[1], 'created_at': row[2]} for row in keywords]

        if not keyword_list:
            return jsonify([]), 200

        serialized_keywords = [serialize_keyword(keyword) for keyword in keyword_list]

        set_cache(CACHE_KEY, serialized_keywords)
        current_app.logger.info("Relevant keywords fetched from database and cached.")

        return jsonify(serialized_keywords), 200
    except Exception as e:
        current_app.logger.error(f"Error retrieving relevant keywords: {str(e)}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@relevant_keywords_bp.route('/api/relevant_keywords', methods=['POST'])
@jwt_required()
def create_keyword():
    data = request.get_json()
    if not data or 'keyword' not in data:
        return jsonify({"msg": "Keyword is required"}), 400

    keyword = data['keyword']
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO relevant_keywords (keyword, created_at) VALUES (%s, %s) RETURNING id, keyword, created_at",
            (keyword, datetime.utcnow())
        )
        new_keyword = cursor.fetchone()
        conn.commit()

        # Serialize the new keyword
        new_keyword_data = {
            'id': new_keyword[0],
            'keyword': new_keyword[1],
            'created_at': new_keyword[2].strftime('%a, %d %b %Y %H:%M:%S GMT')
        }

        delete_cache(CACHE_KEY)
        current_app.logger.info("Cache invalidated after adding a new relevant keyword.")

        return jsonify({"msg": "Keyword created successfully", "keyword": new_keyword_data}), 201
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error creating keyword: {str(e)}")
        return jsonify({"msg": "Error creating keyword", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@relevant_keywords_bp.route('/api/relevant_keywords/<int:keyword_id>', methods=['PUT'])
@jwt_required()
def update_keyword(keyword_id):
    data = request.get_json()
    if not data or 'keyword' not in data:
        return jsonify({"msg": "Keyword is required"}), 400

    new_keyword = data['keyword']
    if not new_keyword.strip():
        return jsonify({"msg": "Keyword cannot be empty"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM relevant_keywords WHERE id = %s", (keyword_id,))
        keyword = cursor.fetchone()

        if keyword is None:
            return jsonify({"msg": "Keyword not found"}), 404

        cursor.execute(
            "UPDATE relevant_keywords SET keyword = %s WHERE id = %s",
            (new_keyword, keyword_id)
        )
        conn.commit()

        delete_cache(CACHE_KEY)
        current_app.logger.info("Cache invalidated after updating a relevant keyword.")

        return jsonify({"msg": "Keyword updated successfully"}), 200
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error updating keyword: {str(e)}")
        return jsonify({"msg": "Error updating keyword", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@relevant_keywords_bp.route('/api/relevant_keywords/<int:keyword_id>', methods=['DELETE'])
@jwt_required()
def delete_keyword(keyword_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM relevant_keywords WHERE id = %s", (keyword_id,))
        keyword = cursor.fetchone()

        if keyword is None:
            return jsonify({"msg": "Keyword not found"}), 404

        cursor.execute("DELETE FROM relevant_keywords WHERE id = %s", (keyword_id,))
        conn.commit()

        delete_cache(CACHE_KEY)
        current_app.logger.info("Cache invalidated after deleting a relevant keyword.")

        return jsonify({"msg": "Keyword deleted successfully"}), 200
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error deleting keyword: {str(e)}")
        return jsonify({"msg": "Error deleting keyword", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@relevant_keywords_bp.route('/api/relevant_keywords', methods=['DELETE'])
@jwt_required()
def delete_multiple_keywords():
    data = request.get_json()
    if not data or 'ids' not in data:
        return jsonify({"msg": "IDs are required"}), 400

    ids = data['ids']
    if not isinstance(ids, list) or not ids:
        return jsonify({"msg": "A non-empty list of IDs is required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create placeholders for the IN clause (e.g., %s, %s, %s)
        placeholders = ', '.join(['%s'] * len(ids))
        # Check if all IDs exist
        query = f"SELECT id FROM relevant_keywords WHERE id IN ({placeholders})"
        cursor.execute(query, ids)
        existing_ids = [row[0] for row in cursor.fetchall()]
        missing_ids = set(ids) - set(existing_ids)

        if missing_ids:
            return jsonify({"msg": f"Keywords with IDs {missing_ids} not found"}), 404

        # Delete the keywords
        delete_query = f"DELETE FROM relevant_keywords WHERE id IN ({placeholders})"
        cursor.execute(delete_query, ids)
        conn.commit()

        delete_cache(CACHE_KEY)
        current_app.logger.info(f"Cache invalidated after deleting {len(ids)} relevant keywords.")

        return jsonify({"msg": f"Deleted {len(ids)} keywords successfully"}), 200
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error deleting multiple keywords: {str(e)}")
        return jsonify({"msg": "Error deleting keywords", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()