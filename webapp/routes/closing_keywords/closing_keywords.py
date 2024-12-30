from flask import Blueprint, request, jsonify
from webapp.config import get_db_connection
from flask_jwt_extended import jwt_required
from webapp.cache.redis_cache import set_cache, get_cache, delete_cache
import logging
from datetime import datetime  # Ensure you import datetime if not already

# Create a Blueprint for closing keywords management
closing_keywords_bp = Blueprint('closing_keywords_bp', __name__)

# Get All Closing Keywords
@closing_keywords_bp.route('/api/closing_keywords', methods=['GET'])
@jwt_required()
def get_closing_keywords():
    """
    Fetch all closing keywords from the cache or database.
    """
    cache_key = 'closing_keywords'
    keywords = get_cache(cache_key)

    if keywords is not None:
        logging.info("Serving closing keywords from cache.")
        return jsonify(keywords), 200

    db_connection = get_db_connection()
    try:
        cur = db_connection.cursor()
        cur.execute("SELECT keyword, created_at FROM closing_keywords;")

        # Assuming created_at is a datetime object, convert it to a string
        keywords = [{
            "keyword": row[0],
            "created_at": row[1].strftime("%a, %d %b %Y %H:%M:%S GMT") if row[1] else None  # Format datetime to the specified string
        } for row in cur.fetchall()]
        cur.close()

        # Cache the results
        set_cache(cache_key, keywords)
        logging.info("Closing keywords fetched from database and cached.")

        return jsonify(keywords), 200
    except Exception as e:
        logging.error(f"Error retrieving closing keywords: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db_connection.close()

# Create a New Closing Keyword
@closing_keywords_bp.route('/api/closing_keywords', methods=['POST'])
@jwt_required()
def create_closing_keyword():
    """
    Create a new closing keyword.
    """
    db_connection = get_db_connection()
    data = request.json

    if not data or 'keyword' not in data:
        return jsonify({"error": "Keyword is required"}), 400

    keyword = data['keyword']
    try:
        cur = db_connection.cursor()
        cur.execute("INSERT INTO closing_keywords (keyword) VALUES (%s)", (keyword,))
        db_connection.commit()
        cur.close()

        # Invalidate the cache
        delete_cache('closing_keywords')
        logging.info("Cache invalidated after adding a new keyword.")

        return jsonify({"message": "Keyword added successfully"}), 201
    except Exception as e:
        logging.error(f"Error adding keyword: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db_connection.close()

# Update an Existing Closing Keyword
@closing_keywords_bp.route('/api/closing_keywords/<string:keyword>', methods=['PUT'])
@jwt_required()
def update_closing_keyword(keyword):
    """
    Update an existing closing keyword.
    """
    db_connection = get_db_connection()
    data = request.json

    if not data or 'new_keyword' not in data:
        return jsonify({"error": "New keyword is required"}), 400

    new_keyword = data['new_keyword']
    logging.info(f"Updating closing keyword from '{keyword}' to '{new_keyword}'.")

    try:
        cur = db_connection.cursor()
        cur.execute("UPDATE closing_keywords SET keyword = %s WHERE keyword = %s", (new_keyword, keyword))
        db_connection.commit()
        if cur.rowcount == 0:
            return jsonify({"error": "Keyword not found"}), 404

        # Invalidate cache
        delete_cache('closing_keywords')
        logging.info("Cache invalidated after updating a closing keyword.")

        return jsonify({"message": "Closing keyword updated successfully"}), 200
    except Exception as e:
        logging.error(f"Error updating closing keyword: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db_connection.close()

# Delete a Closing Keyword
@closing_keywords_bp.route('/api/closing_keywords/<string:keyword>', methods=['DELETE'])
@jwt_required()
def delete_closing_keyword(keyword):
    """
    Delete a closing keyword.
    """
    db_connection = get_db_connection()
    try:
        cur = db_connection.cursor()
        cur.execute("DELETE FROM closing_keywords WHERE keyword = %s", (keyword,))
        if cur.rowcount == 0:
            return jsonify({"error": "Keyword not found"}), 404
        db_connection.commit()
        cur.close()

        # Invalidate the cache
        delete_cache('closing_keywords')
        logging.info("Cache invalidated after deleting a keyword.")

        return jsonify({"message": "Keyword deleted successfully"}), 200
    except Exception as e:
        logging.error(f"Error deleting keyword: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db_connection.close()