from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required
from app.cache.redis_cache import set_cache, get_cache, delete_cache
from app.config import get_db_connection
from datetime import datetime

base_keywords_bp = Blueprint('base_keywords_bp', __name__)

# Caching key constant
CACHE_KEY = 'base_keywords_list'

def serialize_keyword(keyword):
    """
    Helper function to convert keyword dictionary for JSON serialization.
    """
    return {
        'id': keyword['id'],
        'keyword': keyword['keyword'],
        'created_at': keyword['created_at'].strftime('%a, %d %b %Y %H:%M:%S GMT') if isinstance(keyword['created_at'], datetime) else keyword['created_at']
    }

@base_keywords_bp.route('/api/base_keywords', methods=['GET'])
@jwt_required()
def get_keywords():
    # Check cache first
    cached_keywords = get_cache(CACHE_KEY)
    if cached_keywords:
        return jsonify(cached_keywords), 200

    # If no cache, fetch from the database
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, keyword, created_at FROM base_keywords")
    keywords = cursor.fetchall()

    keyword_list = [{'id': row[0], 'keyword': row[1], 'created_at': row[2]} for row in keywords]

    current_app.logger.info(f'Fetched keywords from database: {keyword_list}')

    if not keyword_list:
        return jsonify([]), 200

    serialized_keywords = [serialize_keyword(keyword) for keyword in keyword_list]

    set_cache(CACHE_KEY, serialized_keywords)

    return jsonify(serialized_keywords), 200

@base_keywords_bp.route('/api/base_keywords', methods=['POST'])
@jwt_required()
def create_keyword():
    data = request.get_json()
    required_fields = ['keyword']

    # Check for required fields
    if not all(field in data for field in required_fields):
        return jsonify({"msg": "Missing required fields"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Debug log before executing the insert
        current_app.logger.info(f"Inserting keyword: {data['keyword']} at {datetime.utcnow()}")

        # Insert the new keyword into the database
        cursor.execute("INSERT INTO base_keywords (keyword, created_at) VALUES (%s, %s)", (data['keyword'], datetime.utcnow()))

        conn.commit()

        # Clear the cache for keywords
        delete_cache(CACHE_KEY)

        return jsonify({"msg": "Keyword created successfully"}), 201
    except Exception as e:
        # Rollback in case of error
        conn.rollback()
        current_app.logger.error(f"Error creating keyword: {str(e)}")
        return jsonify({"msg": "Error creating keyword", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@base_keywords_bp.route('/api/base_keywords/<int:keyword_id>', methods=['PUT'])
@jwt_required()
def update_keyword(keyword_id):
    data = request.get_json()

    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if the keyword exists
    cursor.execute("SELECT * FROM base_keywords WHERE id = %s", (keyword_id,))
    keyword = cursor.fetchone()

    if keyword is None:
        return jsonify({"msg": "Keyword not found"}), 404

    # Prepare the updated keyword data
    updated_keyword = {
        'keyword': data.get('keyword', keyword[1]),  # Get keyword from the request or keep the old one
        'created_at': keyword[2]  # Retain original created_at
    }

    # Execute the update command with the correct syntax
    cursor.execute("UPDATE base_keywords SET keyword = %s WHERE id = %s", (updated_keyword['keyword'], keyword_id))

    conn.commit()

    # Clear cache or perform other logic as necessary
    delete_cache(CACHE_KEY)

    return jsonify({"msg": "Keyword updated successfully"}), 200

@base_keywords_bp.route('/api/base_keywords/<int:keyword_id>', methods=['DELETE'])
@jwt_required()
def delete_keyword(keyword_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fix the syntax here by replacing '?' with '%s'
    cursor.execute("SELECT * FROM base_keywords WHERE id = %s", (keyword_id,))
    keyword = cursor.fetchone()

    if keyword is None:
        return jsonify({"msg": "Keyword not found"}), 404

    # Fix the syntax here as well by replacing '$1' with '%s'
    cursor.execute("DELETE FROM base_keywords WHERE id = %s", (keyword_id,))

    conn.commit()

    delete_cache(CACHE_KEY)

    return jsonify({"msg": "Keyword deleted successfully"}), 200

@base_keywords_bp.route('/api/base_keywords/check', methods=['GET'])
@jwt_required()
def check_keyword():
    keyword = request.args.get('keyword')

    if not keyword:
        return jsonify({"msg": "Missing keyword parameter"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM base_keywords WHERE keyword = $1", (keyword,))
        exists = cursor.fetchone()[0] > 0

    except Exception as e:
        current_app.logger.error(f"Database error: {str(e)}")
        return jsonify({"msg": "Error checking keyword", "error": str(e)}), 500

    return jsonify({"exists": exists}), 200