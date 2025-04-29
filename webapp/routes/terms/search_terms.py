from flask import Blueprint, request, jsonify, current_app
from flask_jwt_extended import jwt_required
from webapp.cache.redis_cache import set_cache, get_cache, delete_cache
from webapp.config import get_db_connection
from datetime import datetime

search_terms_bp = Blueprint('search_terms_bp', __name__)

# Cache key for search terms
CACHE_KEY = 'search_terms_list'

def serialize_term(term):
    return {
        'id': term['id'],
        'term': term['term'],
        'created_at': term['created_at'].strftime('%a, %d %b %Y %H:%M:%S GMT') if isinstance(term['created_at'], datetime) else term['created_at']
    }

@search_terms_bp.route('/api/search_terms', methods=['GET'])
@jwt_required()
def get_terms():
    cached_terms = get_cache(CACHE_KEY)
    if cached_terms:
        current_app.logger.info("Serving search terms from cache.")
        return jsonify(cached_terms), 200

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, term, created_at FROM search_terms")
        terms = cursor.fetchall()

        term_list = [{'id': row[0], 'term': row[1], 'created_at': row[2]} for row in terms]

        if not term_list:
            return jsonify([]), 200

        serialized_terms = [serialize_term(term) for term in term_list]

        set_cache(CACHE_KEY, serialized_terms)
        current_app.logger.info("Search terms fetched from database and cached.")

        return jsonify(serialized_terms), 200
    except Exception as e:
        current_app.logger.error(f"Error retrieving search terms: {str(e)}")
        return jsonify({"error": "Internal server error", "details": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@search_terms_bp.route('/api/search_terms', methods=['POST'])
@jwt_required()
def create_term():
    data = request.get_json()
    if not data or 'term' not in data:
        return jsonify({"msg": "Term is required"}), 400

    term = data['term']
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO search_terms (term, created_at) VALUES (%s, %s) RETURNING id, term, created_at",
            (term, datetime.utcnow())
        )
        new_term = cursor.fetchone()
        conn.commit()

        # Serialize the new term
        new_term_data = {
            'id': new_term[0],
            'term': new_term[1],
            'created_at': new_term[2].strftime('%a, %d %b %Y %H:%M:%S GMT')
        }

        delete_cache(CACHE_KEY)
        current_app.logger.info("Cache invalidated after adding a new search term.")

        return jsonify({"msg": "Term created successfully", "term": new_term_data}), 201
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error creating term: {str(e)}")
        return jsonify({"msg": "Error creating term", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@search_terms_bp.route('/api/search_terms/<int:term_id>', methods=['PUT'])
@jwt_required()
def update_term(term_id):
    data = request.get_json()
    if not data or 'term' not in data:
        return jsonify({"msg": "Term is required"}), 400

    new_term = data['term']
    if not new_term.strip():
        return jsonify({"msg": "Term cannot be empty"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM search_terms WHERE id = %s", (term_id,))
        term = cursor.fetchone()

        if term is None:
            return jsonify({"msg": "Term not found"}), 404

        cursor.execute(
            "UPDATE search_terms SET term = %s WHERE id = %s",
            (new_term, term_id)
        )
        conn.commit()

        delete_cache(CACHE_KEY)
        current_app.logger.info("Cache invalidated after updating a search term.")

        return jsonify({"msg": "Term updated successfully"}), 200
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error updating term: {str(e)}")
        return jsonify({"msg": "Error updating term", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@search_terms_bp.route('/api/search_terms/<int:term_id>', methods=['DELETE'])
@jwt_required()
def delete_term(term_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM search_terms WHERE id = %s", (term_id,))
        term = cursor.fetchone()

        if term is None:
            return jsonify({"msg": "Term not found"}), 404

        cursor.execute("DELETE FROM search_terms WHERE id = %s", (term_id,))
        conn.commit()

        delete_cache(CACHE_KEY)
        current_app.logger.info("Cache invalidated after deleting a search term.")

        return jsonify({"msg": "Term deleted successfully"}), 200
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error deleting term: {str(e)}")
        return jsonify({"msg": "Error deleting term", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@search_terms_bp.route('/api/search_terms', methods=['DELETE'])
@jwt_required()
def delete_multiple_terms():
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
        query = f"SELECT id FROM search_terms WHERE id IN ({placeholders})"
        cursor.execute(query, ids)
        existing_ids = [row[0] for row in cursor.fetchall()]
        missing_ids = set(ids) - set(existing_ids)

        if missing_ids:
            return jsonify({"msg": f"Terms with IDs {missing_ids} not found"}), 404

        # Delete the terms
        delete_query = f"DELETE FROM search_terms WHERE id IN ({placeholders})"
        cursor.execute(delete_query, ids)
        conn.commit()

        delete_cache(CACHE_KEY)
        current_app.logger.info(f"Cache invalidated after deleting {len(ids)} search terms.")

        return jsonify({"msg": f"Deleted {len(ids)} terms successfully"}), 200
    except Exception as e:
        conn.rollback()
        current_app.logger.error(f"Error deleting multiple terms: {str(e)}")
        return jsonify({"msg": "Error deleting terms", "error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()