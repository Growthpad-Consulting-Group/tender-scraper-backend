from flask import Blueprint, request, jsonify
from app.config import get_db_connection
from flask_jwt_extended import jwt_required

search_terms_bp = Blueprint('search_terms_bp', __name__)

# Add Search Term
@search_terms_bp.route('/api/search-terms', methods=['POST'])
@jwt_required()
def add_search_term():
    data = request.json
    term = data.get('term')

    if not term:
        return jsonify({"msg": "Term is required"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO search_terms (term) VALUES (%s)", (term,))
            conn.commit()
            return jsonify({"msg": "Search term added", "term": term}), 201
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Get All Search Terms
@search_terms_bp.route('/api/search-terms', methods=['GET'])
@jwt_required()
def get_search_terms():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM search_terms")
            terms = cur.fetchall()
            return jsonify([{"id": term[0], "term": term[1]} for term in terms]), 200
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Edit Search Term
@search_terms_bp.route('/api/search-terms/<int:term_id>', methods=['PUT'])
@jwt_required()
def edit_search_term(term_id):
    data = request.json
    term = data.get('term')

    if not term:
        return jsonify({"msg": "Term is required"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE search_terms SET term = %s WHERE id = %s", (term, term_id))
            conn.commit()
            return jsonify({"msg": "Search term updated", "term": term}), 200
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Delete Search Term
@search_terms_bp.route('/api/search-terms/<int:term_id>', methods=['DELETE'])
@jwt_required()
def delete_search_term(term_id):
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM search_terms WHERE id = %s", (term_id,))
            conn.commit()
            return jsonify({"msg": "Search term deleted"}), 200
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Bulk Add Search Terms
@search_terms_bp.route('/api/search-terms/bulk', methods=['POST'])
@jwt_required()
def bulk_add_search_terms():
    data = request.json
    terms = data.get('terms')

    if not terms or not isinstance(terms, list):
        return jsonify({"msg": "A list of terms is required"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO search_terms (term) VALUES (%s)", [(term,) for term in terms])
            conn.commit()
            return jsonify({"msg": "Search terms added", "terms": terms}), 201
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Bulk Edit Search Terms
@search_terms_bp.route('/api/search-terms/bulk', methods=['PUT'])
@jwt_required()
def bulk_edit_search_terms():
    data = request.json
    terms = data.get('terms')

    if not terms or not isinstance(terms, list):
        return jsonify({"msg": "A list of terms with IDs is required"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            for term in terms:
                cur.execute("UPDATE search_terms SET term = %s WHERE id = %s", (term['term'], term['id']))
            conn.commit()
            return jsonify({"msg": "Search terms updated"}), 200
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Bulk Delete Search Terms
@search_terms_bp.route('/api/search-terms/bulk', methods=['DELETE'])
@jwt_required()
def bulk_delete_search_terms():
    data = request.json
    ids = data.get('ids')

    if not ids or not isinstance(ids, list):
        return jsonify({"msg": "A list of IDs is required"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM search_terms WHERE id IN %s", (tuple(ids),))
            conn.commit()
            return jsonify({"msg": "Search terms deleted"}), 200
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()
