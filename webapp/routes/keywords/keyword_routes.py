# keyword_routes.py
from flask import Blueprint, request, jsonify
from webapp.config import get_db_connection

keyword_bp = Blueprint('keyword_bp', __name__)

# Add a new keyword
@keyword_bp.route('/api/keywords', methods=['POST'])
def add_keyword():
    data = request.json
    keyword = data.get('keyword')

    if not keyword:
        return jsonify({"msg": "Please provide a keyword"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Insert the keyword into the database
        cur.execute("INSERT INTO keywords (keyword) VALUES (%s) RETURNING id", (keyword,))
        keyword_id = cur.fetchone()[0]
        conn.commit()

        return jsonify({"msg": "Keyword added successfully", "keyword_id": keyword_id}), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Error adding keyword", "error": str(e)}), 500

    finally:
        cur.close()
        conn.close()

# Add bulk keywords
@keyword_bp.route('/api/keywords/bulk', methods=['POST'])
def add_bulk_keywords():
    data = request.json
    keywords = data.get('keywords')

    if not keywords or not isinstance(keywords, list) or not all(isinstance(k, str) for k in keywords):
        return jsonify({"msg": "Please provide a list of keywords"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Insert the keywords into the database
        cur.executemany("INSERT INTO keywords (keyword) VALUES (%s) RETURNING id", [(k,) for k in keywords])
        keyword_ids = cur.fetchall()
        conn.commit()

        return jsonify({"msg": "Keywords added successfully", "keyword_ids": [id[0] for id in keyword_ids]}), 201

    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Error adding keywords", "error": str(e)}), 500

    finally:
        cur.close()
        conn.close()

# Get all keywords
@keyword_bp.route('/api/keywords', methods=['GET'])
def get_keywords():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Fetch all keywords from the database
        cur.execute("SELECT * FROM keywords")
        keywords = cur.fetchall()

        # Format the result
        keywords_list = [{"id": row[0], "keyword": row[1]} for row in keywords]

        return jsonify(keywords_list), 200

    except Exception as e:
        return jsonify({"msg": "Error retrieving keywords", "error": str(e)}), 500

    finally:
        cur.close()
        conn.close()

# Update a keyword
@keyword_bp.route('/api/keywords/<int:keyword_id>', methods=['PUT'])
def update_keyword(keyword_id):
    data = request.json
    new_keyword = data.get('keyword')

    if not new_keyword:
        return jsonify({"msg": "Please provide a new keyword"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Update the keyword in the database
        cur.execute("UPDATE keywords SET keyword = %s WHERE id = %s", (new_keyword, keyword_id))
        conn.commit()

        if cur.rowcount == 0:
            return jsonify({"msg": "Keyword not found"}), 404

        return jsonify({"msg": "Keyword updated successfully"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Error updating keyword", "error": str(e)}), 500

    finally:
        cur.close()
        conn.close()

# Delete a keyword
@keyword_bp.route('/api/keywords/<int:keyword_id>', methods=['DELETE'])
def delete_keyword(keyword_id):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Delete the keyword from the database
        cur.execute("DELETE FROM keywords WHERE id = %s", (keyword_id,))
        conn.commit()

        if cur.rowcount == 0:
            return jsonify({"msg": "Keyword not found"}), 404

        return jsonify({"msg": "Keyword deleted successfully"}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Error deleting keyword", "error": str(e)}), 500

    finally:
        cur.close()
        conn.close()

# Filter keywords
@keyword_bp.route('/api/keywords/filter', methods=['GET'])
def filter_keywords():
    filter_value = request.args.get('filter')

    if not filter_value:
        return jsonify({"msg": "Please provide a filter value"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Use LIKE to filter keywords
        cur.execute("SELECT * FROM keywords WHERE keyword ILIKE %s", (f"%{filter_value}%",))
        keywords = cur.fetchall()

        return jsonify(keywords), 200

    except Exception as e:
        return jsonify({"msg": "Error filtering keywords", "error": str(e)}), 500

    finally:
        cur.close()
        conn.close()
 