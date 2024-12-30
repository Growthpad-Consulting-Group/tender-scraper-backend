from flask import Blueprint, request, jsonify  # Import necessary Flask components for creating routes and handling requests
from webapp.config import get_db_connection  # Import database connection utility
from flask_jwt_extended import jwt_required  # Import JWT handling for authentication
from webapp.cache.redis_cache import set_cache, get_cache, delete_cache  # Import caching utilities
from datetime import datetime

# Create a Blueprint for search term management
search_terms_bp = Blueprint('search_terms_bp', __name__)

# Add a Search Term
@search_terms_bp.route('/api/search-terms', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated to access this endpoint
def add_search_term():
    """
    Adds a new search term to the database.

    Expects a JSON payload with a 'term' key.

    Returns:
        jsonify: A response indicating the outcome of the add operation.
    """
    data = request.json  # Get the incoming JSON data
    term = data.get('term')  # Extract the search term from the data

    # Validate that the term is provided
    if not term:
        return jsonify({"msg": "Term is required"}), 400  # Return a 400 Bad Request if term is missing

    conn = get_db_connection()  # Establish database connection
    try:
        with conn.cursor() as cur:
            # Insert the term into the database with the current timestamp for created_at
            cur.execute("INSERT INTO search_terms (term, created_at) VALUES (%s, NOW())", (term,))
            conn.commit()  # Commit the transaction

            # Invalidate cache after adding a search term
            delete_cache('search_terms_all')

            return jsonify({"msg": "Search term added", "term": term}), 201  # Return success response
    except Exception as e:
        return jsonify({"msg": str(e)}), 400  # Return an error if the insertion fails
    finally:
        conn.close()  # Ensure the database connection is closed

# Get All Search Terms
@search_terms_bp.route('/api/search-terms', methods=['GET'])
@jwt_required()  # Ensure the user is authenticated
def get_search_terms():
    """
    Retrieves all search terms from the database.

    Returns:
        jsonify: A list of search terms or an error message.
    """
    # Attempt to retrieve terms from the cache
    cached_terms = get_cache('search_terms_all')
    if cached_terms is not None:
        return jsonify(cached_terms), 200  # If found in cache, return immediately

    conn = get_db_connection()  # Establish database connection
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, term, created_at FROM search_terms")  # Select all relevant fields
            terms = cur.fetchall()  # Fetch all results

            # Transform the results into a list of dictionaries
            terms_list = []
            for row in terms:
                term_dict = {
                    "id": row[0],
                    "term": row[1],
                    "created_at": row[2].strftime("%a, %d %b %Y %H:%M:%S GMT") if row[2] else None  # Format datetime to the specified string
                }
                terms_list.append(term_dict)

            # Cache the fetched terms for future requests
            set_cache('search_terms_all', terms_list)

            return jsonify(terms_list), 200  # Return the list of terms
    except Exception as e:
        return jsonify({"msg": str(e)}), 400  # Handle errors with a response
    finally:
        conn.close()  # Ensure the database connection is closed

# Edit Search Term
@search_terms_bp.route('/api/search-terms/<int:term_id>', methods=['PUT'])
@jwt_required()  # Ensure the user is authenticated
def edit_search_term(term_id):
    """
    Updates an existing search term in the database.

    Expects a JSON payload with a 'term' key and the term's ID in the URL.

    Args:
        term_id (int): The ID of the search term to update.

    Returns:
        jsonify: A response indicating the outcome of the update operation.
    """
    data = request.json  # Get the incoming JSON data
    term = data.get('term')  # Extract the search term from the data
    created_at = data.get('created_at')  # Extract the created_at timestamp from the data

    # Validate that the term is provided
    if not term:
        return jsonify({"msg": "Term is required"}), 400  # Return if the term is missing

    conn = get_db_connection()  # Establish database connection
    try:
        with conn.cursor() as cur:
            # Update the term and created_at timestamp
            cur.execute(
                "UPDATE search_terms SET term = %s, created_at = %s WHERE id = %s",
                (term, created_at, term_id)  # Update the also created_at
            )
            conn.commit()  # Commit the transaction

            # Invalidate cache after updating a search term
            delete_cache('search_terms_all')

            return jsonify({"msg": "Search term updated", "term": term}), 200  # Return success response
    except Exception as e:
        return jsonify({"msg": str(e)}), 400  # Handle errors
    finally:
        conn.close()  # Ensure the database connection is closed

# Delete Search Term
@search_terms_bp.route('/api/search-terms/<int:term_id>', methods=['DELETE'])
@jwt_required()  # Ensure the user is authenticated
def delete_search_term(term_id):
    """
    Deletes a search term from the database.

    Args:
        term_id (int): The ID of the search term to delete.

    Returns:
        jsonify: A response indicating the outcome of the deletion operation.
    """
    conn = get_db_connection()  # Establish database connection
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM search_terms WHERE id = %s", (term_id,))  # Delete the specified term
            conn.commit()  # Commit the transaction

            # Invalidate cache after deleting a search term
            delete_cache('search_terms_all')

            return jsonify({"msg": "Search term deleted"}), 200  # Return success response
    except Exception as e:
        return jsonify({"msg": str(e)}), 400  # Handle errors
    finally:
        conn.close()  # Ensure the database connection is closed

# Bulk Add Search Terms
@search_terms_bp.route('/api/search-terms/bulk', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated
def bulk_add_search_terms():
    """
    Adds multiple search terms to the database in bulk.

    Expects a JSON payload with a list of terms.

    Returns:
        jsonify: A response indicating the outcome of the bulk add operation.
    """
    data = request.json  # Get the incoming JSON data
    terms = data.get('terms')  # Extract the list of terms

    # Validate that the terms list is provided
    if not terms or not isinstance(terms, list):
        return jsonify({"msg": "A list of terms is required"}), 400  # Return if the input is invalid

    conn = get_db_connection()  # Establish database connection
    try:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO search_terms (term) VALUES (%s)", [(term,) for term in terms])  # Insert terms
            conn.commit()  # Commit the transaction

            # Invalidate cache after bulk adding terms
            delete_cache('search_terms_all')

            return jsonify({"msg": "Search terms added", "terms": terms}), 201  # Return success response
    except Exception as e:
        return jsonify({"msg": str(e)}), 400  # Handle errors
    finally:
        conn.close()  # Ensure the database connection is closed

# Bulk Edit Search Terms
@search_terms_bp.route('/api/search-terms/bulk', methods=['PUT'])
@jwt_required()  # Ensure the user is authenticated
def bulk_edit_search_terms():
    """
    Updates multiple search terms in the database in bulk.

    Expects a JSON payload with a list of terms, each containing an ID and a new term.

    Returns:
        jsonify: A response indicating the outcome of the bulk edit operation.
    """
    data = request.json  # Get the incoming JSON data
    terms = data.get('terms')  # Extract the list of terms

    # Validate that the terms list is provided
    if not terms or not isinstance(terms, list):
        return jsonify({"msg": "A list of terms with IDs is required"}), 400  # Return if the input is invalid

    conn = get_db_connection()  # Establish database connection
    try:
        with conn.cursor() as cur:
            for term in terms:
                cur.execute("UPDATE search_terms SET term = %s WHERE id = %s", (term['term'], term['id']))  # Update each term
            conn.commit()  # Commit the transaction

            # Invalidate cache after bulk editing terms
            delete_cache('search_terms_all')

            return jsonify({"msg": "Search terms updated"}), 200  # Return success response
    except Exception as e:
        return jsonify({"msg": str(e)}), 400  # Handle errors
    finally:
        conn.close()  # Ensure the database connection is closed

# Bulk Delete Search Terms
@search_terms_bp.route('/api/search-terms/bulk', methods=['DELETE'])
@jwt_required()  # Ensure the user is authenticated
def bulk_delete_search_terms():
    """
    Deletes multiple search terms from the database in bulk.

    Expects a JSON payload with a list of IDs corresponding to the terms to be deleted.

    Returns:
        jsonify: A response indicating the outcome of the bulk delete operation.
    """
    data = request.json  # Get the incoming JSON data
    ids = data.get('ids')  # Extract the list of IDs

    # Validate that the IDs list is provided
    if not ids or not isinstance(ids, list):
        return jsonify({"msg": "A list of IDs is required"}), 400  # Return if the input is invalid

    conn = get_db_connection()  # Establish database connection
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM search_terms WHERE id IN %s", (tuple(ids),))  # Delete specified terms
            conn.commit()  # Commit the transaction

            # Invalidate cache after bulk deleting terms
            delete_cache('search_terms_all')

            return jsonify({"msg": "Search terms deleted"}), 200  # Return success response
    except Exception as e:
        return jsonify({"msg": str(e)}), 400  # Handle errors
    finally:
        conn.close()  # Ensure the database connection is closed