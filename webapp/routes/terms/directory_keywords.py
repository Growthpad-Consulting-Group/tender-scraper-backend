from flask import Blueprint, jsonify, request
from webapp.config import get_db_connection
from flask_jwt_extended import jwt_required
import logging
from webapp.db.db import (
    get_directory_keywords,
    add_directory_keyword_to_db,
    remove_directory_keyword_from_db,
    rename_directory_keyword
)
from webapp.cache.redis_cache import get_cache, set_cache, delete_cache

directory_keywords_bp = Blueprint('directory_keywords_bp', __name__)

# Cache key template for the keywords
CACHE_KEY_TEMPLATE = 'directory_keywords_{}'
CACHE_EXPIRY = 300  # Cache expiry time in seconds, e.g., 5 minutes

# Function to get keywords from the directory_keywords table
def get_directory_keywords(connection, tender_type=None):
    with connection.cursor() as cursor:
        query = "SELECT keyword, tender_type, created_at FROM directory_keywords"  # Include created_at

        if tender_type:
            query += " WHERE tender_type = %s"
            cursor.execute(query, (tender_type,))
        else:
            cursor.execute(query)

        # Fetch rows as tuples
        fetched_data = cursor.fetchall()

    # Convert fetched data into a list of dictionaries including created_at
    keyword_list = [{"keyword": row[0], "tenderType": row[1], "created_at": row[2]} for row in fetched_data]

    return keyword_list  # Return list of dictionaries


# Get All keywords for Directory
@directory_keywords_bp.route('/api/directory_keywords', methods=['GET'])
@jwt_required()
def directory_keywords():
    """Fetches keywords from the directory_keywords table."""
    tender_type = request.args.get('tender_type')  # Optional parameter
    db_connection = None

    try:
        db_connection = get_db_connection()
        if db_connection is None:
            raise Exception("Failed to connect to the database")

        logging.info("Connected to the database successfully.")

        keywords = get_directory_keywords(db_connection, tender_type)

        if isinstance(keywords, list):
            if all(isinstance(item, dict) for item in keywords):
                return jsonify(keywords), 200  # Return the keywords as a list of dictionaries
            else:
                logging.error("Fetched data is not a list of dictionaries.")
                return jsonify({"error": "Unexpected data structure returned from the database."}), 500
        else:
            logging.error("Fetched data is not a list.")
            return jsonify({"error": "Unexpected data structure returned from the database."}), 500

    except Exception as e:
        logging.error(f"Error fetching directory keywords: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if db_connection:
            try:
                db_connection.close()
                logging.info("Database connection closed successfully.")
            except Exception as close_error:
                logging.warning(f"Error closing the database connection: {str(close_error)}")

# Endpoint to add a new keyword
@directory_keywords_bp.route('/api/directory_keywords', methods=['POST'])
@jwt_required()
def add_keyword():
    """Adds a new keyword to the directory_keywords table and clears the cache."""
    new_keyword = request.json.get('keyword')  # Keyword from the request
    tender_type = request.json.get('tenderType')  # Add retrieval for tender type

    if not new_keyword:
        return jsonify({"error": "Keyword is required"}), 400

    db_connection = None
    try:
        db_connection = get_db_connection()
        if db_connection is None:
            raise Exception("Failed to connect to the database")

        # Add the keyword along with the tender type to the database
        add_directory_keyword_to_db(db_connection, new_keyword, tender_type)  # Make sure the function accepts tender_type

        # Invalidate the existing cache
        delete_cache(CACHE_KEY_TEMPLATE.format(new_keyword))  # Adjusting cache key if needed according to the new keyword
        logging.info(f"Keyword '{new_keyword}' with tender type '{tender_type}' added and cache invalidated.")

        return jsonify({"message": "Keyword added successfully"}), 201
    except Exception as e:
        logging.error(f"Error adding keyword: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if db_connection:
            try:
                db_connection.close()
                logging.info("Database connection closed successfully.")
            except Exception as close_error:
                if 'closed' in str(close_error).lower():
                    logging.info("Database connection was already closed.")
                else:
                    logging.warning(f"Error closing the database connection: {str(close_error)}")

# Endpoint to remove a keyword
@directory_keywords_bp.route('/api/directory_keywords/<string:keyword>', methods=['DELETE'])
@jwt_required()
def remove_keyword(keyword):
    """Removes a keyword from the directory_keywords table and clears the cache."""
    db_connection = None
    cursor = None  # Explicitly declare the cursor here
    try:
        # Establish the database connection
        db_connection = get_db_connection()
        if db_connection is None:
            raise Exception("Failed to connect to the database")

        # Explicitly create the cursor here before using it
        cursor = db_connection.cursor()

        # Call the function to remove the keyword from the database
        remove_directory_keyword_from_db(cursor, keyword)  # Pass the cursor to the function

        # Invalidate the existing cache
        delete_cache(CACHE_KEY_TEMPLATE.format(keyword))  # Invalidate cache for the specific keyword
        logging.info(f"Keyword '{keyword}' removed and cache invalidated.")

        # Commit the transaction
        db_connection.commit()

        return jsonify({"message": "Keyword removed successfully"}), 200

    except Exception as e:
        logging.error(f"Error removing keyword: {str(e)}", exc_info=True)  # Log the exception with full stack trace
        if db_connection:
            db_connection.rollback()  # Rollback the transaction if an error occurs
        return jsonify({"error": str(e)}), 500

    finally:
        # Ensure the cursor is closed if it was created
        if cursor:
            try:
                cursor.close()  # Close the cursor explicitly
                logging.info("Cursor closed successfully.")
            except Exception as cursor_error:
                logging.warning(f"Error closing cursor: {str(cursor_error)}")

        # Ensure the database connection is closed if it was created
        if db_connection:
            try:
                db_connection.close()  # Close the connection explicitly
                logging.info("Database connection closed successfully.")
            except Exception as close_error:
                if 'closed' in str(close_error).lower():
                    logging.info("Database connection was already closed.")
                else:
                    logging.warning(f"Error closing the database connection: {str(close_error)}")


# Endpoint to rename a keyword
@directory_keywords_bp.route('/api/directory_keywords/rename', methods=['PUT'])
@jwt_required()
def rename_keyword():
    """Renames a directory keyword in the directory_keywords table and clears the cache."""
    old_keyword = request.json.get('old_keyword')
    new_keyword = request.json.get('new_keyword')

    if not old_keyword or not new_keyword:
        logging.error(f"Missing old_keyword or new_keyword. old_keyword: {old_keyword}, new_keyword: {new_keyword}")
        return jsonify({"error": "Both old_keyword and new_keyword are required"}), 400

    db_connection = None
    cursor = None
    try:
        db_connection = get_db_connection()
        if db_connection is None:
            raise Exception("Failed to connect to the database")

        logging.info(f"Renaming keyword from '{old_keyword}' to '{new_keyword}' in the database.")

        # Create the cursor manually and ensure it stays open during the transaction
        cursor = db_connection.cursor()
        query = "UPDATE directory_keywords SET keyword = %s WHERE keyword = %s"
        cursor.execute(query, (new_keyword, old_keyword))

        # Commit the transaction
        db_connection.commit()
        logging.info(f"Executed query: {query} with {cursor.rowcount} rows affected.")

        # Invalidate the existing cache
        delete_cache(CACHE_KEY_TEMPLATE.format(old_keyword))  # Invalidate cache for the old keyword
        delete_cache(CACHE_KEY_TEMPLATE.format(new_keyword))  # Cache for the new keyword if needed
        logging.info(f"Keyword '{old_keyword}' renamed to '{new_keyword}' and cache invalidated.")

        return jsonify({"message": f"Keyword renamed from '{old_keyword}' to '{new_keyword}' successfully."}), 200

    except Exception as e:
        logging.error(f"Error renaming keyword: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()  # Close the cursor explicitly
        if db_connection:
            try:
                db_connection.close()  # Close the connection
                logging.info("Database connection closed successfully.")
            except Exception as close_error:
                logging.warning(f"Error closing the database connection: {str(close_error)}")
