from flask import Blueprint, request, jsonify
import pandas as pd
from flask_jwt_extended import jwt_required
import logging
import pg8000
from config import get_db_connection

# Set up logging
logging.basicConfig(level=logging.INFO)

upload_bp = Blueprint('upload_bp', __name__)

# POST method to upload Excel data to the database
@upload_bp.route('/api/upload', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated
def upload_file():
    if 'file' not in request.files:
        return jsonify({"msg": "No file part"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"msg": "No selected file"}), 400

    # Check file extension to determine if it's CSV or XLSX
    file_extension = file.filename.split('.')[-1].lower()
    try:
        if file_extension == 'xlsx':
            # If the file is an Excel file
            df = pd.read_excel(file, engine='openpyxl')
        elif file_extension == 'csv':
            # If the file is a CSV file
            df = pd.read_csv(file)
        else:
            return jsonify({"msg": "Invalid file format. Please upload a CSV or XLSX file."}), 400

        # Strip whitespace from column headers
        df.columns = df.columns.str.strip()

        # Check if required columns exist
        required_columns = ['Website Name', 'URL']  # Updated to 'URL'
        for col in required_columns:
            if col not in df.columns:
                return jsonify({"msg": f"Missing required column: {col}"}), 400

        # Collect data for batch insertion
        insert_data = [(website_name, url, location)
                       for website_name, url, location in zip(df['Website Name'], df['URL'], df.get('Location', [None] * len(df)))]

    except Exception as e:
        logging.error(f"Error reading the file: {e}")
        return jsonify({"msg": "Error reading the file", "error": str(e)}), 500

    # Get the 'overwrite' parameter (default is False)
    overwrite = request.args.get('overwrite', 'false').lower() == 'true'

    # Store URLs in the database using batch insert or overwrite
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                duplicate_urls = []  # List to store all duplicate URLs
                processed_count = 0  # Keep track of how many websites were processed

                for website_name, url, location in insert_data:
                    try:
                        # Check if URL already exists
                        cur.execute("SELECT id FROM websites WHERE url = %s", (url,))
                        existing_website = cur.fetchone()

                        if existing_website:
                            if overwrite:
                                # Overwrite the specific duplicate URL
                                logging.info(f"Overwriting URL: {url}")
                                cur.execute(""" 
                                    UPDATE websites
                                    SET name = %s, location = %s
                                    WHERE url = %s
                                """, (website_name, location, url))
                                processed_count += 1
                            else:
                                # Collect duplicate URL without processing further
                                logging.warning(f"Duplicate URL found: {url}")
                                duplicate_urls.append(url)
                        else:
                            # Insert the new URL if no duplicate found
                            cur.execute(
                                "INSERT INTO websites (name, url, location) VALUES (%s, %s, %s)",
                                (website_name, url, location)
                            )
                            processed_count += 1

                    except pg8000.dbapi.DatabaseError as e:
                        logging.error(f"Error inserting/updating website {url}: {e}")
                        raise e

                # Commit the transaction
                conn.commit()

                # Return response with all duplicates
                if duplicate_urls:
                    return jsonify({
                        "msg": f"Processed {processed_count} websites. Duplicate URLs found.",
                        "duplicate_urls": duplicate_urls
                    }), 409
                else:
                    # Success response if all URLs processed successfully
                    return jsonify({"msg": f"{processed_count} URLs processed successfully"}), 201

    except Exception as e:
        logging.error(f"Error inserting data into the database: {e}")
        return jsonify({"msg": "Error adding URLs", "error": str(e)}), 500



# POST method to manually add a website
@upload_bp.route('/api/websites', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated
def add_website():
    data = request.get_json()

    # Validate the required fields: name and url
    if not data.get('name') or not data.get('url'):
        return jsonify({"msg": "Missing required fields: name and url are required."}), 400

    # If location is provided, use it; otherwise, set it to None (optional field)
    location = data.get('location')  # This can be None if not provided

    try:
        # Insert website data into the database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(""" 
                    INSERT INTO websites (name, url, location)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, url, location  -- Return the newly added website data
                """, (data['name'], data['url'], location))

                # Fetch the newly added website data
                new_website = cur.fetchone()

            conn.commit()

        return jsonify({
            "msg": "Website added successfully",
            "newWebsite": {
                "id": new_website[0],
                "name": new_website[1],
                "url": new_website[2],
                "location": new_website[3]
            }
        }), 201

    except Exception as e:
        logging.error(f"Error adding website: {e}")
        return jsonify({"msg": "Error adding website", "error": str(e)}), 500


# GET method to retrieve data from the database with pagination
@upload_bp.route('/api/websites', methods=['GET'])
@jwt_required()  # Ensure the user is authenticated
def get_websites():
    try:
        # Pagination parameters
        page = int(request.args.get('page', 1))  # Default page is 1
        per_page = int(request.args.get('per_page', 50))  # Default per_page is 50
        offset = (page - 1) * per_page

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Fetch websites with pagination
                cur.execute(
                    "SELECT id, name, url, location FROM websites LIMIT %s OFFSET %s",
                    (per_page, offset)
                )
                websites = cur.fetchall()

        # Return the data in JSON format
        website_list = [{"id": website[0], "name": website[1], "url": website[2], "location": website[3]} for website in websites]
        return jsonify({"websites": website_list, "page": page, "per_page": per_page}), 200

    except Exception as e:
        logging.error(f"Error retrieving websites: {e}")
        return jsonify({"msg": "Error retrieving websites", "error": str(e)}), 500


# PUT method to update website details by ID
@upload_bp.route('/api/websites/<int:id>', methods=['PUT'])
@jwt_required()  # Ensure the user is authenticated
def update_website(id):
    data = request.get_json()

    # Validate input
    if not data.get('name') or not data.get('url') or not data.get('location'):
        return jsonify({"msg": "Missing required fields"}), 400

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Update website details in the database
                cur.execute(""" 
                    UPDATE websites 
                    SET name = %s, url = %s, location = %s 
                    WHERE id = %s
                """, (data['name'], data['url'], data['location'], id))
            conn.commit()

        return jsonify({"msg": "Website updated successfully"}), 200
    except Exception as e:
        logging.error(f"Error updating website: {e}")
        return jsonify({"msg": "Error updating website", "error": str(e)}), 500


# DELETE method to delete a website by ID
@upload_bp.route('/api/websites/<int:id>', methods=['DELETE'])
@jwt_required()  # Ensure the user is authenticated
def delete_website(id):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Delete website from the database by id
                cur.execute("DELETE FROM websites WHERE id = %s", (id,))
            conn.commit()

        return jsonify({"msg": "Website deleted successfully"}), 200
    except Exception as e:
        logging.error(f"Error deleting website: {e}")
        return jsonify({"msg": "Error deleting website", "error": str(e)}), 500


# DELETE method to delete multiple websites by a list of IDs
@upload_bp.route('/api/websites', methods=['DELETE'])
@jwt_required()  # Ensure the user is authenticated
def bulk_delete_websites():
    ids = request.get_json().get('ids', [])
    if not ids:
        return jsonify({"msg": "No IDs provided"}), 400

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Delete websites in bulk
                cur.execute("DELETE FROM websites WHERE id = ANY(%s)", (ids,))
            conn.commit()

        return jsonify({"msg": f"Deleted {len(ids)} websites successfully"}), 200
    except Exception as e:
        logging.error(f"Error deleting websites: {e}")
        return jsonify({"msg": "Error deleting websites", "error": str(e)}), 500


# Optional: Count total websites
@upload_bp.route('/api/websites/count', methods=['GET'])
@jwt_required()  # Ensure the user is authenticated
def count_websites():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Count the total number of websites
                cur.execute("SELECT COUNT(*) FROM websites")
                count = cur.fetchone()[0]

        return jsonify({"total_websites": count}), 200
    except Exception as e:
        logging.error(f"Error counting websites: {e}")
        return jsonify({"msg": "Error counting websites", "error": str(e)}), 500
