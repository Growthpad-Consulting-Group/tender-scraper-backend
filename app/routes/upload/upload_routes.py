from flask import Blueprint, request, jsonify
from app.cache.redis_cache import set_cache, get_cache, delete_cache  # Include delete_cache for cache invalidation
import pandas as pd
from flask_jwt_extended import jwt_required
import logging
import pg8000
from app.config import get_db_connection

# Set up logging
logging.basicConfig(level=logging.INFO)

upload_bp = Blueprint('upload_bp', __name__)

# Constants for batch size
BATCH_SIZE = 100

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
        required_columns = ['Website Name', 'URL']
        for col in required_columns:
            if col not in df.columns:
                return jsonify({"msg": f"Missing required column: {col}"}), 400

        # Add the 'tender_type' column with default value 'Uploaded Websites'
        df['tender_type'] = 'Uploaded Websites'

        # Collect data for batch insertion
        insert_data = [
            (website_name, url, location, tender_type)
            for website_name, url, location, tender_type in zip(
                df['Website Name'], df['URL'], df.get('Location', [None] * len(df)), df['tender_type']
            )
        ]

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
                processed_count = 0  # Track processed websites

                # Batch processing
                for i in range(0, len(insert_data), BATCH_SIZE):
                    batch = insert_data[i:i + BATCH_SIZE]
                    for website_name, url, location, tender_type in batch:
                        try:
                            # Check if URL already exists
                            cur.execute("SELECT id FROM websites WHERE url = %s", (url,))
                            existing_website = cur.fetchone()

                            if existing_website:
                                if overwrite:
                                    logging.info(f"Overwriting URL: {url}")
                                    cur.execute("""
                                        UPDATE websites
                                        SET name = %s, location = %s, tender_type = %s
                                        WHERE url = %s
                                    """, (website_name, location, tender_type, url))
                                    processed_count += 1
                                else:
                                    logging.warning(f"Duplicate URL found: {url}")
                                    duplicate_urls.append(url)
                            else:
                                cur.execute(
                                    "INSERT INTO websites (name, url, location, tender_type) VALUES (%s, %s, %s, %s)",
                                    (website_name, url, location, tender_type)
                                )
                                processed_count += 1

                        except pg8000.dbapi.DatabaseError as e:
                            logging.error(f"Error inserting/updating website {url}: {e}")
                            raise e

                    # Commit the batch
                    conn.commit()

                # Invalidate relevant cache after upload
                delete_cache('total_websites_count')
                delete_cache('websites_page_1_perpage_50_tendertype_Uploaded Websites')

                # Return response with all duplicates
                if duplicate_urls:
                    return jsonify({
                        "msg": f"Processed {processed_count} websites. Duplicate URLs found.",
                        "duplicate_urls": duplicate_urls
                    }), 409
                else:
                    return jsonify({"msg": f"{processed_count} URLs processed successfully"}), 201

    except Exception as e:
        logging.error(f"Error inserting data into the database: {e}")
        return jsonify({"msg": "Error adding URLs", "error": str(e)}), 500


# POST method to manually add a website
@upload_bp.route('/api/websites', methods=['POST'])
@jwt_required()
def add_website():
    data = request.get_json()

    # Validate required fields
    if not data.get('name') or not data.get('url'):
        return jsonify({"msg": "Missing required fields: name and url are required."}), 400

    location = data.get('location')  # Optional field
    tender_type = 'Uploaded Websites'  # Set tender_type for manual addition

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(""" 
                    INSERT INTO websites (name, url, location, tender_type)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, name, url, location
                """, (data['name'], data['url'], location, tender_type))

                new_website = cur.fetchone()

            conn.commit()

        # Invalidate the relevant cache
        delete_cache('total_websites_count')
        delete_cache('websites_page_1_perpage_50_tendertype_Uploaded Websites')

        return jsonify({
            "msg": "Website added successfully",
            "newWebsite": {
                "id": new_website[0],
                "name": new_website[1],
                "url": new_website[2],
                "location": new_website[3],
                "tender_type": tender_type  # Include tender_type in the response
            }
        }), 201

    except Exception as e:
        logging.error(f"Error adding website: {e}")
        return jsonify({"msg": "Error adding website", "error": str(e)}), 500


# GET method to retrieve data from the database with pagination
@upload_bp.route('/api/websites', methods=['GET'])
@jwt_required()
def get_websites():
    try:
        tender_type_filter = request.args.get('tender_type', 'Uploaded Websites')
        page = int(request.args.get('page', 1))
        per_page_str = request.args.get('per_page', '50')  # Get per_page as string

        # Check if per_page is 'all' and set a specific max value
        if per_page_str.lower() == 'all':
            per_page = 1000  # Or whatever max number of records you can handle in one go
        else:
            per_page = int(per_page_str)  # Convert it to an integer

        offset = (page - 1) * per_page

        # Check cache first
        cache_key = f'websites_page_{page}_perpage_{per_page}_tendertype_{tender_type_filter}'
        cached_result = get_cache(cache_key)
        if cached_result:
            return jsonify(cached_result), 200  # Return cached response

        # If not cached, proceed to query the database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Fetch total count of websites with the specified tender_type
                cur.execute("SELECT COUNT(*) FROM websites WHERE tender_type = %s", (tender_type_filter,))
                total_count = cur.fetchone()[0]

                # Fetch websites with pagination and the specified tender_type
                cur.execute("SELECT id, name, url, location FROM websites WHERE tender_type = %s LIMIT %s OFFSET %s",
                            (tender_type_filter, per_page, offset))
                websites = cur.fetchall()

        website_list = [{"id": website[0], "name": website[1], "url": website[2], "location": website[3]} for website in websites]
        response = {
            "websites": website_list,
            "total_websites": total_count,
            "page": page,
            "per_page": per_page
        }

        # Cache the result
        set_cache(cache_key, response)

        return jsonify(response), 200

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

        # Invalidate the relevant cache
        delete_cache('total_websites_count')  # Clear cached total
        delete_cache('websites_page_1_perpage_50_tendertype_Uploaded Websites')  # Optional: clear relevant cached pages

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

        # Invalidate the relevant cache
        delete_cache('total_websites_count')  # Clear cached total
        delete_cache('websites_page_1_perpage_50_tendertype_Uploaded Websites')  # Optional: clear relevant cached pages

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

        # Invalidate the relevant cache
        delete_cache('total_websites_count')  # Clear cached total
        delete_cache('websites_page_1_perpage_50_tendertype_Uploaded Websites')  # Optional: clear relevant cached pages

        return jsonify({"msg": f"Deleted {len(ids)} websites successfully"}), 200
    except Exception as e:
        logging.error(f"Error deleting websites: {e}")
        return jsonify({"msg": "Error deleting websites", "error": str(e)}), 500


# Optional: Count total websites
@upload_bp.route('/api/websites/count', methods=['GET'])
@jwt_required()
def count_websites():
    try:
        # Check cache first
        cached_count = get_cache('total_websites_count')
        if cached_count:
            return jsonify({"total_websites": cached_count}), 200  # Return cached response

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Count the total number of websites
                cur.execute("SELECT COUNT(*) FROM websites")
                count = cur.fetchone()[0]

        # Cache the result
        set_cache('total_websites_count', count)

        return jsonify({"total_websites": count}), 200
    except Exception as e:
        logging.error(f"Error counting websites: {e}")
        return jsonify({"msg": "Error counting websites", "error": str(e)}), 500