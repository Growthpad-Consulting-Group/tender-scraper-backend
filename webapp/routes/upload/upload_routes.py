from flask import Blueprint, request, jsonify
from webapp.cache.redis_cache import set_cache, get_cache, delete_cache  # Include delete_cache for cache invalidation
import pandas as pd
import zipfile
from flask_jwt_extended import jwt_required
import logging
import pg8000
import threading
from webapp.config import get_db_connection

# Set up logging
logging.basicConfig(level=logging.DEBUG,  # Change to DEBUG to capture all logs
                    format='%(asctime)s - %(levelname)s - %(message)s')

upload_bp = Blueprint('upload_bp', __name__)

# Global flag to check if the upload should be canceled
UPLOAD_CANCELLED = False

# Lock to prevent race conditions when updating the global flag
upload_cancel_lock = threading.Lock()


# Constants for batch size
BATCH_SIZE = 100
UPLOAD_PROGRESS = {}

def reset_progress():
    global UPLOAD_PROGRESS  # Declare UPLOAD_PROGRESS as global
    UPLOAD_PROGRESS = {"processed": 0, "total": 0}



# POST method to upload Excel data to the database
@upload_bp.route('/api/upload', methods=['POST'])
@jwt_required()
def upload_file():
    global UPLOAD_CANCELLED  # Use the global cancellation flag
    reset_progress()  # Reset progress on new upload
    logging.debug("Received request to upload file")

    # Check for file in the request
    if 'file' not in request.files:
        logging.warning("No file part in the request")
        return jsonify({"msg": "No file part"}), 400

    file = request.files['file']

    if file.filename == '':
        logging.warning("No selected file")
        return jsonify({"msg": "No selected file"}), 400

    # Log the file properties
    try:
        file_content = file.read()
        logging.info(f"File content length: {len(file_content)}")
        file.seek(0)  # Reset file pointer
    except Exception as e:
        logging.error(f"Error reading file content: {e}")
        return jsonify({"msg": "File upload error"}), 500

    logging.info(f"File type: {file.content_type}")

    # Check file extension
    file_extension = file.filename.split('.')[-1].lower()
    logging.debug(f"Received file: {file.filename} with extension: {file_extension}")

    # Read the file into a DataFrame
    try:
        if file_extension == 'xlsx':
            df = pd.read_excel(file, engine='openpyxl')
            logging.info(f"Successfully read an Excel file: {file.filename}")
        elif file_extension == 'csv':
            df = pd.read_csv(file)
            logging.info(f"Successfully read a CSV file: {file.filename}")
        else:
            logging.error("Invalid file extension")
            return jsonify({"msg": "Invalid file format. Please upload a CSV or XLSX file."}), 400

        logging.info(f"DataFrame shape: {df.shape}")
        logging.info(f"First few rows:\n{df.head()}")

    except pd.errors.EmptyDataError:
        logging.error("No data in the file")
        return jsonify({"msg": "The file is empty or no data."}), 400
    except zipfile.BadZipFile:
        logging.error("Uploaded file is not a valid Excel file")
        return jsonify({"msg": "Uploaded file is not a valid Excel file. Please check the file and try again."}), 400
    except Exception as e:
        logging.exception("Error reading the file")
        return jsonify({"msg": "Error reading the file", "error": str(e)}), 500

    overwrite = request.args.get('overwrite', 'false').lower() == 'true'

    # Prepare data for insertion
    insert_data = df[['Website Name', 'URL', 'Location']].fillna('').values.tolist()
    UPLOAD_PROGRESS['total'] = len(insert_data)  # Set total for progress tracking
    logging.info(f"Prepared insert_data with {len(insert_data)} rows")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                processed_count = 0
                overwrite_count = 0  # Track number of overwritten URLs
                duplicate_urls = []
                overwrite_url = request.args.get('url')
                overwrite_urls = set()

                for website_name, url, location in insert_data:
                    # Check if the upload is cancelled
                    with upload_cancel_lock:
                        if UPLOAD_CANCELLED:
                            logging.info("Upload process has been canceled")
                            return jsonify({"msg": "Upload cancelled by user"}), 400

                    logging.debug(f"Processing: name={website_name}, url={url}, location={location}")
                    try:
                        cur.execute("SELECT id FROM websites WHERE url = %s", (url,))
                        existing_website = cur.fetchone()

                        if existing_website:
                            # Overwrite only if it's the specified URL or if general overwrite is allowed
                            if overwrite and (url == overwrite_url or overwrite_url is None):
                                logging.info(f"Overwriting URL: {url}")
                                cur.execute("UPDATE websites SET name = %s, location = %s, tender_type = %s WHERE url = %s",
                                            (website_name, location, "Uploaded Websites", url))
                                overwrite_count += 1
                                logging.info(f"Incremented overwritten count to: {overwrite_count}")  # Log overwrite count
                                processed_count += 1
                                # continue  # Skip checking for duplicates after overwrite

                        else:
                            # Insert new entry
                            cur.execute(
                                "INSERT INTO websites (name, url, location, tender_type) VALUES (%s, %s, %s, %s)",
                                (website_name, url, location, "Uploaded Websites")
                            )

                        processed_count += 1
                        UPLOAD_PROGRESS['processed'] += 1  # Increment processed count

                    except pg8000.dbapi.DatabaseError as e:
                        logging.error(f"Error inserting/updating website {url}: {e}")
                        return jsonify({"msg": "Database error", "error": str(e)}), 500

                conn.commit()  # Commit the changes after processing each entry
                logging.info(f"Total processed URLs: {processed_count}")

                # Log overwritten count before sending response
                logging.info(f"Total overwritten URLs: {overwrite_count}")

                if duplicate_urls:
                    logging.warning(f"Duplicate URLs: {duplicate_urls}")
                    return jsonify({
                        "msg": f"Processed {processed_count} websites. Duplicate URLs found.",
                        "duplicate_urls": duplicate_urls,
                        "overwritten_count": overwrite_count
                    }), 409
                else:
                    return jsonify({
                        "msg": f"{processed_count} URLs processed successfully",
                        "overwritten_count": overwrite_count
                    }), 201

                delete_cache('total_websites_count')
                delete_cache('websites_page_1_perpage_50_tendertype_Uploaded Websites')

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

@upload_bp.route('/api/get-upload-progress', methods=['GET'])
@jwt_required()
def get_upload_progress():
    return jsonify(UPLOAD_PROGRESS), 200


@upload_bp.route('/api/websites', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated
def add_website():
    data = request.get_json()

    # Validate input, but location is now optional
    if not data.get('name') or not data.get('url'):
        return jsonify({"msg": "Name and URL are required fields"}), 400

    name = data['name']
    url = data['url']
    location = data.get('location')  # Get location, if it exists
    tender_type = "Uploaded Websites"  # Set tender_type to a fixed value

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Prepare the insert statement. The location can be NULL if not provided.
                cur.execute("""
                    INSERT INTO websites (name, url, location, tender_type) 
                    VALUES (%s, %s, %s, %s) 
                    RETURNING id, name, url, location, tender_type
                """, (name, url, location, tender_type))  # Include tender_type in the insertion
                new_website = cur.fetchone()

                # Commit the transaction while still inside the context
                conn.commit()

        # Prepare the response
        response = {
            "newWebsite": {
                "id": new_website[0],
                "name": new_website[1],
                "url": new_website[2],
                "location": new_website[3],  # This may be None if not provided
                "tender_type": new_website[4]  # Return the tender_type as well
            }
        }

        # Invalidate the relevant cache
        delete_cache('total_websites_count')  # Clear cached total

        return jsonify(response), 201
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
            # Log that data was fetched from the cache
            logging.info(f"Fetched websites from cache for key: {cache_key}")
            return jsonify(cached_result), 200  # Return cached response

        # If not cached, proceed to query the database
        logging.info(f"Cache miss for key: {cache_key}. Fetching data from database.")

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
        logging.info(f"Fetched websites from database and cached result for key: {cache_key}")

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