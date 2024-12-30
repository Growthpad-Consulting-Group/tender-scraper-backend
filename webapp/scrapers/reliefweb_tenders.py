import requests
import logging
import time
import urllib.parse
from datetime import datetime
from webapp.config import get_db_connection
from webapp.routes.tenders.tender_utils import insert_tender_to_db
from webapp.db.db import get_directory_keywords


def get_format(url):
    """Determine the document format based on the URL."""
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'  # Default to HTML if no specific format is found


def ensure_db_connection():
    """Check and ensure the database connection is valid."""
    try:
        db_connection = get_db_connection()
        if db_connection is None:
            logging.warning("Database connection is None. Reconnecting...")
            return None

        # Test the connection by executing a simple query
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")  # Simple query to test connection
        cursor.close()
        return db_connection
    except Exception as e:
        logging.error(f"Error establishing or testing database connection: {str(e)}")
        return None


def fetch_reliefweb_tenders():
    """Fetches tenders from the ReliefWeb API and inserts them into the database."""
    base_api_url = "https://api.reliefweb.int/v1/jobs?appname=gcg-tender&profile=list&preset=latest&slim=1"
    session = requests.Session()  # Use a session for persistent connections

    retry_attempts = 3
    for attempt in range(retry_attempts):
        try:
            logging.info(f"Attempting to fetch tenders, attempt {attempt + 1}")

            # Ensure we have a valid database connection
            db_connection = ensure_db_connection()
            if not db_connection:
                logging.error("Failed to establish a database connection.")
                return []

            # Fetch keywords from the database
            try:
                logging.info("Fetching keywords from the database...")
                keywords = get_directory_keywords(db_connection)
                logging.info(f"Fetched keywords: {keywords}")
            except Exception as db_error:
                logging.error(f"Error fetching keywords from database: {db_error}")
                return []

            if not keywords:
                logging.warning("No keywords found in the database.")
                return []

            logging.info("Keywords fetched successfully.")

            # Construct the API URL with the keyword query
            keyword_query = ' OR '.join(keywords)
            country_ids = "131 OR 217 OR 198 OR 102 OR 240 OR 216 OR 175 OR 231 OR 244 OR 256 OR 96 OR 87 OR 82 OR 16"
            type_id = 264
            query_value = f"({keyword_query}) AND (country.id:({country_ids}) AND type.id:{type_id})"
            encoded_query_value = urllib.parse.quote(query_value)
            api_url = f"{base_api_url}&query%5Bvalue%5D={encoded_query_value}&query%5Boperator%5D=AND"

            # Fetch data from the ReliefWeb API with retry logic
            response = None
            for _ in range(3):  # Retry up to 3 times with increasing backoff
                try:
                    logging.info(f"Sending API request.")
                    response = session.get(api_url, timeout=10)  # Set a timeout of 10 seconds
                    if response.status_code == 200:
                        logging.info(f"API request successful: {response.status_code}")
                        break
                    else:
                        logging.error(f"Failed to fetch tenders, status code: {response.status_code}")
                except requests.RequestException as e:
                    logging.error(f"Request error: {str(e)}")
                time.sleep(2 ** _)  # Exponential backoff (1s, 2s, 4s, ...)

            if response is None or response.status_code != 200:
                logging.error("Failed to fetch tenders after multiple attempts.")
                return []

            # Process the API response
            data = response.json()
            tenders = []

            # Reconnect to the database to insert tender data
            db_connection = ensure_db_connection()  # Ensure connection is active
            if not db_connection:
                logging.error("Failed to establish a database connection for insertion.")
                return []

            # Process and insert tenders into the database
            for job in data['data']:
                title = job['fields']['title']
                closing_date = job['fields']['date'].get('closing') if 'date' in job['fields'] else None

                if closing_date:
                    closing_date_obj = datetime.strptime(closing_date, "%Y-%m-%dT%H:%M:%S%z").date()
                    status = "open" if closing_date_obj > datetime.now().date() else "closed"
                    organization = job['fields']['source'][0]['name'] if job['fields'].get('source') else 'Unknown'

                    # Prepare the tender info
                    source_url = job['fields']['url']
                    format_type = get_format(source_url)

                    tender_info = {
                        'title': title,
                        'closing_date': closing_date_obj,
                        'source_url': source_url,
                        'status': status,
                        'format': format_type,
                        'description': organization,
                        'scraped_at': datetime.now(),
                        'tender_type': "ReliefWeb Jobs"
                    }
                    tenders.append(tender_info)

                    # Insert tender information into the database
                    try:
                        logging.info(f"Attempting to insert tender '{title}' into the database.")
                        success = insert_tender_to_db(tender_info, db_connection)
                        if success:
                            logging.info(f"Inserted Tender: {tender_info['title']}")
                        else:
                            logging.error(f"Failed to insert Tender: {tender_info['title']}")
                    except Exception as insert_error:
                        logging.error(f"Error during inserting tender '{title}': {str(insert_error)}")

                else:
                    logging.warning(f"Skipping job '{title}' due to missing closing date.")

            break  # Exit retry loop after successful execution

        except Exception as e:
            logging.error(f"An error occurred during attempt {attempt + 1}: {str(e)}")
            time.sleep(5)  # Exponential backoff on errors

    session.close()  # Always close the session after the task


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)  # Set logging level
    fetch_reliefweb_tenders()
