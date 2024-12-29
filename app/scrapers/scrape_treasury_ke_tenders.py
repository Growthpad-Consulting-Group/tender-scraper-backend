import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from app.config import get_db_connection
from app.routes.tenders.tender_utils import insert_tender_to_db
from app.db.db import get_directory_keywords

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_format(url):
    """Determine the document format based on the URL."""
    if url:
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
            logger.warning("Database connection is None. Reconnecting...")
            return None

        # Test the connection by executing a simple query
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")  # Simple query to test connection
        cursor.close()
        return db_connection
    except Exception as e:
        logger.error(f"Error establishing or testing database connection: {str(e)}")
        return None

def scrape_treasury_ke_tenders():
    """Scrapes tenders from the Kenya Treasury website and inserts them into the database."""
    url = "https://www.treasury.go.ke/tenders/"

    try:
        response = requests.get(url)

        if response.status_code != 200:
            logger.error(f"Failed to retrieve Kenya Treasury page, status code: {response.status_code}")
            return

        logger.info("Successfully retrieved Kenya Treasury page.")
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table
        table = soup.find('table', {'id': 'tablepress-3'})
        if not table:
            logger.warning("The expected tender table was not found.")
            return

        rows = table.find_all('tr')[1:]  # Skip the header row
        logger.info(f"Found {len(rows)} rows in the tender table.")

        # Create the database connection
        db_connection = ensure_db_connection()
        if not db_connection:
            logger.error("Failed to establish a database connection.")
            return

        # Fetch keywords related to "Kenya Treasury" from the database
        keywords = get_directory_keywords(db_connection, "Kenya Treasury")
        if not keywords:
            logger.warning("No keywords found for 'Kenya Treasury'. Aborting scrape.")
            return
        keywords = [keyword.lower() for keyword in keywords]
        current_year = datetime.now().year  # Get the current year

        for row in rows:
            columns = row.find_all('td')
            if len(columns) < 5:  # Ensure there are enough columns
                continue

            reference_number = columns[0].text.strip()
            title = columns[1].text.strip()
            document_url = columns[2].find('a')['href'] if columns[2].find('a') else None
            deadline_str = columns[4].text.strip()

            # logger.info(f"Deadline string found for tender '{title}': {deadline_str}")

            try:
                deadline_date = datetime.strptime(deadline_str, "%Y-%m-%d %H:%M:%S").date()
            except ValueError as e:
                logger.error(f"Error parsing deadline date for tender '{title}': {e}")
                continue

            # Only process tenders with a deadline in the current year
            if deadline_date.year != current_year:
                continue

            # Check if the title contains any of the keywords
            if not any(keyword in title.lower() for keyword in keywords):
                continue  # Skip if there are no matching keywords

            status = "open" if deadline_date > datetime.now().date() else "closed"

            # Determine the format based on the document URL
            format_type = get_format(document_url)

            tender_data = {
                'title': title,
                'description': reference_number,
                'closing_date': deadline_date,
                'source_url': document_url,
                'status': status,
                'format': format_type,  # Set format based on URL
                'scraped_at': datetime.now().date(),
                'tender_type': "Kenya Treasury"  # Specifying the tender type
            }

            # Ensure database connection is valid before insertion
            db_connection = ensure_db_connection()
            if not db_connection:
                logger.error("Database connection dropped. Skipping insertion.")
                continue  # Skip to the next tender if the connection failed

            try:
                insert_tender_to_db(tender_data, db_connection)  # Insert the tender
                logger.info(f"Tender inserted into database: {title}")
                logger.info(f"Title: {title}\n"
                            f"Reference Number: {reference_number}\n"
                            f"Closing Date: {deadline_date}\n"
                            f"Status: {status}\n"
                            f"Source URL: {document_url}\n"  # Include the URL in logs
                            f"Format: {format_type}\n"  # Display the determined format
                            f"Tender Type: Kenya Treasury\n")
                logger.info("=" * 40)  # Separator for readability
            except Exception as e:
                logger.error(f"Error inserting tender '{title}' into database: {e}")

        logger.info("Scraping completed.")

    except Exception as e:
        logger.error(f"An error occurred while scraping: {e}")
    finally:
        if db_connection:  # Ensure the connection is closed if it was opened
            db_connection.close()

if __name__ == "__main__":
    scrape_treasury_ke_tenders()