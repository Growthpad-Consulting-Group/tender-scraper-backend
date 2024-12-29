import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from app.config import get_db_connection
from app.routes.tenders.tender_utils import insert_tender_to_db
from app.db.db import get_directory_keywords
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def scrape_undp_tenders():
    """Scrapes tenders from the UNDP procurement notices page and inserts them into the database."""
    url = "https://procurement-notices.undp.org/"

    try:
        # Ensure a valid database connection
        db_connection = ensure_db_connection()
        if not db_connection:
            logger.error("Failed to establish a database connection.")
            return

        # Fetch keywords related to UNDP from the database
        keywords = get_directory_keywords(db_connection, "UNDP")
        if not keywords:
            logger.warning("No keywords found for 'UNDP'. Aborting scrape.")
            return
        keywords = [keyword.lower() for keyword in keywords]

        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to retrieve UNDP page, status code: {response.status_code}")
            return

        logger.info("Successfully retrieved UNDP page.")
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all tender links
        tenders = soup.find_all('a', class_='vacanciesTableLink')
        logger.info(f"Found {len(tenders)} tenders.")

        for tender in tenders:
            title_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Title' in x.strip())
            title = title_label.find_next_sibling('span').text.strip() if title_label else "N/A"

            # Check if the title contains any of the keywords
            if not any(keyword in title.lower() for keyword in keywords):
                continue  # Skip if there are no matching keywords

            ref_no_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Ref No' in x.strip())
            reference_number = ref_no_label.find_next_sibling('span').text.strip() if ref_no_label else "N/A"

            deadline_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Deadline' in x.strip())
            deadline_str = deadline_label.find_next_sibling('span').find('nobr').text.strip() if deadline_label and deadline_label.find_next_sibling('span').find('nobr') else "N/A"

            logger.info(f"Deadline string found for tender '{title}': {deadline_str}")

            try:
                match = re.search(r'(\d{1,2}-\w{3}-\d{2})', deadline_str)
                if match:
                    cleaned_date = match.group(1)
                    logger.info(f"Cleaned date part: '{cleaned_date}'")
                    deadline_date = datetime.strptime(cleaned_date, "%d-%b-%y").date()
                else:
                    raise ValueError("Date not found in the deadline string.")
            except ValueError as e:
                logger.error(f"Error parsing deadline date for tender '{title}': {e}")
                continue

            status = "open" if deadline_date > datetime.now().date() else "closed"
            negotiation_id = tender['href'].split('=')[-1]
            source_url = f"https://procurement-notices.undp.org/view_negotiation.cfm?nego_id={negotiation_id}"

            # Determine the document format
            format_type = get_format(source_url)

            tender_data = {
                'title': title,
                'description': reference_number,
                'closing_date': deadline_date,
                'source_url': source_url,
                'status': status,
                'format': format_type,
                'scraped_at': datetime.now().date(),
                'tender_type': "UNDP"
            }

            # Ensure database connection is valid before insertion
            db_connection = ensure_db_connection()
            if not db_connection:
                logger.error("Database connection dropped. Skipping insertion.")
                continue  # Skip to the next tender if the connection failed

            try:
                insert_tender_to_db(tender_data, db_connection)  # Insert into the database
                logger.info(f"Tender inserted into database: {title}")
                logger.info(f"Title: {title}\n"
                            f"Reference Number: {reference_number}\n"
                            f"Closing Date: {deadline_date}\n"
                            f"Status: {status}\n"
                            f"Source URL: {source_url}\n"
                            f"Format: {format_type}\n"
                            f"Tender Type: UNDP\n")
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
    scrape_undp_tenders()