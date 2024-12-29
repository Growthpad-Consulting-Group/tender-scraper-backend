import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import time
from app.config import get_db_connection
from app.routes.tenders.tender_utils import insert_tender_to_db
from app.db.db import get_directory_keywords

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

def scrape_jobinrwanda_tenders():
    """Scrapes tenders from Job in Rwanda website and inserts them into the database based on specific keywords."""
    url = "https://www.jobinrwanda.com/jobs/tender"
    retry_attempts = 3

    # Use a session for requests
    session = requests.Session()

    for attempt in range(retry_attempts):
        try:
            logger.info(f"Attempting to fetch tenders, attempt {attempt + 1}")

            # Ensure we have a valid database connection
            db_connection = ensure_db_connection()
            if not db_connection:
                logger.error("Failed to establish a database connection.")
                return []

            # Fetch keywords related to "Job in Rwanda" from the database
            keywords = get_directory_keywords(db_connection, "Job in Rwanda")
            if not keywords:
                logger.warning("No keywords found for 'Job in Rwanda'. Aborting scrape.")
                return []
            keywords = [keyword.lower() for keyword in keywords]

            # Make request to fetch tenders
            response = session.get(url, timeout=10)  # Set a timeout of 10 seconds
            if response.status_code != 200:
                logger.error(f"Failed to retrieve tenders page, status code: {response.status_code}")
                time.sleep(5)  # Wait before retrying
                continue  # Retry fetching the tenders

            logger.info("Successfully retrieved tenders page.")
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all tender cards
            tender_cards = soup.find_all('article', class_='node--type-job')
            logger.info(f"Found {len(tender_cards)} tenders.")        

            # Iterate through each tender card
            for card in tender_cards:
                title_tag = card.find('h5', class_='card-title')
                if title_tag:
                    anchor_tag = title_tag.find_parent('a')
                    if anchor_tag and 'href' in anchor_tag.attrs:
                        title = anchor_tag.find('span').get_text(strip=True).lower()
                        source_url = f"https://www.jobinrwanda.com{anchor_tag['href']}"
                    else:
                        logger.warning("Anchor tag not found or href missing.")
                        continue
                else:
                    logger.warning("Title tag not found.")
                    continue

                # Check if the title contains any of the keywords
                matched_keywords = [keyword for keyword in keywords if keyword in title]
                if matched_keywords:
                    description_tag = card.find('p', class_='card-text')
                    description = description_tag.get_text(strip=True) if description_tag else "N/A"

                    deadline_tag = description_tag.find('time', class_='datetime') if description_tag else None
                    closing_date_str = deadline_tag['datetime'] if deadline_tag else None

                    if closing_date_str:
                        closing_date = datetime.fromisoformat(closing_date_str.replace('Z', '+00:00')).date()
                        status = "open" if closing_date > datetime.now().date() else "closed"

                        format_type = get_format(source_url)

                        tender_data = {
                            'title': title,
                            'description': description,
                            'closing_date': closing_date,
                            'source_url': source_url,
                            'status': status,
                            'format': format_type,
                            'scraped_at': datetime.now().date(),
                            'tender_type': "Job in Rwanda"
                        }

                        # Ensure database connection is still valid before insertion
                        db_connection = ensure_db_connection()
                        if not db_connection:
                            logger.error("Database connection dropped. Reattempting insertion.")
                            continue  # Skip to the next tender if the connection failed

                        # Insert the tender into the database
                        insert_tender_to_db(tender_data, db_connection)
                        logger.info(f"Tender inserted into database: {title}")
                        logger.info(f"Matched keywords: {', '.join(matched_keywords)} for tender: {title}")

            break  # Exit retry loop after successful execution

        except requests.RequestException as e:
            logger.error(f"A request error occurred during attempt {attempt + 1}: {str(e)}")
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            logger.error(f"An unexpected error occurred: {str(e)}")
            time.sleep(5)

    session.close()  # Close the session after the task
    logger.info("Scraping completed.")

if __name__ == "__main__":
    scrape_jobinrwanda_tenders()