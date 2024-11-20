from app.config import get_db_connection
from app.scrapers.scraper import scrape_tenders
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_urls_and_terms(db_connection):
    """Retrieves URLs and search terms from the database."""
    try:
        with db_connection.cursor() as cur:
            cur.execute("SELECT url FROM websites")
            urls = [row[0] for row in cur.fetchall()]

            cur.execute("SELECT term FROM search_terms")
            search_terms = [row[0] for row in cur.fetchall()]

        print(f"Fetched URLs: {urls}")  # Print the fetched URLs
        print(f"Fetched Search Terms: {search_terms}")  # Print the fetched search terms
        return urls, search_terms
    except Exception as e:
        logging.error(f"Error fetching URLs and search terms: {e}")
        return [], []

def scrape_tenders_from_websites(selected_engines=None, time_frame=None, file_type=None, terms=None):
    """Scrapes tenders from specified websites using search terms and stores results in the database."""
    db_connection = None
    try:
        db_connection = get_db_connection()  # Get the database connection
        urls, search_terms = fetch_urls_and_terms(db_connection)

        current_year = datetime.now().year

        # Adjust method to construct queries with incoming terms
        queries = [
            f"site:{url.split('//')[1].rstrip('/')} ( " + " OR ".join([f'"{term}"' for term in terms]) + f") {current_year}"
            for url in urls
        ]

        # You might want to check or filter based on selected_engines if needed

        all_tenders = []
        total_found_tenders = 0

        for query in queries:
            logging.info(f"Scraping for query: {query}")  # Print current query being scraped
            scraped_tenders = scrape_tenders(db_connection, query, selected_engines)  # Use selected engines

            # Count found tenders after scraping
            total_found_tenders += len(scraped_tenders)
            all_tenders.extend(scraped_tenders)

        logging.info(f"Scraping completed. Total tenders found: {total_found_tenders}")

    except Exception as e:
        logging.error(f"An error occurred while scraping: {e}")
    finally:
        if db_connection is not None:
            db_connection.close()  # Ensure the connection is closed




if __name__ == "__main__":
    scrape_tenders_from_websites()
