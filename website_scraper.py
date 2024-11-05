from config import get_db_connection
from scraper import scrape_tenders
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_tenders_from_websites():
    """Scrapes tenders from specified websites using search terms and stores results in the database."""
    try:
        db_connection = get_db_connection()  # Get the database connection

        with db_connection:
            cur = db_connection.cursor()
            cur.execute("SELECT url FROM websites")
            urls = [row[0] for row in cur.fetchall()]

            cur.execute("SELECT term FROM search_terms")
            search_terms = [row[0] for row in cur.fetchall()]

            current_year = datetime.now().year
            queries = []

            for url in urls:
                search_query = f"site:{url} " + " OR ".join([f'"{term}"' for term in search_terms]) + f" {current_year}"
                queries.append(search_query)

            search_engines = ["Google", "Bing", "Yahoo", "DuckDuckGo", "Ask"]

            tenders = []
            for query in queries:
                # Pass the db_connection to scrape_tenders
                scraped_tenders = scrape_tenders(search_engines, [query], db_connection)
                tenders.extend(scraped_tenders)

            logging.info(f"Scraping completed. Total tenders found: {len(tenders)}")

    except Exception as e:
        logging.error(f"An error occurred while scraping: {e}")

if __name__ == "__main__":
    scrape_tenders_from_websites()
