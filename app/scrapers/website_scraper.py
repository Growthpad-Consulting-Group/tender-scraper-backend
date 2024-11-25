from app.config import get_db_connection  # Import function to establish database connection
from app.scrapers.scraper import scrape_tenders  # Import the function used for scraping tenders
from datetime import datetime  # For handling date and time
import logging  # For logging operation statuses and errors

# Configure logging settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_urls_and_terms(db_connection):
    """
    Retrieves URLs and search terms from the database.

    Args:
        db_connection: The active database connection object.

    Returns:
        tuple: A tuple containing a list of URLs and a list of search terms.
    """
    try:
        with db_connection.cursor() as cur:
            # Execute query to fetch all URLs from the 'websites' table
            cur.execute("SELECT url FROM websites")
            urls = [row[0] for row in cur.fetchall()]  # Retrieve URLs

            # Execute query to fetch all search terms from the 'search_terms' table
            cur.execute("SELECT term FROM search_terms")
            search_terms = [row[0] for row in cur.fetchall()]  # Retrieve search terms

        # Print the fetched URLs and search terms for debugging purposes
        print(f"Fetched URLs: {urls}")
        print(f"Fetched Search Terms: {search_terms}")
        return urls, search_terms  # Return a tuple of URLs and search terms

    except Exception as e:
        # Log any error encountered during the fetching process
        logging.error(f"Error fetching URLs and search terms: {e}")
        return [], []  # Return empty lists in case of error

def scrape_tenders_from_websites(selected_engines=None, time_frame=None, file_type=None, terms=None, region=None):
    """
    Scrapes tenders from specified websites using search terms and stores results in the database.

    Args:
        selected_engines (list, optional): List of search engines to use; defaults to None.
        time_frame (str, optional): Time frame for scraping; defaults to None.
        file_type (str, optional): Type of files to consider; defaults to None.
        terms (list, optional): List of search terms to use for scraping; defaults to None.
        region (str, optional): The geographical region for which to scrape tenders; defaults to None.
    """
    db_connection = None
    try:
        # Establish database connection
        db_connection = get_db_connection()

        # Fetch URLs and search terms from the database
        urls, search_terms = fetch_urls_and_terms(db_connection)

        # Get the current year for the query
        current_year = datetime.now().year

        # Construct queries for scraping with URLs, search terms and region.
        google_queries = [
            f'site:{url.split("//")[1].rstrip("/")} ("{terms[0]}" OR "{terms[1]}") {current_year}' +
            f"&as_qdr={time_frame}&" +  # Date range filter (e.g., 'qdr:y' for the past year)
            f"as_epq=&" +  # Optional exact phrase (empty for now)
            f"&as_eq=&as_nlo=&as_nhi=&lr=&" +  # Various empty filters (as_eq = exact match, etc.)
            f"cr=country{region}&" +  # Country filter (e.g., countryKE)
            (f"as_filetype={file_type}&" if file_type and file_type != 'any' else "") +  # Add filetype filter if not 'any'
            f"as_occt=any&" +  # Correct placement of any file type filter if added
            f"tbs="  # Optional for additional filters, though in this case not necessary
            for url in urls
        ]




        bing_yahoo_queries = [
            f'site:{url.split("//")[1].rstrip("/")} ("{terms[0]}" OR "{terms[1]}")'
            for url in urls
        ]

        # Initialize lists for storing all scraped tenders and counting found tenders
        all_tenders = []
        total_found_tenders = 0

        # Choose queries based on selected engines
        for query in google_queries if 'Google' in selected_engines else bing_yahoo_queries:
            logging.info(f"Scraping for query: {query}")  # Log the current query being scraped
            # Call the scraping function and collect the returned tenders
            scraped_tenders = scrape_tenders(db_connection, query, selected_engines)

            # Count the total number of tenders found after scraping each query
            total_found_tenders += len(scraped_tenders)
            all_tenders.extend(scraped_tenders)  # Add newly scraped tenders to the list

        logging.info(f"Scraping completed. Total tenders found: {total_found_tenders}")  # Log total found tenders

    except Exception as e:
        # Log any error encountered during scraping
        logging.error(f"An error occurred while scraping: {e}")

    finally:
        if db_connection is not None:
            db_connection.close()  # Ensure the database connection is closed

# Entry point of the script when executed directly
if __name__ == "__main__":
    scrape_tenders_from_websites()