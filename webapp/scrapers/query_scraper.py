from webapp.config import get_db_connection  # Import function to establish database connection
from webapp.scrapers.run_query_scraper import scrape_tenders  # Import the function used for scraping tenders
from datetime import datetime  # For handling date and time
from webapp.services.log import ScrapingLog  # Import your custom logging class
from webapp.scrapers.scraper_status import scraping_status  # Import the global scraping status


def fetch_terms(db_connection):
    """
    Retrieves search terms from the database.

    Args:
        db_connection: The active database connection object.

    Returns:
        tuple: A tuple containing a list of search terms.
    """
    try:
        with db_connection.cursor() as cur:
            # Execute query to fetch all search terms from the 'search_terms' table
            cur.execute("SELECT term FROM search_terms")
            search_terms = [row[0] for row in cur.fetchall() if row]  # Filter out any None rows

        # ScrapingLog.add_log(f"Fetched Search Terms: {search_terms}")  # Log to ScrapingLog
        return search_terms  # Return a tuple of search terms

    except Exception as e:
        ScrapingLog.add_log(f"Error fetching search terms: {e}")  # Log the error
        return []  # Return empty lists in case of error

def scrape_tenders_from_query(selected_engines=None, time_frame=None, file_type=None, terms=None, region=None):
    """
    Scrapes tenders from search engine results using search terms and stores results in the database.

    Args:
        selected_engines (list, optional): List of search engines to use; defaults to None.
        time_frame (str, optional): Time frame for scraping; defaults to None.
        file_type (str, optional): Type of files to consider; defaults to None.
        terms (list, optional): List of search terms to use for scraping; defaults to None.
        region (str, optional): The geographical region for which to scrape tenders; defaults to None.
    """
    db_connection = None
    global scraping_status  # Access the global variable to update scraping status

    try:
            # Fetch terms passed directly
        if terms is None or not terms:
            terms = []  # Handles any null/empty lists
        # terms = terms if terms is not None else fetch_terms(db_connection)  # Try to fetch terms if none passed.

        if not terms:
            ScrapingLog.add_log("Error: No search terms provided.")
            return  # Exit early if no terms to search with

        # Establish database connection
        db_connection = get_db_connection()

        # Fetch search terms from the database
        search_terms = fetch_terms(db_connection)

        if not search_terms:  # Check if search terms are not empty
            ScrapingLog.add_log("No valid search terms found. Aborting scraping.")
            return

        # Get the current year for the query
        current_year = datetime.now().year

        # Construct queries for scraping with search terms and region.
        google_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +  # Add quotes around each term
            (f" {current_year}" if time_frame == 'y' else '') +  # Add the year only if the past year is selected
            (f"&as_qdr={time_frame}" if time_frame != 'anytime' else '') +  # Apply qdr filter only if time_frame is not 'anytime'
            f"&as_eq=&as_nlo=&as_nhi=&lr=&" +  # Various empty filters (as_eq = exact match, etc.)
            (f"cr=country{region}&" if region and region != 'any' else '') +  # Country filter (e.g., countryKE)
            (f"as_filetype={file_type}&" if file_type and file_type != 'any' else "") +  # Add filetype filter if not 'any'
            f"as_occt=any&" +  # Correct placement of any file type filter if added
            f"tbs="  # Optional for additional filters; not necessary in this case
        ]

            # Construct queries for Bing
        bing_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&qft=+filterui:date:y" if time_frame == 'y' else '') +
            (f"&filter=all" if file_type and file_type != 'any' else '') +  # Add appropriate file type filter
            (f"site:{region} " if region and region != 'any' else '')  # Region-based filter if needed
        ]

        # Construct queries for Yahoo (similar to Bing)
        yahoo_queries = bing_queries  # For simplicity, assuming Yahoo uses similar queries as Bing

        # Construct queries for DuckDuckGo
        duckduckgo_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&t=hg" if file_type and file_type != 'any' else '') +  # Potential file type filtering
            (f"site:{region} " if region and region != 'any' else '')  # Use site filter for regional search
        ]

        # Construct queries for Ask.com
        ask_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&filetype={file_type}" if file_type and file_type != 'any' else '') +  # File type
            (f"site:{region}" if region and region != 'any' else '')  # Regional search
        ]


    # Initialize lists for storing all scraped tenders and counting found tenders
        all_tenders = []
        total_found_tenders = 0
        ScrapingLog.clear_logs()  # Clear logs before starting

        ScrapingLog.add_log("Starting the scraping process.")

        # Select the appropriate queries based on selected engines
        all_queries = []
        if 'Google' in selected_engines:
            all_queries.extend(google_queries)
        if 'Bing' in selected_engines:
            all_queries.extend(bing_queries)
        if 'Yahoo' in selected_engines:
            all_queries.extend(yahoo_queries)
        if 'DuckDuckGo' in selected_engines:
            all_queries.extend(duckduckgo_queries)
        if 'Ask' in selected_engines:
            all_queries.extend(ask_queries)    

        # Choose queries based on selected engines
        for query in all_queries:
            ScrapingLog.add_log(f"Scraping for query: {query}")  # Log the current query being scraped
            # Call the scraping function and collect the returned tenders
            scraped_tenders = scrape_tenders(db_connection, query, selected_engines)

            # Count the total number of tenders found after scraping each query
            if scraped_tenders is not None:  # Check for None response
                total_found_tenders += len(scraped_tenders)
                all_tenders.extend(scraped_tenders)

        ScrapingLog.add_log(f"Scraping completed. Total tenders found: {total_found_tenders}")  # Log to ScrapingLog

        # Update scraping status to complete
        scraping_status['complete'] = True

    except Exception as e:
        ScrapingLog.add_log(f"An error occurred while scraping: {e}")  # Log the error

    finally:
        if db_connection is not None:
            db_connection.close()  # Ensure the database connection is closed
            ScrapingLog.add_log("Database connection closed.")  # Log closing database connection

# Entry point of the script when executed directly
if __name__ == "__main__":
    scrape_tenders_from_query()
