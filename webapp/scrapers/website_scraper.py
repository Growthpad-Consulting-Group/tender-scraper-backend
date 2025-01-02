from webapp.config import get_db_connection  # Import function to establish database connection
from webapp.scrapers.scraper import scrape_tenders  # Import the function used for scraping tenders
from datetime import datetime  # For handling date and time
from webapp.services.log import ScrapingLog  # Import your custom logging class
from webapp.scrapers.scraper_status import scraping_status  # Import the global scraping status
import logging

def fetch_urls_and_terms(db_connection):
    """
    Retrieves URLs and search terms from the database.

    Args:
        db_connection: The active database connection object.

    Returns:
        tuple: A tuple containing a list of URLs and a list of search terms.
    """
    try:
        # Use 'with' to ensure that the cursor is closed properly after use
        with db_connection.cursor() as cur:
            # Execute query to fetch all URLs from the 'websites' table
            cur.execute("SELECT url FROM websites")
            urls = [row[0] for row in cur.fetchall()]  # Retrieve URLs

            # Execute query to fetch all search terms from the 'search_terms' table
            cur.execute("SELECT term FROM search_terms")
            search_terms = [row[0] for row in cur.fetchall()]  # Retrieve search terms

            # Logging the fetched results
            # ScrapingLog.add_log(f"Fetched URLs: {urls}")
            # ScrapingLog.add_log(f"Fetched Search Terms: {search_terms}")
            return urls, search_terms  # Return the results as a tuple

    except Exception as e:
        # Log any error encountered
        ScrapingLog.add_log(f"Error in fetch_urls_and_terms: {e}")
        return [], []  # Return empty lists in case of error



def scrape_tenders_from_websites(selected_engines=None, time_frame=None, file_type=None, terms=None, website=None):
    """
    Scrapes tenders from specified websites using search terms and stores results in the database.
    """
    db_connection = None
    global scraping_status  # Access the global variable to update scraping status
    scraping_status['tenders'] = []

    try:
        terms = terms or []

        # Log the received search terms for debugging
        logging.info(f"Scraping function called with terms: {terms}")


        if not terms:
            ScrapingLog.add_log("Error: No search terms provided.")
            return  # Exit early if no terms to search with

        db_connection = get_db_connection()

        urls, search_terms = fetch_urls_and_terms(db_connection)

        if website:
            urls = [website]  # If a specific website is provided, only scrape that one

        if not urls:
            ScrapingLog.add_log("Error: No URLs fetched from the database.")
            return  # Exit early if no URLs are fetched

        current_year = datetime.now().year

        google_queries = [
            f'site:{url.split("//")[1].rstrip("/")} ' +
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&as_qdr={time_frame}" if time_frame != 'anytime' else '') +
            f"&as_eq=&as_nlo=&as_nhi=&lr=&" +
            (f"as_filetype={file_type}&" if file_type and file_type != 'any' else "") +
            f"as_occt=any&" +
            f"tbs="
            for url in urls
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

        total_found_tenders = 0
        total_relevant_tenders = 0
        total_irrelevant_tenders = 0
        total_open_tenders = 0
        total_closed_tenders = 0

        for query in all_queries:
            ScrapingLog.add_log(f"Scraping for query: {query}")

            try:
                scraped_tenders = scrape_tenders(db_connection, query, selected_engines)

                total_found_tenders += len(scraped_tenders)

                for tender in scraped_tenders:
                    scraping_status['tenders'].append(tender)
                    # Assuming each tender is a dictionary returned from scrape_tender_details
                    is_relevant = tender.get('is_relevant', 'No')  # Default to 'No'
                    status = tender.get('status', 'unknown')  # Default to 'unknown'

                    if is_relevant == "Yes":
                        total_relevant_tenders += 1
                    else:
                        total_irrelevant_tenders += 1

                    if status == "open":
                        total_open_tenders += 1
                    elif status == "closed":
                        total_closed_tenders += 1

            except Exception as e:
                ScrapingLog.add_log(f"Error scraping for query {query}: {e}")

                # Log counts after processing all queries
                ScrapingLog.add_log(f"Scraping completed. Total tenders found: {total_found_tenders}, "
                                    f"Relevant: {total_relevant_tenders}, "
                                    f"Irrelevant: {total_irrelevant_tenders}, "
                                    f"Open: {total_open_tenders}, "
                                    f"Closed: {total_closed_tenders}")

        scraping_status.update({
            'complete': True,
            'total_found': total_found_tenders,
            'relevant_count': total_relevant_tenders,
            'irrelevant_count': total_irrelevant_tenders,
            'open_count': total_open_tenders,
            'closed_count': total_closed_tenders
        })

    except Exception as e:
        ScrapingLog.add_log(f"An error occurred while scraping: {e}")  # Log the error

    finally:
        if db_connection is not None:
            db_connection.close()  # Ensure the database connection is closed
            ScrapingLog.add_log("Database connection closed.")  # Log closing database connection

# Entry point of the script when executed directly
if __name__ == "__main__":
    scrape_tenders_from_websites()