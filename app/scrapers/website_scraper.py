from app.config import get_db_connection  # Import function to establish database connection
from app.scrapers.scraper import scrape_tenders  # Import the function used for scraping tenders
from datetime import datetime  # For handling date and time
from app.services.log import ScrapingLog  # Import your custom logging class
from app.scrapers.scraper_status import scraping_status  # Import the global scraping status

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

        ScrapingLog.add_log(f"Google queries: {google_queries}")

        total_found_tenders = 0
        total_relevant_tenders = 0
        total_irrelevant_tenders = 0
        total_open_tenders = 0
        total_closed_tenders = 0

        for query in google_queries:
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
        ScrapingLog.add_log(f"An error occurred while scraping: {e}")

    finally:
        if db_connection is not None:
            db_connection.close()  # Ensure the database connection is closed

# Entry point of the script when executed directly
if __name__ == "__main__":
    scrape_tenders_from_websites()