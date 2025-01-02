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
            terms = []

        if not terms:
            ScrapingLog.add_log("Error: No search terms provided.")
            return

        db_connection = get_db_connection()
        search_terms = fetch_terms(db_connection)

        if not search_terms:
            ScrapingLog.add_log("No valid search terms found. Aborting scraping.")
            return

        # Get the current year for the query
        current_year = datetime.now().year

        # Construct queries for scraping with search terms and region
        google_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&as_qdr={time_frame}" if time_frame != 'anytime' else '') +
            f"&as_eq=&as_nlo=&as_nhi=&lr=&" +
            (f"cr=country{region}&" if region and region != 'any' else '') +
            (f"as_filetype={file_type}&" if file_type and file_type != 'any' else "") +
            "as_occt=any&"
        ]

        # Construct queries for Bing
        bing_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&qft=+filterui:date:y" if time_frame == 'y' else '') +
            (f"&filter=all" if file_type and file_type != 'any' else '') +
            (f" site:{region}" if region and region != 'any' else '')  # Add space before site:{region}
        ]


        # Construct queries for Yahoo (similar to Bing)
        yahoo_queries = bing_queries

        # Construct queries for DuckDuckGo
        duckduckgo_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&t=hg" if file_type and file_type != 'any' else '') +
            (f"site:{region} " if region and region != 'any' else '')
        ]

        # Construct queries for Ask.com
        ask_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&filetype={file_type}" if file_type and file_type != 'any' else '') +
            (f"site:{region}" if region and region != 'any' else '')
        ]

        # Initialize counters and logs
        all_tenders = []
        total_found_tenders = 0
        total_relevant_tenders = 0
        total_irrelevant_tenders = 0
        total_open_tenders = 0
        total_closed_tenders = 0
        ScrapingLog.clear_logs()

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

            # Perform scraping for each query
        for query in all_queries:
            ScrapingLog.add_log(f"Scraping for query: {query}")
            try:
                scraped_tenders = scrape_tenders(db_connection, query, selected_engines)

                if scraped_tenders is not None:
                    total_found_tenders += len(scraped_tenders)
                    for tender in scraped_tenders:
                        scraping_status['tenders'].append(tender)
                        is_relevant = tender.get('is_relevant', 'No')
                        status = tender.get('status', 'unknown')

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

        # Log results after processing all queries
        ScrapingLog.add_log(f"Scraping completed. Total tenders found: {total_found_tenders}, "
                            f"Relevant: {total_relevant_tenders}, "
                            f"Irrelevant: {total_irrelevant_tenders}, "
                            f"Open: {total_open_tenders}, "
                            f"Closed: {total_closed_tenders}")

        # Update scraping status
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
            db_connection.close()
            ScrapingLog.add_log("Database connection closed.")

# Execution entry point
if __name__ == "__main__":
    scrape_tenders_from_query()
