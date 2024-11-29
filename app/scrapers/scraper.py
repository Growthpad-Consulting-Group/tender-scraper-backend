from app.config import get_db_connection  # Import function to establish database connection
import logging  # For logging operation statuses and errors
import requests  # For making HTTP requests to scrape data
from bs4 import BeautifulSoup  # For parsing HTML content
from datetime import datetime  # For handling date and time
from app.db import insert_tender_to_db, get_keywords_and_terms  # Import database utilities
from app.routes.tenders.tender_utils import (
    extract_closing_dates,
    parse_closing_date,
    get_format,
    extract_pdf_text,
    extract_docx_text,
    construct_search_url,
    extract_description_from_response,
    is_relevant_tender
)
import random  # For generating random delays
import time  # For adding sleep delays between requests
import re  # For regular expression operations
from app.extensions import socketio  # Import your SocketIO instance here

# Configure logging settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of common user agents to simulate different browsers
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:58.0) Gecko/20100101 Firefox/58.0',
    'Mozilla/5.0 (Linux; Android 6.0; Nexus 6 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 10_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14E277 Safari/602.1',
]

# Mapping of supported search engines
SEARCH_ENGINES = [
    "Google",
    "Bing",
    "Yahoo",
    "DuckDuckGo",
    "Ask"
]

def scrape_tenders(db_connection, query, search_engines):
    """
    Scrapes tenders from specified search engines using a constructed query.

    Args:
        db_connection: The active database connection object.
        query (str): The constructed search query.
        search_engines (list): A list of selected search engines for scraping.

    Returns:
        list: A list of tender information dictionaries scraped from the web.
    """
    tenders = []  # Initialize a list to hold scraped tender data
    excluded_domains = [
        "microsoft.com", "go.microsoft.com", "privacy.microsoft.com",
        "support.microsoft.com", "about.ads.microsoft.com",
        "aka.ms", "yahoo.com", "search.yahoo.com",
        "duckduckgo.com", "ask.com", "bing.com",
    ]

    total_steps = len(search_engines)  # Count the total search engines for progress calculation

    # Loop through each specified search engine
    for i, engine in enumerate(search_engines):
        search_url = construct_search_url(engine, query)  # Construct the search URL
        logging.info(f"Constructed Search URL for engine '{engine}': {search_url}")

        # Set a random user agent from the list to simulate browser requests
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
        }

        # Add a random delay before sending the request to avoid rate limiting
        time.sleep(random.uniform(1, 3))

        try:
            # Make the HTTP GET request to the constructed search URL
            response = requests.get(search_url, headers=headers)
            response.raise_for_status()  # Raise an error for bad responses
            soup = BeautifulSoup(response.content, 'html.parser')  # Parse the response content with BeautifulSoup

            # Extract links from the search results
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                actual_url = extract_actual_link_from_search_result(href, engine)  # Get the actual URL from the search result

                # Skip internal pages and excluded domains
                if "google.com" in actual_url or any(domain in actual_url for domain in excluded_domains):
                    continue

                # Check if the extracted URL is valid before scraping
                if is_valid_url(actual_url):
                    logging.info(f"Visiting URL: {actual_url}")
                    tender_details = scrape_tender_details(actual_url, link.text.strip(), headers, db_connection)  # Pass the title
                    if tender_details:
                        tenders.append(tender_details)  # Add the tender details to the list

            # Emit progress update after processing an engine's results
            progress = ((i + 1) / total_steps) * 100
            logging.info(f'Emitting progress: {progress}%')
            socketio.emit('scraping_progress', {'progress': progress})  # Emit the progress

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"Error scraping {search_url}: {str(http_err)}")
            continue  # Continue on errors

    # Emit that scraping is complete
    logging.info(f"Scraping completed. Total tenders found: {len(tenders)}")
    socketio.emit('scraping_complete', {})
    return tenders  # Return the list of scraped tenders

def extract_actual_link_from_search_result(href, engine):
    """
    Extracts the actual link from the search engine results based on the search engine.

    Args:
        href (str): The href attribute from the search result anchor tag.
        engine (str): The name of the search engine.

    Returns:
        str: The extracted actual URL.
    """
    if engine in ['Google', 'Bing', 'Yahoo', 'DuckDuckGo', 'Ask']:
        match = re.search(r'q=(.+?)(&|$)', href)  # Find the query parameter in the URL
        if match:
            return match.group(1)  # Return the actual URL
    return href  # If not matched, return the href as is

def is_valid_url(url):
    """
    Check if the URL is valid and begins with http or https.

    Args:
        url (str): The URL to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    return re.match(r'https?://', url) is not None  # Validate the URL format

def scrape_tender_details(url, title, headers, db_connection):
    """
    Scrapes the actual tender details from the specific URL.

    Args:
        url (str): The URL from which to scrape tender details.
        title (str): The title of the tender extracted from the search results.
        headers (dict): HTTP headers for the request.
        db_connection: The active database connection object.

    Returns:
        dict or None: A dictionary containing tender info if successful, else None.
    """
    time.sleep(random.uniform(1, 3))  # Random delay to mimic human browsing
    try:
        response = requests.get(url, headers=headers)  # Fetch the tender page
        response.raise_for_status()  # Check for HTTP errors

        # Extract the content format
        format_type = get_format(url)  # Determine content format (PDF, DOCX, HTML)

        # Initialize text variable
        extracted_text = ""

        # Check the content format for text extraction
        if format_type == 'PDF':
            extracted_text = extract_pdf_text(response.content)
        elif format_type == 'DOCX':
            extracted_text = extract_docx_text(response.content)
        else:  # Assume HTML
            extracted_text = BeautifulSoup(response.content, 'html.parser').get_text()

        closing_dates = extract_closing_dates(extracted_text, db_connection)  # Now pass the extracted text

        # Initialize a variable to track the relevant keyword
        filtered_keyword = None
        is_relevant = "No"  # Default value for relevance

        if closing_dates:  # If closing dates were found
            for date, keyword in closing_dates:  # Process each closing date found
                try:
                    closing_date_parsed = parse_closing_date(date)  # Parse the date string

                    # Now check for relevance using the extracted text
                    filtered_keyword = is_relevant_tender(extracted_text, db_connection)

                    # Determining relevance
                    if filtered_keyword:
                        is_relevant = "Yes"

                    # Create a dictionary to hold tender information
                    tender_info = {
                        'title': title,
                        'description': extract_description_from_response(response, format_type),
                        'closing_date': closing_date_parsed,
                        'source_url': url,
                        'status': "open" if closing_date_parsed > datetime.now().date() else "closed",
                        'format': format_type,
                        'scraped_at': datetime.now().date(),
                        'tender_type': 'Uploaded Websites'
                    }

                    logging.info("====================================")
                    logging.info("Found Tender")
                    logging.info(f"Tender Title: {tender_info['title']}")
                    logging.info(f"Closing Date: {closing_date_parsed}")
                    logging.info(f"Closing Date Keyword Found: {keyword}")
                    logging.info(f"Status: {tender_info['status']}")
                    logging.info(f"Tender Type: {tender_info['tender_type']}")
                    logging.info(f"Filtered Based on: {filtered_keyword if filtered_keyword else 'None'}")
                    logging.info(f"Relevant Tender: {is_relevant}")
                    logging.info("====================================")

                    # If the tender is relevant, attempt to insert it into the database
                    if is_relevant == "Yes":
                        insertion_status = insert_tender_to_db(tender_info, db_connection)
                        if insertion_status:
                            logging.info("Inserted into database: Success")
                        else:
                            logging.error("Inserting into database: Failed")

                    return tender_info  # Return the scraped tender info
                except ValueError as ve:
                    logging.error(f"Error parsing date for tender from '{url}': {str(ve)}")
    except Exception as e:
        logging.error(f"Could not scrape the tender details from {url}: {str(e)}")
    return None  # Return None if scraping failed



def extract_closing_dates_from_content(response, format_type, db_connection):
    """
    Extracts closing dates based on the content format of the response.

    Args:
        response: The HTTP response object containing content.
        format_type (str): The format type of the content ('PDF', 'DOCX', 'HTML').
        db_connection: The active database connection object.

    Returns:
        list: A list of closing dates and associated keywords extracted from the content.
    """
    if format_type == 'PDF':
        pdf_text = extract_pdf_text(response.content)  # Extract text from the PDF
        return extract_closing_dates(pdf_text, db_connection)  # Pass db_connection to extract closing dates
    elif format_type == 'DOCX':
        docx_text = extract_docx_text(response.content)  # Extract text from the DOCX
        return extract_closing_dates(docx_text, db_connection)  # Pass db_connection to extract closing dates
    else:  # Assuming it's HTML
        soup = BeautifulSoup(response.content, 'html.parser')  # Parse HTML
        page_text = soup.get_text()  # Get all text content from the page
        return extract_closing_dates(page_text, db_connection)  # Pass db_connection to extract closing dates


# The script's entry point when executed directly
if __name__ == "__main__":
    db_connection = get_db_connection()  # Establish the database connection
    try:
        # Construct a search query using keywords and the current year
        query = construct_search_query("tender", get_keywords_and_terms(), datetime.now().year)
        # Call the scraping function to fetch tenders
        scraped_tenders = scrape_tenders(db_connection, query, SEARCH_ENGINES)
        logging.info(f"Scraped tenders: {scraped_tenders}")  # Log the scraped tenders
    finally:
        # Ensure the database connection is closed properly
        if db_connection:
            db_connection.close()  # Close the database connection
            logging.info("Database connection closed.")