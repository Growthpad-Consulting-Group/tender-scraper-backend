from app.config import get_db_connection  # Import function to establish database connection
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
from urllib.parse import urlparse  # Import this to parse URLs
import urllib.parse  # Import this to parse URLs
import random  # For generating random delays
import time  # For adding sleep delays between requests
import re  # For regular expression operations
from app.extensions import socketio  # Import your SocketIO instance here
from app.services.log import ScrapingLog  # Import your custom logging class

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
        "duckduckgo.com", "ask.com", "bing.com", "youtube.com",
        "investopedia.com"
    ]

    total_steps = len(search_engines)  # Count the total search engines for progress calculation

    ScrapingLog.add_log("Starting tender scraping...")

    # Loop through each specified search engine
    for i, engine in enumerate(search_engines):
        search_url = construct_search_url(engine, query)  # Construct the search URL
        ScrapingLog.add_log(f"Constructed Search URL for engine '{engine}': {search_url}")

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
                    ScrapingLog.add_log(f"Visiting URL: {actual_url}")  # Log to ScrapingLog
                tender_details = scrape_tender_details(actual_url, link.text.strip(), headers, db_connection)  # Pass the title
                if tender_details:
                    tenders.append(tender_details)  # Add the tender details to the list

            # Emit progress update after processing an engine's results
            progress = ((i + 1) / total_steps) * 100
            ScrapingLog.add_log(f'Emitting progress: {progress}%')
            socketio.emit('scraping_progress', {'progress': progress})  # Emit the progress

        except requests.exceptions.HTTPError as http_err:
            ScrapingLog.add_log(f"Error scraping {search_url}: {str(http_err)}")
            continue  # Continue on errors

    # Emit that scraping is complete
    ScrapingLog.add_log(f"Scraping completed. Total tenders found: {len(tenders)}")
    socketio.emit('scraping_complete', { 'total_tenders': len(tenders) })
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
        # Handle relative URLs
        if href.startswith('/'):
            base_url = {
                "Google": "https://www.google.com",
                "Bing": "https://www.bing.com",
                "Yahoo": "https://search.yahoo.com",
                "DuckDuckGo": "https://duckduckgo.com",
                "Ask": "https://www.ask.com"
            }[engine]
            return urllib.parse.urljoin(base_url, href)

        # Decode the URL in the search result
        match = re.search(r'q=([^&]+)', href)  # Get everything after 'q='
        if match:
            return urllib.parse.unquote(match.group(1))  # Decode the URL

    return href  # Fallback to returning original href if no match




def is_valid_url(url):
    """
    Check if the URL is valid and begins with http or https.

    Args:
        url (str): The URL to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    # Check if the URL is valid and starts with 'http://' or 'https://'
    return url.startswith('http://') or url.startswith('https://')


def log_scraping_details(db_connection, website_name, visiting_url, tenders_found,
                         tender_title, closing_date, closing_keyword,
                         filtered_keyword, is_relevant, status):
    """
    Logs the scraping details into the database, updating existing entries based on visiting_url.
    """
    try:
        with db_connection.cursor() as cursor:
            # Check if the visiting_url already exists in the table
            check_query = """
            SELECT id FROM scraping_log WHERE visiting_url = %s;
            """
            cursor.execute(check_query, (visiting_url,))
            existing_record = cursor.fetchone()  # Fetch the existing record if it exists

            if existing_record:
                # If a record exists, update it
                update_query = """
                UPDATE scraping_log
                SET website_name = %s, tenders_found = %s, tender_title = %s, closing_date = %s,
                    closing_keyword = %s, filtered_keyword = %s, relevant = %s, status = %s, created_at = NOW()
                WHERE visiting_url = %s;
                """
                cursor.execute(update_query, (website_name, tenders_found, tender_title, closing_date,
                                              closing_keyword, filtered_keyword, is_relevant, status, visiting_url))
                ScrapingLog.add_log(f"Updated existing record for visiting_url: {visiting_url}")
            else:
                # If no record exists, insert a new one
                insert_query = """
                INSERT INTO scraping_log (website_name, visiting_url, tenders_found, tender_title, closing_date,
                                          closing_keyword, filtered_keyword, relevant, status, created_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
                """
                cursor.execute(insert_query, (website_name, visiting_url, tenders_found, tender_title, closing_date,
                                              closing_keyword, filtered_keyword, is_relevant, status))
                ScrapingLog.add_log(f"Inserted new record for visiting_url: {visiting_url}")

            db_connection.commit()
            ScrapingLog.add_log("Log entry successfully inserted/updated in scraping_log table.")

    except Exception as e:
        ScrapingLog.add_log(f"Error in logging tender details: {str(e)}")
        if db_connection:
            db_connection.rollback()  # Roll back if an error occurs


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
    description = ""  # Initialize description to ensure it's defined

    # Extract the base domain name for logging (e.g., chrips.or.ke)
    parsed_url = urlparse(url)
    website_name = f"{parsed_url.scheme}://{parsed_url.netloc}"

    # Default value for tender title
    tender_title = title

    ScrapingLog.add_log(f"Visiting URL: {url}")

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        format_type = get_format(url)
        extracted_text = ""
        closing_dates = []

        # Fetching and processing closing dates based on format
        if format_type == 'PDF':
            extracted_text = extract_pdf_text(response.content)
            closing_dates = extract_closing_dates(extracted_text, db_connection)
        elif format_type == 'DOCX':
            extracted_text = extract_docx_text(response.content)
            closing_dates = extract_closing_dates(extracted_text, db_connection)
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            tender_title = (soup.find('h1') or soup.find('h2') or title).text.strip() if (soup.find('h1') or soup.find('h2')) else title
            description = " ".join(p.text.strip() for p in soup.find_all('p')[:2]) if soup.find_all('p') else ""
            extracted_text = f"{tender_title} {description}"
            closing_dates = extract_closing_dates(extracted_text, db_connection)

        filtered_keyword = None
        is_relevant = "No"
        log_tenders_found = 0

        if closing_dates:
            for date, keyword in closing_dates:
                ScrapingLog.add_log(f"Closing date for URL '{url}': {date}")
                try:
                    closing_date_parsed = parse_closing_date(date)
                    filtered_keyword = is_relevant_tender(extracted_text, db_connection)

                    if filtered_keyword:
                        is_relevant = "Yes"

                    tender_info = {
                        'title': tender_title,
                        'description': description,
                        'closing_date': closing_date_parsed,
                        'source_url': url,
                        'status': "open" if closing_date_parsed > datetime.now().date() else "closed",
                        'format': format_type,
                        'scraped_at': datetime.now().date(),
                        'tender_type': 'Query Tenders'
                    }

                    ScrapingLog.add_log("====================================")
                    ScrapingLog.add_log("Found Tender")
                    ScrapingLog.add_log(f"Tender Title: {tender_info['title']}")
                    ScrapingLog.add_log(f"Closing Date: {closing_date_parsed}")
                    ScrapingLog.add_log(f"Closing Date Keyword Found: {keyword}")
                    ScrapingLog.add_log(f"Status: {tender_info['status']}")
                    ScrapingLog.add_log(f"Tender Type: {tender_info['tender_type']}")
                    ScrapingLog.add_log(f"Filtered Based on: {filtered_keyword}")
                    ScrapingLog.add_log(f"Relevant Tender: {is_relevant}")
                    ScrapingLog.add_log("====================================")

                    log_tenders_found += 1

                    # Prepare log details before logging
                    log_status = tender_info['status']
                    log_closing_keyword = keyword if keyword else 'None'
                    log_filtered_keyword = filtered_keyword if filtered_keyword else 'None'
                    log_is_relevant = is_relevant

                    # Log the scraping details
                    try:
                        log_scraping_details(db_connection, website_name, url, log_tenders_found,
                                             tender_info['title'], closing_date_parsed,
                                             log_closing_keyword, log_filtered_keyword,
                                             log_is_relevant, log_status)
                    except Exception as log_error:
                        ScrapingLog.add_log(f"Error occurred while logging details: {log_error}")

                    # Insert the tender into the database only if relevant
                    if is_relevant == "Yes":
                        insertion_status = insert_tender_to_db(tender_info, db_connection)
                        if insertion_status:
                            ScrapingLog.add_log(f"Inserted into database: Success - {tender_info['title']}")
                        else:
                            ScrapingLog.add_log(f"Inserting into database: Failed - {tender_info['title']}")

                except Exception as ve:
                    ScrapingLog.add_log(f"Error processing closing date for tender from '{url}': {str(ve)}")

        else:
            ScrapingLog.add_log(f"No closing dates found for URL: {url}")

    except requests.exceptions.HTTPError as http_err:
        ScrapingLog.add_log(f"HTTP error while fetching `{url}`: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        ScrapingLog.add_log(f"Connection error while trying to reach `{url}`: {conn_err}")
    except Exception as e:
        ScrapingLog.add_log(f"Error scraping details from `{url}`: {str(e)}")

    return None

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
        ScrapingLog.add_log(f"Scraped tenders: {scraped_tenders}")  # Log the scraped tenders
    finally:
        # Ensure the database connection is closed properly
        if db_connection:
            db_connection.close()  # Close the database connection
            ScrapingLog.add_log("Database connection closed.")
