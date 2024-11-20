from app.config import get_db_connection
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from app.db import insert_tender_to_db, get_keywords_and_terms
from app.routes.tenders.tender_utils import (
    extract_closing_dates,
    parse_closing_date,
    get_format,
    extract_pdf_text,
    extract_docx_text,
    construct_search_url,
    extract_description_from_response
)
import random
import time
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# List of common user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:58.0) Gecko/20100101 Firefox/58.0',
    'Mozilla/5.0 (Linux; Android 6.0; Nexus 6 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 10_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14E277 Safari/602.1',
]

# Mapping of search engines
SEARCH_ENGINES = [
    "Google",
    "Bing",
    "Yahoo",
    "DuckDuckGo",
    "Ask"
]

def scrape_tenders(db_connection, query, search_engines):
    """Scrapes tenders from specified search engines using keywords from the database."""
    tenders = []

    for engine in search_engines:
        search_url = construct_search_url(engine, query)
        logging.info(f"Constructed Search URL for engine '{engine}': {search_url}")

        headers = {
            "User-Agent": random.choice(USER_AGENTS),
        }

        # Add delay before sending the request
        time.sleep(random.uniform(1, 3))

        try:
            response = requests.get(search_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract links based on the search engine
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']
                actual_url = extract_actual_link_from_search_result(href, engine)

                # Skip Google internal pages
                if "google.com" in actual_url:
                    logging.info(f"Ignored Google internal page: {actual_url}")
                    continue

                # Check if the extracted URL is valid before scraping
                if is_valid_url(actual_url):
                    logging.info(f"Visiting URL: {actual_url}")  # Log the URL being visited
                    tender_details = scrape_tender_details(actual_url, headers, db_connection)
                    if tender_details:
                        tenders.append(tender_details)
                else:
                    logging.error(f"Ignored invalid URL: {actual_url}")  # Log ignored invalid URLs

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 429:
                logging.warning(f"Received 429 from {engine}, switching to next search engine.")
                continue  # Move to next engine
            else:
                logging.error(f"Error scraping {search_url}: {str(http_err)}")
                break  # Break on other HTTP errors

    return tenders

def extract_actual_link_from_search_result(href, engine):
    """Extracts the actual tender link from the search engine result."""
    if engine in ['Google', 'Bing', 'Yahoo', 'DuckDuckGo', 'Ask']:  # Logic for extracting from common search engines
        match = re.search(r'q=(.+?)(&|$)', href)
        if match:
            return match.group(1)  # Actual URL
    return href  # If not matched, return the href as is

def is_valid_url(url):
    """Check if the URL is valid and begins with http or https."""
    return re.match(r'https?://', url) is not None

def scrape_tender_details(url, headers, db_connection):
    """Scrape the actual tender details from the specific URL."""
    time.sleep(random.uniform(1, 3))
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        format_type = get_format(url)
        closing_dates = extract_closing_dates_from_content(response, format_type)

        if closing_dates:
            # Extract title from the response content
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.title.string if soup.title else url  # Use page title or URL if not found
            for date, keyword in closing_dates:
                try:
                    closing_date_parsed = parse_closing_date(date)
                    tender_info = {
                        'title': title,  # Use the extracted title
                        'description': extract_description_from_response(response, format_type),
                        'closing_date': closing_date_parsed,
                        'source_url': url,
                        'status': "open" if closing_date_parsed > datetime.now().date() else "closed",
                        'format': format_type,
                        'scraped_at': datetime.now().date(),
                        'tender_type': 'Uploaded Websites'  # Modify this if you have specific type logic
                    }

                    logging.info("====================================")
                    logging.info("Found Tender")
                    logging.info(f"Tender Title: {tender_info['title']}")
                    logging.info(f"Closing Date: {closing_date_parsed}")
                    logging.info(f"Closing Date Keyword Found: {keyword}")
                    logging.info(f"Status: {tender_info['status']}")
                    logging.info(f"Tender Type: {tender_info['tender_type']}")
                    logging.info("====================================")

                    try:
                        # Attempt to insert the tender into the database
                        insertion_status = insert_tender_to_db(tender_info, db_connection)
                        if insertion_status:
                            logging.info("==========")
                            logging.info("Inserting into database: Success")
                            logging.info("==========")
                        else:
                            logging.error("==========")
                            logging.error("Inserting into database: Failed")
                            logging.error("==========")

                    except Exception as e:
                        logging.error(f"Error inserting tender into database: {str(e)}")

                    return tender_info
                except ValueError as ve:
                    logging.error(f"Error parsing date for tender from '{url}': {str(ve)}")
    except Exception as e:
        logging.error(f"Could not scrape the tender details from {url}: {str(e)}")
    return None

def extract_closing_dates_from_content(response, format_type):
    """Extracts closing dates based on the content format."""
    if format_type == 'PDF':
        pdf_text = extract_pdf_text(response.content)
        return extract_closing_dates(pdf_text)
    elif format_type == 'DOCX':
        docx_text = extract_docx_text(response.content)
        return extract_closing_dates(docx_text)
    else:  # Assuming it's HTML
        soup = BeautifulSoup(response.content, 'html.parser')
        page_text = soup.get_text()
        return extract_closing_dates(page_text)

if __name__ == "__main__":
    db_connection = get_db_connection()
    try:
        query = construct_search_query("tender", get_keywords_and_terms(), datetime.now().year)
        scraped_tenders = scrape_tenders(db_connection, query, SEARCH_ENGINES)
        logging.info(f"Scraped tenders: {scraped_tenders}")
    finally:
        # Ensure the database connection is closed properly
        if db_connection:
            db_connection.close()
            logging.info("Database connection closed.")