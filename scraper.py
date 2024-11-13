from config import get_db_connection
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from db import insert_tender_to_db, get_keywords_and_terms
from utils import (
    extract_closing_dates,
    parse_closing_date,
    is_valid_url,
    get_format,
    extract_pdf_text,
    extract_docx_text,
    construct_search_url,
    extract_description_from_response  # Add this line

)
import random
import time

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
    current_year = datetime.now().year

    for engine in SEARCH_ENGINES:
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
            links = soup.find_all('a', href=True)

            for link in links:
                process_link(link, response, headers, current_year, db_connection, tenders)

            if len(tenders) > 0:  # Stop trying other engines if we found tenders
                break

        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 429:
                logging.warning(f"Received 429 from {engine}, switching to next search engine.")
                continue  # Move to next engine
            else:
                logging.error(f"Error scraping {search_url}: {str(http_err)}")
                break  # Break on other HTTP errors

    return tenders

def construct_search_query(keyword, search_terms, current_year):
    """Constructs a search query string."""
    terms_query = " OR ".join([term.strip() for term in search_terms if term.strip()])
    return f"{keyword} {terms_query} {current_year}" if terms_query else f"{keyword} {current_year}"

def process_link(link, response, headers, current_year, db_connection, tenders):
    """Processes each link found in the search result."""
    href = link['href']

    # Extract just the visible text of the link as the title
    title = link.get_text(strip=True)

    base_url = "https://www.google.com"
    valid_url = is_valid_url(href, base_url)

    if valid_url is None:
        logging.debug(f"Link skipped (invalid): {href}")
        return

    # Add delay before accessing the valid URL
    time.sleep(random.uniform(1, 3))

    try:
        response = requests.get(valid_url, headers=headers, allow_redirects=True)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.warning(f"Failed to access URL: {valid_url}. Error: {e}")
        return

    format_type = get_format(valid_url)
    closing_dates = extract_closing_dates_from_content(response, format_type)

    if closing_dates:
        for date, keyword in closing_dates:
            try:
                closing_date_parsed = parse_closing_date(date)
                tender_info = {
                    'title': title,  # Extracted title should only be the visible text
                    'description': extract_description_from_response(response, format_type),
                    'closing_date': closing_date_parsed,
                    'source_url': valid_url,
                    'status': "open" if closing_date_parsed > datetime.now().date() else "closed",
                    'format': format_type,
                    'scraped_at': datetime.now().date(),
                    # Tender type defined here
                    'tender_type': 'Uploaded Websites'
                }

                logging.info("====================================")
                logging.info("Found Tender")
                logging.info(f"Tender Title: {tender_info['title']}")
                logging.info(f"Closing Date: {closing_date_parsed}")
                logging.info(f"Closing Date Keyword Found: {keyword}")
                logging.info(f"Status: {tender_info['status']}")
                logging.info(f"Tender Type: {tender_info['tender_type']}")  # Log Type of Tender
                logging.info("====================================")

                insertion_status = insert_tender_to_db(tender_info, db_connection)

                if insertion_status:
                    logging.info("==========")
                    logging.info("Inserting into database: Success")
                    logging.info("==========")
                else:
                    logging.error("==========")
                    logging.error("Inserting into database: Failed")
                    logging.error("==========")

                tenders.append(tender_info)
            except ValueError as ve:
                logging.error(f"Error parsing date for '{title}': {str(ve)}")
    else:
        logging.debug(f"No closing date found for: {title}. Skipping.")



def extract_closing_dates_from_content(response, format_type):
    """Extracts closing dates based on the content format."""
    if format_type == 'PDF':
        pdf_text = extract_pdf_text(response.content)
        return extract_closing_dates(pdf_text)
    elif format_type == 'DOCX':
        docx_text = extract_docx_text(response.content)
        return extract_closing_dates(docx_text)
    else:
        soup = BeautifulSoup(response.content, 'html.parser')
        page_text = soup.get_text()
        return extract_closing_dates(page_text)

if __name__ == "__main__":
    db_connection = get_db_connection()
    query = construct_search_query("tender", get_keywords_and_terms(), datetime.now().year)
    scraped_tenders = scrape_tenders(db_connection, query, search_engines)
    logging.info(f"Scraped tenders: {scraped_tenders}")

