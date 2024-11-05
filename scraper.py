from config import get_db_connection
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from db import insert_tender_to_db, get_keywords_and_terms  # Updated import
from utils import (
    extract_closing_dates,
    parse_closing_date,
    is_valid_url,
    get_format,
    extract_pdf_text,
    extract_docx_text,
    construct_search_url
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_tenders(search_engines, db_connection):
    """Scrapes tenders from the specified search engines using keywords from the database."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }

    tenders = []
    current_year = datetime.now().year

    # Get keywords and their terms from the database
    keywords = get_keywords_and_terms(db_connection)

    for engine in search_engines:
        for keyword_info in keywords:
            # Get the keyword and associated search terms
            keyword = keyword_info['keyword']
            search_terms = keyword_info['terms']

            # Log the keyword and terms for debugging
            # logging.info(f"Keyword: '{keyword}', Search Terms: {search_terms}")

            # Create a search query with the format "keyword term1 OR term2 OR term3 year"
            if search_terms:  # Only join if there are terms
                terms_query = " OR ".join([term.strip() for term in search_terms if term.strip()])
                search_query = f"{keyword} {terms_query} {current_year}"
            else:
                search_query = f"{keyword} {current_year}"  # Fallback if no terms

            # Log the constructed search query
            logging.info(f"Constructed search query: {search_query}")

            # Construct the search URL
            search_url = construct_search_url(engine, search_query)
            logging.info(f"Performing search with query: {search_url}")

            try:
                response = requests.get(search_url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.find_all('a', href=True)

                for link in links:
                    href = link['href']
                    title = link.get_text(strip=True)

                    valid_url = is_valid_url(href, search_url)
                    if not valid_url or 'google.com' in valid_url or 'microsoft.com' in valid_url or 'tendersontime.com' in valid_url or 'tenderimpulse.com' in valid_url or 'biddingsource.com' in valid_url or 'jobinrwanda.com' in valid_url or 'globaltenders.com' in valid_url:
                        print(f"Skipping internal link or ad: {href}")
                        continue

                    try:
                        response = requests.get(valid_url, headers=headers, allow_redirects=True)
                        response.raise_for_status()
                    except requests.RequestException as e:
                        logging.warning(f"Failed to access {valid_url}: {e}")
                        continue

                    format_type = get_format(valid_url)
                    logging.info(f"Visiting URL: {valid_url}\nFormat: {format_type}\n")

                    closing_dates = extract_closing_dates_from_content(response, format_type)

                    if closing_dates:
                        for date, keyword in closing_dates:
                            try:
                                closing_date_parsed = parse_closing_date(date)
                                tender_status = "Open" if closing_date_parsed > datetime.now().date() else "Closed"

                                tender_info = {
                                    'title': title,
                                    'closing_date': closing_date_parsed,
                                    'source_url': valid_url,
                                    'status': tender_status,
                                    'format': format_type,
                                    'scraped_at': datetime.now().date()
                                }

                                logging.info(f"Closing Keyword Found: {keyword}")
                                logging.info(f"Closing Keyword Date: {closing_date_parsed}")
                                logging.info(f"Tender Status: {tender_status}")

                                insert_tender_to_db(tender_info, db_connection)
                                logging.info(f"Tender inserted into database: {title}")

                                tenders.append(tender_info)

                            except ValueError as ve:
                                logging.error(f"Date parsing error for '{title}': {str(ve)}")
                                continue

                    else:
                        logging.info(f"Closing date not found for {title}, skipping.")

            except Exception as e:
                logging.error(f"Error scraping {search_url}: {str(e)}")

    return tenders  # Return the list of tenders if needed


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
    # Sample search engines
    search_engines = ["Google"]  # Add other engines as needed

    # Get the database connection
    from config import get_db_connection
    db_connection = get_db_connection()

    # Call the function with the required arguments
    scraped_tenders = scrape_tenders(search_engines, db_connection)
    print("Scraped tenders:", scraped_tenders)
