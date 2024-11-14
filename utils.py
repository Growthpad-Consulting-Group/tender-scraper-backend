import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse
from io import BytesIO
import fitz  # PyMuPDF
from docx import Document  # Ensure you have python-docx installed

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_closing_dates(text):
    closing_keywords = r"(closing date|submit by|deadline|expiry date|due date|last date to submit|submitted by|submission deadline|final date to submit|response due date|proposal submission deadline|offer submission deadline|bids due by|deadline for submission|end date|last day to apply|response deadline|submission end date|accepting applications until|applications due|on or before|not later than)"
    date_formats = r"(\d{1,2}\s*(AM|PM)?\s*on\s*\d{1,2}\s+\w+\s+\d{4}|\d{1,2}\s*\w+\s*\d{4}|\d{1,2}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{2,4}|\w+\s+\d{1,2},\s+\d{4}|\d{1,2} \w+ \d{4}|\d{1,2}\s+\w+\s+\d{4}|\d{1,2}\s+\w+\s+\d{2})"

    pattern = rf"{closing_keywords}[\s:]*({date_formats})"
    matches = re.findall(pattern, text, re.IGNORECASE)
    dates = [(match[1], match[0]) for match in matches]
    return dates

def clean_date_string(date_str):
    date_str = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1', date_str)
    date_str = re.sub(r'(?i)(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', '', date_str)
    date_str = ' '.join(date_str.split())
    return date_str

def parse_closing_date(date):
    cleaned_date = clean_date_string(date)
    formats = [
        "%d %B %Y", "%d %b %Y", "%d %m %Y", "%d/%m/%Y", "%Y-%m-%d",
        "%B %d, %Y", "%d %B %y", "%d-%m-%Y", "%d %B", "%B %d",
        "%I:%M %p on %d %B %Y", "%d %B %Y %I:%M %p"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(cleaned_date, fmt).date()
        except ValueError:
            continue

    if " " in cleaned_date:
        month_day = cleaned_date.split(" ")
        if len(month_day) == 2:
            current_year = datetime.now().year
            try:
                return datetime.strptime(f"{month_day[1]} {month_day[0]} {current_year}", "%d %b %Y").date()
            except ValueError:
                pass

    raise ValueError(f"Unable to parse date: {cleaned_date}")

def is_valid_url(url, base_url):
    """Validates and normalizes URLs."""
    parsed_url = urlparse(url)

    if parsed_url.scheme in ['http', 'https']:
        return url

    if url.startswith('/'):
        return urljoin(base_url, url)

    logging.warning(f"Invalid URL encountered: {url}")
    return None

def get_format(url):
    """Determines the format of the content based on the URL."""
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'

def construct_search_url(search_engine, query):
    """Constructs search URLs for different search engines."""
    if search_engine == "Google":
        return f"https://www.google.com/search?q={query}"
    elif search_engine == "Bing":
        return f"https://www.bing.com/search?q={query}"
    elif search_engine == "Yahoo":
        return f"https://search.yahoo.com/search?p={query}"
    elif search_engine == "DuckDuckGo":
        return f"https://duckduckgo.com/?q={query}"
    elif search_engine == "Ask":
        return f"https://www.ask.com/web?q={query}"
    else:
        logging.error(f"Search engine '{search_engine}' not supported.")
        return None

def extract_pdf_text(pdf_content):
    """Extracts text from a PDF file."""
    pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
    text = ""
    for page in pdf_document:
        text += page.get_text()
    pdf_document.close()
    return text

def extract_docx_text(docx_content):
    """Extracts text from a DOCX file."""
    with BytesIO(docx_content) as f:
        doc = Document(f)
        return "\n".join([paragraph.text for paragraph in doc.paragraphs])

def extract_description_from_response(response, format_type):
    """Extracts description from the response based on content format."""
    if format_type == 'PDF':
        pdf_text = extract_pdf_text(response.content)
        return pdf_text.split('\n')[0] if pdf_text else ""
    else:  # HTML or other formats
        soup = BeautifulSoup(response.content, 'html.parser')

        # Attempt to get description from <meta> tag
        description_meta = soup.find('meta', attrs={'name': 'description'})
        if description_meta and description_meta.get('content'):
            return description_meta['content']

        # Fallback to the first paragraph
        paragraphs = soup.find_all('p')
        return paragraphs[0].text if paragraphs else ""

def insert_tender_to_db(tender_info, db_connection):
    """Inserts or updates the tender information in the database."""
    if db_connection is None or db_connection.closed:
        logging.error("Database connection is not active.")
        return False  # Return false for a failed attempt

    cur = db_connection.cursor()

    # Ensure status is set correctly
    current_date = datetime.now().date()
    tender_info['status'] = 'open' if tender_info['closing_date'] > current_date else 'closed'

    # Check for duplicates based on source_url or title
    check_sql = """
        SELECT * FROM tenders 
        WHERE source_url = %s OR title = %s
    """
    params_check = (tender_info['source_url'], tender_info['title'])

    try:
        cur.execute(check_sql, params_check)
        existing_tender = cur.fetchone()
        if existing_tender:
            logging.warning(f"Duplicate found for Source URL: {tender_info['source_url']} or Title: {tender_info['title']}. Overwriting the record in the database.")

        # Insert SQL with conflict resolution
        insert_sql = """
            INSERT INTO tenders (title, description, closing_date, source_url, status, scraped_at, format, tender_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_url) DO UPDATE
            SET closing_date = EXCLUDED.closing_date,
                status = EXCLUDED.status,
                description = EXCLUDED.description,
                scraped_at = EXCLUDED.scraped_at,
                format = EXCLUDED.format,
                tender_type = EXCLUDED.tender_type
        """

        # Prepare parameters for insertion
        params = (
            tender_info['title'],
            tender_info.get('description', ''),  # Ensure you provide a default description if not found
            tender_info['closing_date'],
            tender_info['source_url'],
            tender_info['status'],
            tender_info['scraped_at'],
            tender_info['format'],
            tender_info['tender_type']
        )

        logging.debug(f"Executing SQL: {insert_sql}")
        logging.debug(f"With parameters: {params}")

        # Execute the insertion/updating query and commit
        cur.execute(insert_sql, params)
        db_connection.commit()
        logging.info(f"Successfully inserted/updated tender: {tender_info['title']}")
        return True  # Indicate success
    except Exception as e:
        db_connection.rollback()
        logging.error("Error inserting/updating tender: %s", str(e))
        logging.error("Detailed diagnostic information:")
        logging.error("SQL: %s", insert_sql)
        logging.error("Parameters: %s", params)
        return False  # Indicate failure
    finally:
        cur.close()

