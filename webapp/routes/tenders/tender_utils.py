import re
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin, urlparse
from io import BytesIO
import fitz  # PyMuPDF for handling PDF files
from docx import Document  # Ensure you have python-docx installed for handling DOCX files

# Configure logging for debugging and tracking purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_closing_keywords(db_connection):
    """
    Fetches closing keywords from the database.

    Args:
        db_connection: The active database connection object.

    Returns:
        list: A list of closing keywords.
    """
    cur = db_connection.cursor()
    cur.execute("SELECT keyword FROM closing_keywords;")
    keywords = [row[0] for row in cur.fetchall()]
    cur.close()
    return keywords


def fetch_directory_keywords(db_connection, tender_type='Uploaded Websites'):
    """
    Fetches directory keywords from the database filtered by tender type.

    Args:
        db_connection: The active database connection object.
        tender_type (str): The tender type to filter keywords.

    Returns:
        list: A list of directory keywords.
    """
    cur = db_connection.cursor()
    cur.execute("SELECT keyword FROM directory_keywords WHERE tender_type = %s;", (tender_type,))
    keywords = [row[0] for row in cur.fetchall()]
    cur.close()
    return keywords

def is_relevant_tender(text: str, db_connection) -> str:
    """
    Checks for the presence of directory keywords in the provided text and returns matched keywords.

    Args:
        text (str): The text to search for relevant keywords.
        db_connection: The active database connection object.

    Returns:
        str: Comma-separated matched keywords in their original case, or None if no relevant keywords are found.
    """
    directory_keywords = fetch_directory_keywords(db_connection, tender_type='Uploaded Websites')
    if not directory_keywords:
        logging.warning("No directory keywords found in the database for tender type 'Uploaded Websites'.")
        return None

    # Log the fetched directory keywords
    logging.debug(f"Fetched directory keywords: {directory_keywords}")

    # Compile regex pattern for keywords
    directory_keywords_pattern = r"|".join(map(re.escape, directory_keywords))
    logging.debug(f"Using regex pattern: {directory_keywords_pattern}")

    # Find all matches in the text while ignoring case
    matches = re.findall(directory_keywords_pattern, text, re.IGNORECASE)

    if matches:
        # Normalize to lowercase and remove duplicates
        unique_matches = set(keyword.lower() for keyword in matches)
        # Retrieve original-cased words from the matches
        final_keywords = [keyword for keyword in directory_keywords if keyword.lower() in unique_matches]

        # Log found keywords (original case)
        logging.debug(f"Matched keywords in original case: {final_keywords}")

        # Return unique matched keywords as a comma-separated string
        return ', '.join(final_keywords)

    return None  # No relevant keywords found





def extract_closing_dates(text: str, db_connection) -> list:
    """
    Extracts closing dates from the provided text using regular expressions
    and keywords fetched from the database.

    Args:
        text (str): The text to search for closing dates.
        db_connection: The active database connection object.

    Returns:
        list: A list of tuples containing closing date keywords and the corresponding dates.
    """
    closing_keywords = fetch_closing_keywords(db_connection)
    if not closing_keywords:
        logging.warning("No closing keywords found in the database.")
        return []

    # Join keywords into a regex pattern
    closing_keywords_pattern = r"|".join(map(re.escape, closing_keywords))
    date_formats = r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\d{1,2}\s*(?:AM|PM)?\s*on\s*\d{1,2}\s+\w+\s+\d{4}|" \
                   r"\w+\s+\d{1,2},\s+\d{4}|\d{1,2}\s+\w+\s+\d{4}|\d{1,2}\s+\w+\s+\d{2}|" \
                   r"\w+\s+\d{1,2}|\d{1,2}\s*[ -]\s*\d{1,2})"

    # Construct regex pattern and find matches
    pattern = rf"({closing_keywords_pattern})[\s:]*({date_formats})"
    matches = re.findall(pattern, text, re.IGNORECASE)
    dates = [(match[1], match[0]) for match in matches]  # Store date and corresponding keyword
    return dates


def clean_date_string(date_str: str) -> str:
    """
    Cleans and formats the date string to a standard format.
    
    Args:
        date_str (str): The raw date string to clean.
    
    Returns:
        str: A cleaned date string.
    """
    # Remove suffixes like "st", "nd", "rd", "th"
    date_str = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1', date_str)
    # Remove day names (e.g., "Monday")
    date_str = re.sub(r'(?i)(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', '', date_str)
    # Normalize whitespace
    date_str = ' '.join(date_str.split())
    return date_str

def parse_closing_date(date: str) -> datetime.date:
    """
    Parses a cleaned date string into a datetime.date object.
    
    Args:
        date (str): The cleaned date string to parse.
    
    Returns:
        datetime.date: A date object representing the parsed date.
    
    Raises:
        ValueError: If the date cannot be parsed into a known format.
    """
    cleaned_date = clean_date_string(date)

    # Define the possible date formats
    formats = [
        "%d %B %Y", "%d %b %Y", "%d %m %Y", "%d/%m/%Y",
        "%Y-%m-%d", "%B %d, %Y", "%d %B %y", "%d-%m-%Y",
        "%d %B", "%B %d", "%I:%M %p on %d %B %Y", "%d %B %Y %I:%M %p",
        "%d-%m-%y", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y",
        "%B %d %Y", "%B %d", "%d %b"
    ]

    # Try each format to parse the date
    for fmt in formats:
        try:
            return datetime.strptime(cleaned_date, fmt).date()
        except ValueError:
            continue  # Try the next format if this fails

    # Special handling for ambiguous month/day cases
    if " " in cleaned_date:
        month_day = cleaned_date.split(" ")
        if len(month_day) == 2:
            current_year = datetime.now().year
            try:
                return datetime.strptime(f"{month_day[1]} {month_day[0]} {current_year}", "%d %b %Y").date()
            except ValueError:
                pass  # Skip if this format fails

    # Log the error if no formats matched
    logging.error(f"Unable to parse date: {cleaned_date}")
    raise ValueError(f"Unable to parse date: {cleaned_date}")

def is_valid_url(url: str, base_url: str) -> str:
    """
    Validates and normalizes a given URL.
    
    Args:
        url (str): The URL to validate.
        base_url (str): The base URL to resolve relative URLs against.
    
    Returns:
        str: The valid (and possibly normalized) URL, or None if invalid.
    """
    parsed_url = urlparse(url)

    # Check for valid HTTP/HTTPS URLs
    if parsed_url.scheme in ['http', 'https']:
        return url

    # Normalize relative URLs by joining with the base URL
    if url.startswith('/'):
        return urljoin(base_url, url)

    # Log a warning for invalid URLs
    logging.warning(f"Invalid URL encountered: {url}")
    return None

def get_format(url: str) -> str:
    """
    Determines the content format based on the file extension in the URL.
    
    Args:
        url (str): The URL to inspect.
    
    Returns:
        str: The format type as 'PDF', 'DOCX', or 'HTML'.
    """
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'  # Default is HTML format

def construct_search_url(search_engine: str, query: str) -> str:
    """
    Constructs search URLs for various search engines based on a query.
    
    Args:
        search_engine (str): The name of the search engine.
        query (str): The search query.
    
    Returns:
        str: The full search URL for the specified search engine, or None if unsupported.
    """
    # Mapping of search engines to their respective base URLs
    search_engines = {
        "Google": "https://www.google.com/search?q=",
        "Bing": "https://www.bing.com/search?q=",
        "Yahoo": "https://search.yahoo.com/search?p=",
        "DuckDuckGo": "https://duckduckgo.com/?q=",
        "Ask": "https://www.ask.com/web?q="
    }

    if search_engine in search_engines:
        return f"{search_engines[search_engine]}{query}"

    # Log an error for unsupported search engines
    logging.error(f"Search engine '{search_engine}' not supported.")
    return None

def extract_pdf_text(pdf_content: bytes) -> str:
    """
    Extracts text from the provided PDF content.
    
    Args:
        pdf_content (bytes): The PDF file content as bytes.
    
    Returns:
        str: The extracted text from the PDF.
    """
    # Open the PDF using PyMuPDF and extract text from each page
    pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
    text = ""
    for page in pdf_document:
        text += page.get_text()
    pdf_document.close()  # Clean up
    return text

def extract_docx_text(docx_content: bytes) -> str:
    """
    Extracts text from the provided DOCX content.
    
    Args:
        docx_content (bytes): The DOCX file content as bytes.
    
    Returns:
        str: The extracted text from the DOCX file.
    """
    with BytesIO(docx_content) as f:
        doc = Document(f)  # Load the DOCX file
        # Join all paragraph texts into a single string
        return "\n".join([paragraph.text for paragraph in doc.paragraphs])

def extract_description_from_response(response, format_type: str) -> str:
    """
    Extracts a description from the HTTP response based on the content format.
    
    Args:
        response: The HTTP response object containing the content.
        format_type (str): The format of the content ('PDF', 'DOCX', or 'HTML').
    
    Returns:
        str: A description extracted from the content, or an empty string if not found.
    """
    if format_type == 'PDF':
        # For PDF, extract text and return the first line as the description
        pdf_text = extract_pdf_text(response.content)
        return pdf_text.split('\n')[0] if pdf_text else ""
    else:  # For HTML or other formats
        soup = BeautifulSoup(response.content, 'html.parser')

        # Try to get description from the <meta> tag with name="description"
        description_meta = soup.find('meta', attrs={'name': 'description'})
        if description_meta and description_meta.get('content'):
            return description_meta['content']

        # Fallback: return the text of the first paragraph in the content
        paragraphs = soup.find_all('p')
        return paragraphs[0].text if paragraphs else ""

def insert_tender_to_db(tender_info: dict, db_connection) -> bool:
    """
    Inserts or updates the tender information in the database.
    
    Args:
        tender_info (dict): A dictionary containing all relevant information about the tender.
        db_connection: The active database connection object.
    
    Returns:
        bool: True if the operation was successful, False otherwise.
    """
    # Ensure the database connection is active
    if db_connection is None:
        logging.error("Database connection is not active.")
        return False  # Return false if the connection is invalid

    cur = db_connection.cursor()  # Create a cursor for database operations
    current_date = datetime.now().date()  # Get today's date
    # Determine the status of the tender based on its closing date
    tender_info['status'] = 'open' if tender_info['closing_date'] > current_date else 'closed'

    # SQL query to check for existing records
    check_sql = """
        SELECT * FROM tenders 
        WHERE source_url = %s OR title = %s
    """
    params_check = (tender_info['source_url'], tender_info['title'])

    try:
        # Execute the check for existing tender records
        cur.execute(check_sql, params_check)
        existing_tender = cur.fetchone()

        if existing_tender:
            # Log a warning if a duplicate tender is found
            logging.warning(f"Duplicate found for Source URL: {tender_info['source_url']} or Title: {tender_info['title']}. Overwriting the record in the database.")

        # SQL statement to insert or update the tender record
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

        params = (
            tender_info['title'],
            tender_info.get('description', ''),
            tender_info['closing_date'],
            tender_info['source_url'],
            tender_info['status'],
            tender_info['scraped_at'],
            tender_info['format'],
            tender_info['tender_type']
        )

        logging.debug(f"Executing SQL: {insert_sql}")
        logging.debug(f"With parameters: {params}")

        # Execute the insert/update operation
        cur.execute(insert_sql, params)
        db_connection.commit()  # Commit the transaction
        logging.info(f"Successfully inserted/updated tender: {tender_info['title']}")
        return True  # Indicate success
    except Exception as e:
        db_connection.rollback()  # Roll back if there was an error
        logging.error(f"Error inserting/updating tender: {str(e)}")
        return False  # Indicate failure
    finally:
        cur.close()  # Always ensure the cursor is closed to free up resources