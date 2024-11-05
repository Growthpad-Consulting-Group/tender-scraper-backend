import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from io import BytesIO
import fitz  # PyMuPDF

def extract_closing_dates(text):
    closing_keywords = r"(closing date|submit by|deadline|expiry date|due date|last date to submit|submitted by|submission deadline|final date to submit|response due date|proposal submission deadline|offer submission deadline|bids due by|deadline for submission|end date|last day to apply|response deadline|submission end date|accepting applications until|applications due|on or before|not later than|reach us not later than)"

    date_formats = r"(\d{1,2}\s*(AM|PM)?\s*on\s*\d{1,2}\s+\w+\s+\d{4}|\d{1,2}\s*\w+\s*\d{4}|\d{1,2}\s*[-/]\s*\d{1,2}\s*[-/]\s*\d{2,4}|\w+\s+\d{1,2},\s+\d{4}|\d{1,2} \w+ \d{4}|\d{1,2}\s+\w+\s+\d{4}|\d{1,2}\s+\w+\s+\d{2})"

    pattern = rf"{closing_keywords}[\s:]*({date_formats})"

    matches = re.findall(pattern, text, re.IGNORECASE)
    dates = [(match[1], match[0]) for match in matches]
    return dates


def clean_date_string(date_str):
    date_str = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1', date_str)  # Remove ordinal suffixes
    date_str = re.sub(r'(?i)(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', '', date_str)  # Remove weekday
    date_str = ' '.join(date_str.split())  # Remove extra spaces
    return date_str

def parse_closing_date(date):
    cleaned_date = clean_date_string(date)
    formats = [
        "%d %B %Y", "%d %b %Y", "%d %m %Y", "%d/%m/%Y", "%Y-%m-%d",
        "%B %d, %Y", "%d %B %y", "%d-%m-%Y", "%d %B", "%B %d",
        "%I:%M %p on %d %B %Y", "%d %B %Y %I:%M %p"  # New formats
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
    parsed_url = urlparse(url)
    if parsed_url.scheme in ['http', 'https']:
        return url
    if url.startswith('/'):
        return urljoin(base_url, url)
    return None

def get_format(url):
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'

def construct_search_url(search_engine, query):
    if search_engine == "Google":
        return f"https://www.google.com/search?q={query}&tbs=qdr:w"  # Past week filter
    return None

def extract_pdf_text(pdf_content):
    pdf_document = fitz.open(stream=pdf_content, filetype="pdf")
    text = ""
    for page in pdf_document:
        text += page.get_text()
    pdf_document.close()
    return text


def extract_docx_text(docx_content):
    from docx import Document

    with BytesIO(docx_content) as f:
        doc = Document(f)
        return "\n".join([paragraph.text for paragraph in doc.paragraphs])

def insert_tender_to_db(tender_info, db_connection):
    cur = db_connection.cursor()

    current_date = datetime.now().date()
    tender_info['status'] = 'open' if tender_info['closing_date'] > current_date else 'closed'

    try:
        print(f"Inserting tender with values: {tender_info}")
        cur.execute(
            """
            INSERT INTO tenders (title, closing_date, source_url, status, format, description, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_url) DO UPDATE  -- Adjust to conflict only on source_url
            SET closing_date = EXCLUDED.closing_date,
                status = EXCLUDED.status
            WHERE tenders.closing_date <> EXCLUDED.closing_date
            """,
            (tender_info['title'], tender_info['closing_date'], tender_info['source_url'],
             tender_info['status'], tender_info['format'], tender_info.get('description', ''),
             tender_info['scraped_at'])
        )
        db_connection.commit()
        print(f"Inserted/Updated tender:\nTitle: {tender_info['title']}\nURL: {tender_info['source_url']}\nStatus: {tender_info['status']}\nFormat: {tender_info['format']}\n")
    except Exception as e:
        db_connection.rollback()
        print(f"Error inserting/updating tender: {e}")
    finally:
        cur.close()
