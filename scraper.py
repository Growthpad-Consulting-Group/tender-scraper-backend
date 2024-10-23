import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import PyPDF2
from io import BytesIO
from urllib.parse import urljoin, urlparse
from config import get_db_connection
from docx import Document  # Import for reading .docx files

def extract_closing_dates(text):
    # Define a pattern to find various date formats, including submission deadline
    closing_keywords = r"(closing date|submitted by|not later than|closes on|submit by|deadline for submission|deadline for submission of bids|deadline|submit before|expiry date|due date|final submission|end date|submission date|last date to submit|submission deadline|final date|Submission Deadline|SUBMISSION DEADLINE|Deadline for sending application)"
    date_formats = r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\w+\s+\d{1,2},\s+\d{4}|\d{1,2} \w+ \d{4}|(\d{1,2}(?:th|st|nd|rd)?\s+\w+\s+\d{4})|(\w+\s+\d{1,2},\s+\d{4})|\d{1,2} \w+ \d{4}|\d{1,2} \w+ \d{2,4})"

    pattern = rf"{closing_keywords}[\s:]*{date_formats}"

    matches = re.findall(pattern, text, re.IGNORECASE)
    dates = [(match[1], match[0]) for match in matches]  # Extract both date and keyword
    return dates

def clean_date_string(date_str):
    # Remove ordinal suffixes (st, nd, rd, th)
    date_str = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1', date_str)
    # Remove extra spaces
    date_str = ' '.join(date_str.split())
    return date_str

def parse_closing_date(date):
    # Clean the date string
    cleaned_date = clean_date_string(date)

    # Try different date formats to parse it
    for fmt in ("%d %B %Y", "%d/%m/%Y", "%d %b %Y", "%d %B %Y at %H.%M %Z"):
        try:
            return datetime.strptime(cleaned_date, fmt).date()  # Returns a date object
        except ValueError:
            continue

    raise ValueError(f"Unable to parse date: {cleaned_date}")

def get_format(url):
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'

def is_valid_url(url, base_url):
    parsed_url = urlparse(url)
    if parsed_url.scheme in ['http', 'https']:
        return url
    if url.startswith('/'):
        return urljoin(base_url, url)
    return None  # Return None if URL is not valid

def construct_search_url(search_engine, query):
    if search_engine == "Google":
        return f"https://www.google.com/search?q={query}&tbs=qdr:w"  # Past week filter
    return None

def scrape_tenders(search_engines, keywords):
    tenders = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }

    for engine in search_engines:
        for keyword in keywords:
            search_url = construct_search_url(engine, keyword)
            print(f"Performing search with query: {search_url}")
            try:
                response = requests.get(search_url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.find_all('a', href=True)

                for link in links:
                    href = link['href']
                    title = link.get_text(strip=True)

                    valid_url = is_valid_url(href, search_url)
                    if not valid_url or 'google.com' in valid_url or 'microsoft.com' in valid_url or 'tendersontime.com' in valid_url or 'tenderimpulse.com' in valid_url:
                        print(f"Skipping internal link or ad: {href}")
                        continue

                    # Check for potential redirects
                    try:
                        response = requests.get(valid_url, headers=headers, allow_redirects=True)
                        response.raise_for_status()
                    except requests.RequestException as e:
                        print(f"Failed to access {valid_url}: {e}")
                        continue

                    format_type = get_format(valid_url)

                    # Updated log output for visiting URL
                    print(f"Visiting URL: {valid_url}\n"
                          f"Format: {format_type}\n")

                    closing_dates = []

                    if format_type == 'PDF':
                        pdf_text = extract_pdf_text(response.content)
                        closing_dates = extract_closing_dates(pdf_text)
                    elif format_type == 'DOCX':
                        docx_text = extract_docx_text(response.content)
                        closing_dates = extract_closing_dates(docx_text)
                    else:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        page_text = soup.get_text()
                        closing_dates = extract_closing_dates(page_text)

                    # Print formatted log output for closing dates
                    if closing_dates:
                        for date, keyword in closing_dates:
                            try:
                                closing_date_parsed = parse_closing_date(date)
                                tender_status = "Open" if closing_date_parsed > datetime.now().date() else "Closed"

                                print(f"Closing Keyword Found:  {keyword}")

                                # Tender information to insert into the database
                                tender_info = {
                                    'title': title,
                                    'closing_date': closing_date_parsed,
                                    'source_url': valid_url,
                                    'status': tender_status,
                                    'format': format_type,
                                    'scraped_at': datetime.now().date()
                                }

                                # Insert or update the tender in the database
                                insert_tender_to_db(tender_info)

                                # Updated log output for database insertion
                                print(f"Inserting tender into database:\n"
                                      f"Title: {title}\n"
                                      f"URL: {valid_url}\n"
                                      f"Status: {tender_status}\n"
                                      f"Format: {format_type}\n")

                            except ValueError as ve:
                                print(str(ve))
                                continue  # Skip this tender if date parsing fails

                    else:
                        print(f"Closing date not found, skipping tender.\n")

            except Exception as e:
                print(f"Error scraping {search_url}: {str(e)}")
    return tenders

def extract_pdf_text(pdf_content):
    pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
    text = ""
    for page_num in range(len(pdf_reader.pages)):
        text += pdf_reader.pages[page_num].extract_text() or ""
    return text

def extract_docx_text(docx_content):
    with BytesIO(docx_content) as f:
        doc = Document(f)
        text = "\n".join([paragraph.text for paragraph in doc.paragraphs])
    return text

def insert_tender_to_db(tender_info):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        title = tender_info['title']
        closing_date = tender_info['closing_date']
        source_url = tender_info['source_url']
        status = tender_info['status']
        format_type = tender_info['format']
        scraped_at = tender_info['scraped_at']

        print(f"Inserting tender into database: {title} (URL: {source_url}, Status: {status}, Format: {format_type})")

        cur.execute(
            "INSERT INTO tenders (title, closing_date, source_url, status, format, scraped_at) VALUES (%s, %s, %s, %s, %s, %s)",
            (title, closing_date, source_url, status, format_type, scraped_at)
        )
        conn.commit()
    except Exception as e:
        print(f"Error inserting tender into database: {str(e)}")
    finally:
        cur.close()
        conn.close()

def fetch_reliefweb_jobs():
    api_url = "https://api.reliefweb.int/v1/jobs"
    params = {
        "appname": "ReliefWebKenyaScrape",
        "profile": "list",
        "preset": "latest",
        "query[value]": "theme.id:4588 AND country.id:131",
        "query[operator]": "AND",
        "limit": 20
    }
    response = requests.get(api_url, params=params)
    if response.status_code == 200:
        jobs = response.json()
        return jobs['data']  # Adjust based on the actual JSON structure
    else:
        print(f"Failed to fetch jobs: {response.status_code}")
        return []

def scrape_jobs_from_reliefweb():
    jobs = fetch_reliefweb_jobs()
    for job in jobs:
        title = job['fields']['title']
        url = job['fields']['url']
        closing_date_str = job['fields']['closing_date']

        try:
            closing_date_parsed = parse_closing_date(closing_date_str)
            tender_status = "Open" if closing_date_parsed > datetime.now().date() else "Closed"

            tender_info = {
                'title': title,
                'closing_date': closing_date_parsed,
                'source_url': url,
                'status': tender_status,
                'format': 'API',
                'scraped_at': datetime.now().date()
            }

            insert_tender_to_db(tender_info)

        except ValueError as ve:
            print(str(ve))
            continue  # Skip this job if date parsing fails

if __name__ == "__main__":
    search_engines = ["Google"]  # Add more search engines if needed
    keywords = ["tender", "proposal", "RFP"]  # Add your keywords here

    # Scrape tenders from search engines
    scrape_tenders(search_engines, keywords)

    # Scrape jobs from ReliefWeb API
    scrape_jobs_from_reliefweb()
