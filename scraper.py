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
    closing_keywords = r"(closing date|submit by|deadline|expiry date|due date|last date to submit|submission deadline|final date to submit|response due date|proposal submission deadline|offer submission deadline|bids due by|deadline for submission|end date|last day to apply|response deadline|submission end date|accepting applications until|applications due|on or before)"
    date_formats = r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}|\w+\s+\d{1,2},\s+\d{4}|\d{1,2} \w+ \d{4}|\d{1,2} \w+ \d{2,4}|\w+\s+\d{1,2} \s*,?\s*\d{4}|\d{2,4}[-/]\d{1,2}[-/]\d{1,2})"
    pattern = rf"{closing_keywords}[\s:]*({date_formats})"

    matches = re.findall(pattern, text, re.IGNORECASE)
    for match in matches:
        print(f"Matched: {match}")  # Debugging output
    dates = [(match[1], match[0]) for match in matches]
    return dates


def clean_date_string(date_str):
    # Remove ordinal suffixes (st, nd, rd, th)
    date_str = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1', date_str)
    date_str = re.sub(r'(?i)(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', '', date_str)  # Remove weekday
    # Remove extra spaces
    date_str = ' '.join(date_str.split())
    return date_str


def parse_closing_date(date):
    # Clean the date string
    cleaned_date = clean_date_string(date)

    # Try different date formats to parse it
    formats = [
        "%d %B %Y",  # 14 November 2024
        "%d %b %Y",  # 14 Nov 2024
        "%d %m %Y",  # 14 11 2024
        "%d/%m/%Y",  # 14/11/2024
        "%Y-%m-%d",  # 2024-11-21
        "%B %d, %Y",  # November 14, 2024
        "%d %B %y",  # 14 November 24
        "%d-%m-%Y",  # 14-11-2024
        "%d %B",  # 14 November (for partial parsing if needed)
        "%B %d",  # December 12 (assumed current year if no year is provided)
    ]

    for fmt in formats:
        try:
            return datetime.strptime(cleaned_date, fmt).date()  # Returns a date object
        except ValueError:
            continue

    # Handle cases where year is inferred
    if " " in cleaned_date:  # Ensure it's a month and day format
        month_day = cleaned_date.split(" ")
        if len(month_day) == 2:  # e.g., 'Dec 12'
            current_year = datetime.now().year
            try:
                return datetime.strptime(f"{month_day[1]} {month_day[0]} {current_year}", "%d %b %Y").date()
            except ValueError:
                pass

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
                    if not valid_url or 'google.com' in valid_url or 'microsoft.com' in valid_url or 'tendersontime.com' in valid_url or 'tenderimpulse.com' in valid_url or 'biddingsource.com' in valid_url:
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
                            print(f"Attempting to parse date: '{date}' from keyword: '{keyword}'")
                            try:
                                closing_date_parsed = parse_closing_date(date)
                                tender_status = "Open" if closing_date_parsed > datetime.now().date() else "Closed"

                                print(f"Closing Keyword Found:  {keyword}")
                                print(f"Closing Keyword Date: {closing_date_parsed}")

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

def fetch_tenders_from_api():
    api_url = "https://api.reliefweb.int/v1/jobs?appname=rwint-user-0&profile=list&preset=latest&slim=1&query%5Bvalue%5D=country.id%3A131&query%5Boperator%5D=AND"
    response = requests.get(api_url)

    if response.status_code == 200:
        data = response.json()
        tenders = []

        for job in data['data']:
            title = job['fields']['title']
            closing_date = job['fields']['date']['closing'] if 'date' in job['fields'] else None

            if closing_date:
                closing_date_obj = datetime.strptime(closing_date, "%Y-%m-%dT%H:%M:%S%z").date()
                status = "Open" if closing_date_obj > datetime.now().date() else "Closed"
                organization = job['fields']['source'][0]['name'] if job['fields'].get('source') else 'Unknown'

                # Prepare the tender info
                tender_info = {
                    'title': title,
                    'closing_date': closing_date_obj,
                    'source_url': f"https://reliefweb.int/job/{job['id']}",
                    'status': status,
                    'format': "HTML",  # Setting format to HTML
                    'description': organization,  # Store organization in the description column
                    'scraped_at': datetime.now().date()
                }

                tenders.append(tender_info)

                # Insert into the database
                insert_tender_to_db(tender_info)

                # Print formatted log output
                print(f"Title: {title}")
                print(f"Organization: {organization}")
                print(f"Closing Date: {closing_date_obj}")
                print(f"Status: {status}")
                print(f"Format: HTML")
                print("=" * 40)  # Separator for readability

            else:
                print(f"Skipping job '{title}' due to missing closing date.")

        return tenders
    else:
        print(f"Failed to fetch tenders, status code: {response.status_code}")
        return []

def insert_tender_to_db(tender_info):
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Check if the tender already exists based on title and URL
        cur.execute("SELECT id FROM tenders WHERE title = %s AND source_url = %s",
                    (tender_info['title'], tender_info['source_url']))
        if cur.fetchone() is None:
            cur.execute(
                """
                INSERT INTO tenders (title, closing_date, source_url, status, format, description, scraped_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (tender_info['title'], tender_info['closing_date'], tender_info['source_url'],
                 tender_info['status'], tender_info['format'], tender_info['description'], tender_info['scraped_at'])
            )
            conn.commit()
            print("Tender inserted:", tender_info['title'])
        else:
            print("Duplicate tender found, skipping:", tender_info['title'])
    except Exception as e:
        print(f"Error inserting tender: {e}")
    finally:
        cur.close()
        conn.close()

