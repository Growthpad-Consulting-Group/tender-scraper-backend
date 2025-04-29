# import logging
# from webapp.config import get_db_connection  # Import function to establish database connection
# import requests  # For making HTTP requests to scrape data
# from bs4 import BeautifulSoup  # For parsing HTML content
# from datetime import datetime  # For handling date and time
# from webapp.db import insert_tender_to_db, get_keywords_and_terms  # Import database utilities
# from webapp.routes.tenders.tender_utils import (
#     extract_closing_dates,
#     parse_closing_date,
#     get_format,
#     extract_pdf_text,
#     extract_docx_text,
#     construct_search_url,
#     extract_description_from_response,
#     is_relevant_tender
# )
# from urllib.parse import urlparse  # Import this to parse URLs
# import random  # For generating random delays
# import time  # For adding sleep delays between requests
# import re  # For regular expression operations
# from webapp.extensions import socketio  # Import your SocketIO instance here
# from webapp.services.log import ScrapingLog  # Import your custom logging class


# # List of common user agents to simulate different browsers
# USER_AGENTS = [
#     'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
#     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
#     'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:58.0) Gecko/20100101 Firefox/58.0',
#     'Mozilla/5.0 (Linux; Android 6.0; Nexus 6 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Mobile Safari/537.36',
#     'Mozilla/5.0 (iPhone; CPU iPhone OS 10_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14E277 Safari/602.1',
# ]

# # Exponential backoff configuration
# MAX_RETRIES = 5
# BACKOFF_FACTOR = 2  # Experiment with this to suit your needs

# # Mapping of supported search engines
# SEARCH_ENGINES = [
#     "Google",
#     "Bing",
#     "Yahoo",
#     "DuckDuckGo",
#     "Ask"
# ]

# def scrape_tenders(db_connection, query, search_engines):
#     """
#     Scrapes tenders from specified search engines using a constructed query.

#     Args:
#         db_connection: The active database connection object.
#         query (str): The constructed search query.
#         search_engines (list): A list of selected search engines for scraping.

#     Returns:
#         list: A list of tender information dictionaries scraped from the web.
#     """
#     tenders = []  # Initialize a list to hold scraped tender data
#     excluded_domains = [
#         "microsoft.com", "go.microsoft.com", "privacy.microsoft.com",
#         "support.microsoft.com", "about.ads.microsoft.com",
#         "aka.ms", "yahoo.com", "search.yahoo.com",
#         "duckduckgo.com", "ask.com", "bing.com",
#         "youtube.com",
#     ]

#     total_steps = len(search_engines)  # Count the total search engines for progress calculation

#     for i, engine in enumerate(search_engines):
#         search_url = construct_search_url(engine, query)
#         ScrapingLog.add_log(f"Constructed Search URL for engine '{engine}': {search_url}")

#         for attempt in range(MAX_RETRIES):
#             headers = {
#                 "User-Agent": random.choice(USER_AGENTS),
#             }
#             time.sleep(random.uniform(3, 10))  # Random delay before each request

#             try:
#                 response = requests.get(search_url, headers=headers)
#                 response.raise_for_status()

#                 soup = BeautifulSoup(response.content, 'html.parser')
#                 links = soup.find_all('a', href=True)
#                 for link in links:
#                     href = link['href']
#                     actual_url = extract_actual_link_from_search_result(href, engine)

#                     if "google.com" in actual_url or any(domain in actual_url for domain in excluded_domains):
#                         continue

#                     if is_valid_url(actual_url):
#                         ScrapingLog.add_log(f"Visiting URL: {actual_url}")
#                         tender_title = clean_title(link.text.strip())
#                         tender_details = scrape_tender_details(actual_url, tender_title, headers, db_connection)
#                         if tender_details:
#                             tenders.append(tender_details)

#                 progress = ((i + 1) / total_steps) * 100
#                 logging.info(f"Emitting progress: {progress}%")
#                 socketio.emit('scraping_progress', {'progress': progress})  # Emit the progress
#                 break  # Exit loop if the request is successful

#             except requests.exceptions.HTTPError as http_err:
#                 ScrapingLog.add_log(f'Error scraping {search_url}: {str(http_err)}')

#                 # Handle 429 Too Many Requests
#                 if response.status_code == 429 and attempt < MAX_RETRIES - 1:
#                     wait_time = BACKOFF_FACTOR ** attempt  # Exponential backoff
#                     ScrapingLog.add_log(f"429 Too Many Requests. Backing off for {wait_time} seconds.")
#                     time.sleep(wait_time)
#                     continue  # Retry the request
#                 else:
#                     break  # Exit the retry loop on non-retriable error

#     socketio.emit('scraping_complete', {})
#     return tenders

# def clean_title(title):
#     """
#     Cleans the title by removing URLs, unwanted text, and hyphens.

#     Args:
#         title (str): The title to clean.

#     Returns:
#         str: The cleaned title without URLs, unwanted segments, or hyphens.
#     """
#     # print(f"Original Title Before Clean: '{title}'")  # For debugging
#     # ScrapingLog.add_log(f"Original Title Before Clean: '{title}'")

#     # Remove any URLs (http, https, www, etc.)
#     clean_title = re.sub(r'https?://\S+|www\.\S+', '', title)  # Remove any URLs
#     clean_title = re.sub(r'\s*›.*$', '', clean_title)  # Remove everything after '›'

#     # Remove domain-like patterns and any suffixes (e.g., chrips.or.ke) with preceding spaces or hyphens
#     clean_title = re.sub(r'[\w.-]+\.[a-zA-Z]{2,}[\s-]*', '', clean_title)

#     # Remove hyphens
#     clean_title = clean_title.replace('-', '')  # Remove all hyphens

#     # Remove leading/trailing spaces and handle excessive spaces
#     clean_title = re.sub(r'^\s*|\s*$', '', clean_title)  # Remove whitespace from the start and end
#     clean_title = re.sub(r'\s+', ' ', clean_title)  # Collapse multiple spaces to a single space

#     # ScrapingLog.add_log(f"Cleaned Title After Clean: '{clean_title.strip()}'")
#     return clean_title.strip()


# def extract_actual_link_from_search_result(href, engine):
#     """
#     Extracts the actual link from the search engine results based on the search engine.

#     Args:
#         href (str): The href attribute from the search result anchor tag.
#         engine (str): The name of the search engine.

#     Returns:
#         str: The extracted actual URL.
#     """
#     if engine in ['Google', 'Bing', 'Yahoo', 'DuckDuckGo', 'Ask']:
#         match = re.search(r'q=(.+?)(&|$)', href)  # Find the query parameter in the URL
#         if match:
#             return match.group(1)  # Return the actual URL
#     return href  # If not matched, return the href as is

# def is_valid_url(url):
#     """
#     Check if the URL is valid and begins with http or https.

#     Args:
#         url (str): The URL to validate.

#     Returns:
#         bool: True if valid, False otherwise.
#     """
#     return re.match(r'https?://', url) is not None  # Validate the URL format

# def log_scraping_details(db_connection, website_name, visiting_url, tenders_found,
#                          tender_title, closing_date, closing_keyword,
#                          filtered_keyword, is_relevant, status):
#     """
#     Logs the scraping details into the database, updating existing entries based on visiting_url.
    
#     Args:
#         db_connection: Database connection object
#         website_name (str): Name of the website
#         visiting_url (str): URL being visited
#         tenders_found (bool): Whether tenders were found
#         tender_title (str): Title of the tender
#         closing_date (date): Closing date of the tender
#         closing_keyword (str): Keyword used to identify closing date
#         filtered_keyword (str): Keywords matched in the tender
#         is_relevant (str): "Yes" or "No" string indicating relevance
#         status (str): Status of the tender
#     """
#     try:
#         with db_connection.cursor() as cursor:
#             # Convert boolean tenders_found to integer
#             tenders_found_int = 1 if tenders_found else 0

#             # Convert Yes/No to boolean for relevant field
#             is_relevant_bool = is_relevant.lower() == 'yes'

#             # Check if the visiting_url already exists
#             check_query = """
#             SELECT id FROM scraping_log WHERE visiting_url = %s;
#             """
#             cursor.execute(check_query, (visiting_url,))
#             existing_record = cursor.fetchone()

#             if existing_record:
#                 update_query = """
#                 UPDATE scraping_log
#                 SET website_name = %s, 
#                     tenders_found = %s, 
#                     tender_title = %s, 
#                     closing_date = %s,
#                     closing_keyword = %s, 
#                     filtered_keyword = %s, 
#                     relevant = %s, 
#                     status = %s, 
#                     created_at = NOW()
#                 WHERE visiting_url = %s;
#                 """
#                 cursor.execute(update_query, (
#                     website_name,
#                     tenders_found_int,
#                     tender_title,
#                     closing_date,
#                     closing_keyword,
#                     filtered_keyword,
#                     is_relevant_bool,
#                     status,
#                     visiting_url
#                 ))
#             else:
#                 insert_query = """
#                 INSERT INTO scraping_log (
#                     website_name, 
#                     visiting_url, 
#                     tenders_found, 
#                     tender_title, 
#                     closing_date,
#                     closing_keyword, 
#                     filtered_keyword, 
#                     relevant, 
#                     status, 
#                     created_at
#                 ) 
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
#                 """
#                 cursor.execute(insert_query, (
#                     website_name,
#                     visiting_url,
#                     tenders_found_int,
#                     tender_title,
#                     closing_date,
#                     closing_keyword,
#                     filtered_keyword,
#                     is_relevant_bool,
#                     status
#                 ))

#             db_connection.commit()
#             # ScrapingLog.add_log("Log entry successfully inserted/updated in scraping_log table.")
#             logging.info(f"Log entry successfully inserted/updated in scraping_log table.")

#     except Exception as e:
#         ScrapingLog.add_log(f"Error in logging tender details: {str(e)}")
#         if db_connection:
#             db_connection.rollback()




# def scrape_tender_details(url, title, headers, db_connection):
#     time.sleep(random.uniform(1, 3))
#     description = ""

#     parsed_url = urlparse(url)
#     website_name = f"{parsed_url.scheme}://{parsed_url.netloc}"

#     # Ensure tender_title is initialized correctly
#     tender_title = title.strip()

#     try:
#         response = requests.get(url, headers=headers)
#         response.raise_for_status()

#         format_type = get_format(url)
#         extracted_text = ""

#         # Extract text based on the format
#         if format_type == 'PDF':
#             extracted_text = extract_pdf_text(response.content)
#         elif format_type == 'DOCX':
#             extracted_text = extract_docx_text(response.content)
#         else:
#             soup = BeautifulSoup(response.content, 'html.parser')
#             h1_tags = soup.find('h1')
#             h2_tags = soup.find('h2')
#             tender_title = (h1_tags.text.strip() if h1_tags else h2_tags.text.strip() if h2_tags else tender_title)

#             description = " ".join(p.text.strip() for p in soup.find_all('p')[:2]) if soup.find_all('p') else ""
#             extracted_text = f"{tender_title} {description}"

#         # Extract closing dates now
#         closing_dates = extract_closing_dates(extracted_text, db_connection)

#         log_tenders_found = 0

#         if closing_dates:
#             for date, keyword in closing_dates:
#                 try:
#                     closing_date_parsed = parse_closing_date(date)
#                     filtered_keyword = is_relevant_tender(extracted_text, db_connection)
#                     is_relevant = "Yes" if filtered_keyword else "No"

#                     tender_info = {
#                         'title': tender_title,
#                         'description': description,
#                         'closing_date': closing_date_parsed,
#                         'source_url': url,
#                         'status': "open" if closing_date_parsed > datetime.now().date() else "closed",
#                         'format': format_type,
#                         'scraped_at': datetime.now().date(),
#                         'tender_type': 'Uploaded Websites',
#                         'is_relevant': is_relevant,
#                         'filtered_based_on': filtered_keyword,  # Additional field

#                     }

#                     # Log tender information
#                     ScrapingLog.add_log("====================================")
#                     ScrapingLog.add_log("Found Tender")
#                     ScrapingLog.add_log(f"Tender Title: {tender_info['title']}")
#                     ScrapingLog.add_log(f"Closing Date: {closing_date_parsed}")
#                     ScrapingLog.add_log(f"Closing Date Keyword Found: {keyword}")
#                     ScrapingLog.add_log(f"Status: {tender_info['status']}")
#                     ScrapingLog.add_log(f"Tender Type: {tender_info['tender_type']}")
#                     ScrapingLog.add_log(f"Filtered Based on: {filtered_keyword}")
#                     ScrapingLog.add_log(f"Relevant Tender: {is_relevant}")
#                     ScrapingLog.add_log("====================================")

#                     # Log scraping details
#                     log_scraping_details(db_connection, website_name, url, True, tender_info['title'],
#                                          closing_date_parsed, keyword, filtered_keyword, is_relevant, tender_info['status'])

#                     # Insert the tender into the database if relevant
#                     if is_relevant == "Yes":
#                         try:
#                             insertion_status = insert_tender_to_db(tender_info, db_connection)
#                             if insertion_status:
#                                 ScrapingLog.add_log(f"Inserted tender into database: Success - {tender_info['title']}")
#                             else:
#                                 ScrapingLog.add_log(f"Inserting tender into database: Failed - {tender_info['title']}")
#                         except Exception as insert_err:
#                             # ScrapingLog.add_log(f"Error inserting tender into database: {str(insert_err)}")
#                             logging.info(f"Error inserting tender into database: {str(insert_err)}")

#                     return tender_info  # Return this structured data

#                 except Exception as ve:
#                     ScrapingLog.add_log(f"Error processing closing date for tender from '{url}': {str(ve)}")

#         else:
#             ScrapingLog.add_log(f"No closing dates found for URL: {url}")
#             return None  # Return None if no tenders found

#     except Exception as e:
#         ScrapingLog.add_log(f"Error processing tender details for URL {url}: {str(e)}")
#         return None  # Return None if there is an exception