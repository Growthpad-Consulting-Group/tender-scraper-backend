import re
import time
from datetime import datetime, date
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import uuid
from webapp.config import get_db_connection
from webapp.routes.tenders.tender_utils import insert_tender_to_db
from webapp.db.db import get_relevant_keywords
import logging

# Configure Python logging
logging.basicConfig(level=logging.INFO)

# Suppress Selenium DEBUG logs
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('selenium.webdriver').setLevel(logging.WARNING)

def get_format(url):
    """Determine the document format based on the URL."""
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'

def extract_deadline_date(tender):
    """Extract the deadline date from the tender element."""
    try:
        deadline_cell = tender.find('div', class_='tableCell resultInfo1 deadline')
        if not deadline_cell:
            logging.info("No deadline cell found.")
            return None

        deadline_str = deadline_cell.get_text(strip=True)
        match = re.search(r'(\d{1,2}-\w{3}-\d{4})', deadline_str)
        if match:
            cleaned_date = match.group(1)
            return datetime.strptime(cleaned_date, "%d-%b-%Y").date()
        else:
            raise ValueError("Deadline date not found in the extracted string.")

    except Exception as e:
        logging.error(f"Error extracting deadline date: {e}")
        return None

def ensure_db_connection():
    """Check and ensure the database connection is valid."""
    try:
        db_connection = get_db_connection()
        if db_connection is None:
            logging.error("Database connection is None. Reconnecting...")
            return None
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return db_connection
    except Exception as e:
        logging.error(f"Error establishing or testing database connection: {str(e)}")
        return None

def setup_selenium_driver():
    """Setup Chrome WebDriver with optimized settings for better performance."""
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--window-size=1920,1080')

        logging.info("Initializing Chrome WebDriver...")
        driver = webdriver.Chrome(options=options)

        driver.set_page_load_timeout(60)
        driver.implicitly_wait(20)

        logging.info("Chrome WebDriver setup completed successfully")
        return driver
    except Exception as e:
        logging.error(f"Failed to setup Chrome WebDriver: {str(e)}")
        raise

def load_page_with_retry(driver, url, max_retries=3):
    """Load page with retry mechanism."""
    for attempt in range(max_retries):
        try:
            driver.delete_all_cookies()
            driver.get(url)

            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            logging.info("Successfully visited the URL.")
            return True

        except TimeoutException:
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            else:
                raise
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            else:
                raise

def select_beneficiary_country(driver, country):
    """Select a beneficiary country or territory from the dropdown based on the provided country name."""
    try:
        search_input = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "selNoticeCountry-input"))
        )
        search_input.clear()
        search_input.send_keys(country)

        time.sleep(2)  # Wait for the dropdown to populate

        country_option = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, f"//li[contains(text(), '{country}')]"))
        )

        country_option.click()

        selected_country = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "selNoticeCountry"))
        )
        selected_value = selected_country.get_attribute('value')

        logging.info(f"{country} successfully selected from the Beneficiary country or territory dropdown.")
        return selected_value
    except NoSuchElementException as e:
        logging.error(f"Element not found during the selection process for {country}: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Failed to select {country} from the dropdown: {str(e)}")
        return None

def make_tender_serializable(tender):
    """Convert non-serializable fields in a tender dictionary to serializable formats."""
    serializable_tender = tender.copy()
    if 'closing_date' in serializable_tender and isinstance(serializable_tender['closing_date'], date):
        serializable_tender['closing_date'] = serializable_tender['closing_date'].isoformat()
    if 'scraped_at' in serializable_tender and isinstance(serializable_tender['scraped_at'], date):
        serializable_tender['scraped_at'] = serializable_tender['scraped_at'].isoformat()
    return serializable_tender

def scrape_ungm_tenders(scraping_task_id=None, set_task_state=None, socketio=None):
    """Scrapes tenders from UNGM."""
    url = "https://www.ungm.org/Public/Notice"
    driver = None
    db_connection = None
    countries = {
        "Kenya": "2397",
        "South Africa": "2481",
        "Uganda": "2503",
        "Ghana": "2370",
        "Nigeria": "2443",
        "Togo": "2494",
        "Ethiopia": "2358",
        "Rwanda": "2462",
        "Tanzania": "2507",
    }

    # Generate a scraping_task_id if not provided (for standalone execution)
    scraping_task_id = scraping_task_id or str(uuid.uuid4())
    start_time = datetime.now().isoformat()

    # Initialize task state
    set_task_state(scraping_task_id, {
        "status": "running",
        "startTime": start_time,
        "tenders": [],
        "visited_urls": [url],
        "total_urls": 1,
        "summary": {}
    })
    socketio.emit('scrape_update', {
        'taskId': scraping_task_id,
        'status': 'running',
        'startTime': start_time,
        'tenders': [],
        'visitedUrls': [url],
        'totalUrls': 1,
        'message': "Started scraping UNGM tenders"
    }, namespace='/scraping')

    tenders = []
    open_tenders = 0
    closed_tenders = 0
    visited_urls = [url]

    try:
        db_connection = ensure_db_connection()
        if not db_connection:
            logging.error("Failed to establish a database connection.")
            set_task_state(scraping_task_id, {
                "status": "error",
                "startTime": start_time,
                "tenders": [],
                "visited_urls": visited_urls,
                "total_urls": len(visited_urls),
                "summary": {}
            })
            socketio.emit('scrape_update', {
                'taskId': scraping_task_id,
                'status': 'error',
                'startTime': start_time,
                'message': "Failed to establish database connection"
            }, namespace='/scraping')
            return tenders  # Return empty tenders list

        keywords = get_relevant_keywords(db_connection)
        if not keywords:
            logging.error("No keywords found for 'UNGM'. Aborting scrape.")
            set_task_state(scraping_task_id, {
                "status": "error",
                "startTime": start_time,
                "tenders": [],
                "visited_urls": visited_urls,
                "total_urls": len(visited_urls),
                "summary": {}
            })
            socketio.emit('scrape_update', {
                'taskId': scraping_task_id,
                'status': 'error',
                'startTime': start_time,
                'message': "No keywords found for 'UNGM'"
            }, namespace='/scraping')
            return tenders  # Return empty tenders list

        keywords = [keyword.lower() for keyword in keywords]

        driver = setup_selenium_driver()

        load_page_with_retry(driver, url)
        time.sleep(5)

        for country, value in countries.items():
            chosen_value = select_beneficiary_country(driver, country)
            if chosen_value != value:
                logging.error(f"Expected {country} to be selected, but got value: {chosen_value}.")
                continue

            time.sleep(5)

            total_results = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "noticeSearchTotal"))
            ).text
            total_tenders = int(total_results)
            logging.info(f"{total_tenders} tenders found after selecting {country}.")

            scroll_pause_time = 2
            last_height = driver.execute_script("return document.body.scrollHeight")

            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time)

                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            tender_elements = soup.find_all('div', class_='tableRow dataRow notice-table')
            total_dynamic_tenders = len(tender_elements)
            logging.info(f"Total tenders after dynamic load for {country}: {total_dynamic_tenders}")

            found_for_keyword = 0

            for tender in tender_elements:
                title_elem = tender.find('div', class_='resultTitle')
                title = title_elem.get_text(strip=True) if title_elem else ""

                # Extracting the href for the specific tender link
                tender_link = title_elem.find('a', href=True)
                if tender_link and "href" in tender_link.attrs:
                    href = tender_link['href']
                    source_url = f"https://www.ungm.org{href}"
                    if source_url not in visited_urls:
                        visited_urls.append(source_url)
                else:
                    logging.warning(f"No valid link found for tender titled: {title} ({country})")
                    continue

                deadline_date = extract_deadline_date(tender)
                if not deadline_date:
                    continue

                status = "open" if deadline_date > datetime.now().date() else "closed"
                if status == "open":
                    open_tenders += 1
                else:
                    closed_tenders += 1
                format_type = get_format(source_url)

                tender_data = {
                    'title': title,
                    'description': title,
                    'closing_date': deadline_date,
                    'source_url': source_url,
                    'status': status,
                    'format': format_type,
                    'scraped_at': datetime.now().date(),
                    'tender_type': "UNGM",
                    'location': country,
                }

                if any(keyword in title.lower() for keyword in keywords):
                    found_for_keyword += 1
                    tenders.append(tender_data)
                    try:
                        # Attempt to execute an insert, reconnecting if there's an issue
                        if db_connection is None:
                            logging.warning("Database connection is None. Attempting to reconnect...")
                            db_connection = ensure_db_connection()

                        insert_tender_to_db(tender_data, db_connection)
                        logging.info(f"Inserted tender from {country}: {title} | Source URL: {source_url}")

                        # Create a serializable version of tenders for Redis and Socket.IO
                        serializable_tenders = [make_tender_serializable(t) for t in tenders]

                        # Emit an update with the current state
                        set_task_state(scraping_task_id, {
                            "status": "running",
                            "startTime": start_time,
                            "tenders": serializable_tenders,
                            "visited_urls": visited_urls,
                            "total_urls": len(visited_urls),
                            "summary": {
                                "urlsVisited": len(visited_urls),
                                "openTenders": open_tenders,
                                "closedTenders": closed_tenders,
                                "totalTenders": len(tenders)
                            }
                        })
                        socketio.emit('scrape_update', {
                            'taskId': scraping_task_id,
                            'status': 'running',
                            'startTime': start_time,
                            'tenders': serializable_tenders,
                            'visitedUrls': visited_urls,
                            'totalUrls': len(visited_urls),
                            'summary': {
                                "urlsVisited": len(visited_urls),
                                "openTenders": open_tenders,
                                "closedTenders": closed_tenders,
                                "totalTenders": len(tenders)
                            },
                            'message': f"Processed tender: {title} ({country})"
                        }, namespace='/scraping')

                    except Exception as e:
                        logging.error(f"Error inserting tender '{title}' from {country} into database: {e}")
                        # Attempt to reconnect & retry insert
                        db_connection = ensure_db_connection()
                        if db_connection:
                            try:
                                insert_tender_to_db(tender_data, db_connection)
                                logging.info(f"Reinserted tender from {country}: {title} | Source URL: {source_url}")
                            except Exception as retry_exception:
                                logging.error(f"Error reinserting tender '{title}' from {country}: {retry_exception}")

            logging.info(f"{found_for_keyword} tenders found for the specified keywords from {country}.")
            logging.info(f"{country} tender scraping completed.")

        # Calculate time taken
        time_taken = (datetime.now() - datetime.fromisoformat(start_time)).total_seconds()

        # Create a serializable version of tenders for the final state
        serializable_tenders = [make_tender_serializable(t) for t in tenders]

        # Emit completion event
        set_task_state(scraping_task_id, {
            "status": "complete",
            "startTime": start_time,
            "tenders": serializable_tenders,
            "visited_urls": visited_urls,
            "total_urls": len(visited_urls),
            "summary": {
                "urlsVisited": len(visited_urls),
                "timeTaken": time_taken,
                "openTenders": open_tenders,
                "closedTenders": closed_tenders,
                "totalTenders": len(tenders)
            }
        })
        socketio.emit('scrape_update', {
            'taskId': scraping_task_id,
            'status': 'complete',
            'startTime': start_time,
            'tenders': serializable_tenders,
            'visitedUrls': visited_urls,
            'totalUrls': len(visited_urls),
            'summary': {
                "urlsVisited": len(visited_urls),
                "timeTaken": time_taken,
                "openTenders": open_tenders,
                "closedTenders": closed_tenders,
                "totalTenders": len(tenders)
            },
            'message': "Completed scraping UNGM tenders"
        }, namespace='/scraping')

        return tenders  # Return tenders on successful completion

    except Exception as e:
        logging.error(f"Fatal error during scraping: {str(e)}")
        serializable_tenders = [make_tender_serializable(t) for t in tenders]
        set_task_state(scraping_task_id, {
            "status": "error",
            "startTime": start_time,
            "tenders": serializable_tenders,
            "visited_urls": visited_urls,
            "total_urls": len(visited_urls),
            "summary": {
                "urlsVisited": len(visited_urls),
                "timeTaken": (datetime.now() - datetime.fromisoformat(start_time)).total_seconds(),
                "openTenders": open_tenders,
                "closedTenders": closed_tenders,
                "totalTenders": len(tenders)
            }
        })
        socketio.emit('scrape_update', {
            'taskId': scraping_task_id,
            'status': 'error',
            'startTime': start_time,
            'tenders': serializable_tenders,
            'visitedUrls': visited_urls,
            'totalUrls': len(visited_urls),
            'summary': {
                "urlsVisited": len(visited_urls),
                "timeTaken": (datetime.now() - datetime.fromisoformat(start_time)).total_seconds(),
                "openTenders": open_tenders,
                "closedTenders": closed_tenders,
                "totalTenders": len(tenders)
            },
            'message': f"Error scraping UNGM tenders: {str(e)}"
        }, namespace='/scraping')
        return tenders  # Return tenders collected so far
    finally:
        if driver:
            driver.quit()
        if db_connection:
            db_connection.close()

if __name__ == "__main__":
    try:
        tenders = scrape_ungm_tenders()
        logging.info(f"Scraped {len(tenders)} tenders")
    except Exception as e:
        logging.error(f"Script failed with error: {str(e)}")