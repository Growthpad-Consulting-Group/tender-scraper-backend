import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webapp.config import get_db_connection
from webapp.routes.tenders.tender_utils import insert_tender_to_db
from webapp.db.db import get_directory_keywords
import logging

# Configure Python logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress Selenium DEBUG logs
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('selenium.webdriver').setLevel(logging.WARNING)

def get_format(url):
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'

def parse_date(date_str):
    """Parse date from string and return a datetime object."""
    try:
        # Remove ordinal suffixes and extra spaces/characters
        date_str = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str).strip()

        # Attempt to parse with various date formats
        formats = [
            "%B %d, %Y",         # e.g., January 7, 2025
            "%B %d %Y",          # e.g., January 7 2025
            "%d %B, %Y",         # e.g., 7 January, 2025
            "%d %B %Y",          # e.g., 7 January 2025
            "%B %d %Y %H:%M",    # e.g., January 7 2025 09:00
            "%d %B %Y %H:%M",    # e.g., 7 January 2025 09:00
            "%b %d %Y %H:%M",    # e.g., Jan 9 2025 11:00
            "%b %d, %Y",         # e.g., Jan 9, 2025
            "%b %d %Y"           # e.g., Jan 9 2025
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)  # Return datetime object
            except ValueError:
                continue

        raise ValueError("Date format not recognized")
    except Exception as e:
        logger.error(f"Error parsing date '{date_str}': {e}")
        return None

def ensure_db_connection():
    try:
        db_connection = get_db_connection()
        if db_connection is None:
            logger.error("Database connection is None. Reconnecting...")
            return None

        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")  # Simple test query to check connection
        cursor.close()
        return db_connection
    except Exception as e:
        logger.error(f"Error establishing or testing database connection: {str(e)}")
        return None


def setup_selenium_driver():
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--window-size=1920,1080')

        logger.info("Initializing Chrome WebDriver...")
        driver = webdriver.Chrome(options=options)

        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)

        logger.info("Chrome WebDriver setup completed successfully")
        return driver
    except Exception as e:
        logger.error(f"Failed to set up Chrome WebDriver: {str(e)}")
        raise

def load_page_with_retry(driver, url, max_retries=3):
    for attempt in range(max_retries):
        try:
            driver.delete_all_cookies()
            logger.info(f"Visiting URL: {url}")
            driver.get(url)

            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            logger.info("Successfully visited the URL.")
            return True

        except TimeoutException:
            logger.warning(f"Timeout while loading the URL: {url}. Attempt {attempt + 1}/{max_retries}.")
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            else:
                logger.error("Failed to load the page after multiple attempts.")
                raise
        except Exception as e:
            logger.error(f"Error loading the page: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            else:
                logger.error(f"Could not load {url} after {attempt + 1} attempts.")
                raise

def scrape_ppip_tenders():
    url = "https://tenders.go.ke/tenders"
    driver = None
    db_connection = None

    try:
        db_connection = ensure_db_connection()
        if not db_connection:
            logger.error("Failed to establish a database connection.")
            return

        keywords = get_directory_keywords(db_connection, 'PPIP')
        if not keywords:
            logger.error("No keywords found for 'PPIP'. Aborting scrape.")
            return

        keywords = [keyword.lower() for keyword in keywords]
        logger.info(f"Fetched keywords: {keywords}")

        driver = setup_selenium_driver()
        load_page_with_retry(driver, url)

        for search_keyword in keywords:
            logger.info(f"Searching for: {search_keyword}")

            # Locate both search inputs and perform the search
            try:
                # Wait for search form 1
                search_input1 = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "search"))
                )
                search_input1.clear()
                search_input1.send_keys(search_keyword)

                # Wait for search form 2
                search_input2 = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.ID, "input-101"))
                )
                search_input2.clear()
                search_input2.send_keys(search_keyword)

                # Click the search button
                search_button = driver.find_element(By.XPATH, "//button[contains(@class, 'v-btn') and span[text()='Search']]")
                search_button.click()
                logger.info("Search submitted.")

                # Wait for results directly
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".v-table.v-theme--lightTheme tbody tr"))
                )
                logger.info("Results loaded successfully.")

            except TimeoutException:
                logger.warning(f"No results loaded for keyword '{search_keyword}' within the expected time.")
                continue

            # Fetch the page source after results are available
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            tenders = soup.find_all('tr')[1:]  # Skip header row
            tender_count = len(tenders)

            logger.info(f"Found {tender_count} tenders after searching for '{search_keyword}'.")

            if tender_count == 0:
                logger.warning(f"No tenders found for keyword: '{search_keyword}'.")
                continue

            # Process each tender
            for tender in tenders:
                # Ensure the tender row has enough cells
                cells = tender.find_all('td')
                if len(cells) < 6:
                    logger.warning("Tender row does not have enough data.")
                    continue

                # Extract various tender details
                tender_number = cells[0].text.strip()
                title = cells[1].text.strip()
                description = cells[1].text.strip()
                procure_method = cells[3].text.strip()
                proc_category = cells[4].text.strip()
                close_date = cells[5].text.strip()

                closing_date_datetime = parse_date(close_date)
                if closing_date_datetime is None:
                    logger.warning(f"Could not parse closing date '{close_date}' for tender: {title}.")
                    continue

                if closing_date_datetime > datetime.now():
                    logger.info(f"Open tender found: {title} with closing date {close_date}")

                    # Handle clicking on the actions button
                    try:
                        actions_button = WebDriverWait(driver, 20).until(
                            EC.element_to_be_clickable((By.XPATH, ".//button[span[text()=' Actions ']]"))
                        )
                        actions_button.click()  # Click the action button
                        logger.info("Clicked on the ACTIONS button.")

                        # Wait for the dropdown menu to show
                        dropdown_menu = WebDriverWait(driver, 60).until(
                            EC.visibility_of_element_located((By.CSS_SELECTOR, ".v-list.v-theme--lightTheme"))
                        )

                        view_more_details_item = dropdown_menu.find_element(By.XPATH, ".//div[contains(@class, 'v-list-item-title') and contains(text(), 'View more details')]")
                        view_more_details_item.click()
                        logger.info("'View more details' menu was clicked successfully.")

                        # Wait for the modal to open and fetch the details
                        modal_element = WebDriverWait(driver, 20).until(
                            EC.visibility_of_element_located((By.CLASS_NAME, 'v-row'))
                        )
                        logger.info("Modal opened.")

                        # Fetch additional details from the modal
                        time.sleep(1)  # Optional stability wait
                        modal_title = modal_element.find_element(By.XPATH, ".//span[text()='Title']/following::span[1]").text.strip()
                        logger.info(f"Title extracted from Modal: {modal_title}")

                        tender_number = modal_element.find_element(By.XPATH, "//span[text()='Tender number']/following-sibling::span").text.strip()
                        closing_date = modal_element.find_element(By.XPATH, "//span[text()='Close date and time']/following-sibling::span").text.strip()
                        closing_date_datetime = parse_date(closing_date)
                        public_link = modal_element.find_element(By.XPATH, "//span[text()='Public Link']/following-sibling::span/a").get_attribute('href')

                        status = 'Open' if closing_date_datetime > datetime.now() else 'Closed'

                        tender_data = {
                            'title': description,
                            'description': tender_number,
                            'closing_date': closing_date_datetime.date() if closing_date_datetime else None,
                            'source_url': public_link,
                            'status': status,
                            'format': 'HTML',
                            'scraped_at': datetime.now().date(),
                            'tender_type': "PPIP",
                        }

                        # Inserting tender into DB
                        try:
                            db_connection.cursor().execute("SELECT 1")  # Check DB connection
                            insert_tender_to_db(tender_data, db_connection)
                            logger.info(f"Inserted tender: {modal_title} | Source URL: {public_link}")
                        except Exception as e:
                            logger.error(f"Error inserting tender '{modal_title}' into database: {e}")

                    except Exception as e:
                        logger.error(f"Error processing action for tender '{title}': {str(e)}")

        logger.info("Scraping completed.")

    except Exception as e:
        logger.error(f"Fatal error during scraping: {str(e)}")
    finally:
        if driver:
            driver.quit()
            logger.info("Chrome WebDriver closed.")
        if db_connection:
            db_connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    try:
        scrape_ppip_tenders()
    except Exception as e:
        logger.error(f"Script failed with error: {str(e)}")