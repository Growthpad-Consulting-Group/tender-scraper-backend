import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from app.config import get_db_connection
from app.routes.tenders.tender_utils import insert_tender_to_db
from app.db.db import get_directory_keywords
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
    return 'HTML'  # Default to HTML if no specific format is found

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

def scrape_ungm_tenders():
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

    try:
        db_connection = ensure_db_connection()
        if not db_connection:
            logging.error("Failed to establish a database connection.")
            return

        keywords = get_directory_keywords(db_connection, 'UNGM')
        if not keywords:
            logging.error("No keywords found for 'UNGM'. Aborting scrape.")
            return
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

            tenders = soup.find_all('div', class_='tableRow dataRow notice-table')
            total_dynamic_tenders = len(tenders)
            logging.info(f"Total tenders after dynamic load for {country}: {total_dynamic_tenders}")

            found_for_keyword = 0

            for tender in tenders:
                title_elem = tender.find('div', class_='resultTitle')
                title = title_elem.get_text(strip=True) if title_elem else ""

                # Extracting the href for the specific tender link
                tender_link = title_elem.find('a', href=True)
                if tender_link and "href" in tender_link.attrs:
                    href = tender_link['href']
                    source_url = f"https://www.ungm.org{href}"  # Construct the full URL
                else:
                    logging.warning(f"No valid link found for tender titled: {title} ({country})")
                    continue  # Skip if no valid link is present

                deadline_date = extract_deadline_date(tender)
                if not deadline_date:
                    continue

                status = "open" if deadline_date > datetime.now().date() else "closed"
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
                }

                if any(keyword in title.lower() for keyword in keywords):
                    found_for_keyword += 1
                    try:
                        # Attempt to execute an insert, reconnecting if there's an issue
                        if db_connection is None:
                            logging.warning("Database connection is None. Attempting to reconnect...")
                            db_connection = ensure_db_connection()  # Reopen connection

                        insert_tender_to_db(tender_data, db_connection)
                        logging.info(f"Inserted tender from {country}: {title} | Source URL: {source_url}")

                    except Exception as e:
                        logging.error(f"Error inserting tender '{title}' from {country} into database: {e}")
                        # Attempt to reconnect & retry insert
                        db_connection = ensure_db_connection()  # Attempt to re-establish connection
                        if db_connection:
                            try:
                                insert_tender_to_db(tender_data, db_connection)  # Retry insert
                                logging.info(f"Reinserted tender from {country}: {title} | Source URL: {source_url}")
                            except Exception as retry_exception:
                                logging.error(f"Error reinserting tender '{title}' from {country}: {retry_exception}")

            logging.info(f"{found_for_keyword} tenders found for the specified keywords from {country}.")
            logging.info(f"{country} tender scraping completed.")

    except Exception as e:
        logging.error(f"Fatal error during scraping: {str(e)}")
    finally:
        if driver:
            driver.quit()
        if db_connection:
            db_connection.close()

if __name__ == "__main__":
    try:
        scrape_ungm_tenders()
    except Exception as e:
        logging.error(f"Script failed with error: {str(e)}")