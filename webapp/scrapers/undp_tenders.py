import re
from datetime import datetime, date
import requests
from bs4 import BeautifulSoup
import uuid
from webapp.config import get_db_connection
from webapp.routes.tenders.tender_utils import insert_tender_to_db
from webapp.db.db import get_relevant_keywords
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_format(url):
    """Determine the document format based on the URL."""
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'

def ensure_db_connection():
    """Check and ensure the database connection is valid."""
    try:
        db_connection = get_db_connection()
        if db_connection is None:
            logger.warning("Database connection is None. Reconnecting...")
            return None
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return db_connection
    except Exception as e:
        logger.error(f"Error establishing or testing database connection: {str(e)}")
        return None

def make_tender_serializable(tender):
    """Convert non-serializable fields in a tender dictionary to serializable formats."""
    serializable_tender = tender.copy()
    if 'closing_date' in serializable_tender and isinstance(serializable_tender['closing_date'], date):
        serializable_tender['closing_date'] = serializable_tender['closing_date'].isoformat()
    if 'scraped_at' in serializable_tender and isinstance(serializable_tender['scraped_at'], date):
        serializable_tender['scraped_at'] = serializable_tender['scraped_at'].isoformat()
    return serializable_tender

def scrape_undp_tenders(scraping_task_id=None, set_task_state=None, socketio=None):
    """Scrapes tenders from the UNDP procurement notices page and inserts them into the database."""
    url = "https://procurement-notices.undp.org/"

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
        'summary': {
            "urlsVisited": 1,
            "openTenders": 0,
            "closedTenders": 0,
            "totalTenders": 0
        },
        'message': "Started scraping UNDP tenders"
    }, namespace='/scraping')

    tenders = []
    open_tenders = 0
    closed_tenders = 0
    visited_urls = [url]
    db_connection = None

    try:
        # Ensure a valid database connection
        db_connection = ensure_db_connection()
        if not db_connection:
            logger.error("Failed to establish a database connection.")
            set_task_state(scraping_task_id, {
                "status": "error",
                "startTime": start_time,
                "tenders": [],
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
                'status': 'error',
                'startTime': start_time,
                'tenders': [],
                'visitedUrls': visited_urls,
                'totalUrls': len(visited_urls),
                'summary': {
                    "urlsVisited": len(visited_urls),
                    "openTenders": open_tenders,
                    "closedTenders": closed_tenders,
                    "totalTenders": len(tenders)
                },
                'message': "Failed to establish database connection"
            }, namespace='/scraping')
            return tenders  # Return empty tenders list

        # Fetch keywords related to UNDP from the database
        keywords = get_relevant_keywords(db_connection)
        if not keywords:
            logger.warning("No keywords found for 'UNDP'. Aborting scrape.")
            set_task_state(scraping_task_id, {
                "status": "error",
                "startTime": start_time,
                "tenders": [],
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
                'status': 'error',
                'startTime': start_time,
                'tenders': [],
                'visitedUrls': visited_urls,
                'totalUrls': len(visited_urls),
                'summary': {
                    "urlsVisited": len(visited_urls),
                    "openTenders": open_tenders,
                    "closedTenders": closed_tenders,
                    "totalTenders": len(tenders)
                },
                'message': "No keywords found for 'UNDP'"
            }, namespace='/scraping')
            return tenders  # Return empty tenders list

        keywords = [keyword.lower() for keyword in keywords]

        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to retrieve UNDP page, status code: {response.status_code}")
            set_task_state(scraping_task_id, {
                "status": "error",
                "startTime": start_time,
                "tenders": [],
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
                'status': 'error',
                'startTime': start_time,
                'tenders': [],
                'visitedUrls': visited_urls,
                'totalUrls': len(visited_urls),
                'summary': {
                    "urlsVisited": len(visited_urls),
                    "openTenders": open_tenders,
                    "closedTenders": closed_tenders,
                    "totalTenders": len(tenders)
                },
                'message': f"Failed to retrieve UNDP page, status code: {response.status_code}"
            }, namespace='/scraping')
            return tenders  # Return empty tenders list

        logger.info("Successfully retrieved UNDP page.")
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all tender links
        tender_elements = soup.find_all('a', class_='vacanciesTableLink')
        logger.info(f"Found {len(tender_elements)} tenders.")

        for tender in tender_elements:
            title_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Title' in x.strip())
            title = title_label.find_next_sibling('span').text.strip() if title_label else "N/A"

            # Check if the title contains any of the keywords
            if not any(keyword in title.lower() for keyword in keywords):
                continue

            ref_no_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Ref No' in x.strip())
            reference_number = ref_no_label.find_next_sibling('span').text.strip() if ref_no_label else "N/A"

            deadline_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Deadline' in x.strip())
            deadline_str = deadline_label.find_next_sibling('span').find('nobr').text.strip() if deadline_label and deadline_label.find_next_sibling('span').find('nobr') else "N/A"

            logger.info(f"Deadline string found for tender '{title}': {deadline_str}")

            try:
                match = re.search(r'(\d{1,2}-\w{3}-\d{2})', deadline_str)
                if match:
                    cleaned_date = match.group(1)
                    logger.info(f"Cleaned date part: '{cleaned_date}'")
                    deadline_date = datetime.strptime(cleaned_date, "%d-%b-%y").date()
                else:
                    raise ValueError("Date not found in the deadline string.")
            except ValueError as e:
                logger.error(f"Error parsing deadline date for tender '{title}': {e}")
                continue

            status = "open" if deadline_date > datetime.now().date() else "closed"
            if status == "open":
                open_tenders += 1
            else:
                closed_tenders += 1
            negotiation_id = tender['href'].split('=')[-1]
            source_url = f"https://procurement-notices.undp.org/view_negotiation.cfm?nego_id={negotiation_id}"
            if source_url not in visited_urls:
                visited_urls.append(source_url)

            format_type = get_format(source_url)

            tender_data = {
                'title': title,
                'description': reference_number,
                'closing_date': deadline_date,
                'source_url': source_url,
                'status': status,
                'format': format_type,
                'scraped_at': datetime.now().date(),
                'tender_type': "UNDP",
                'location': "Kenya",
            }
            tenders.append(tender_data)

            # Ensure database connection is valid before insertion
            db_connection = ensure_db_connection()
            if not db_connection:
                logger.error("Database connection dropped. Skipping insertion.")
                serializable_tenders = [make_tender_serializable(t) for t in tenders]
                set_task_state(scraping_task_id, {
                    "status": "error",
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
                    'status': 'error',
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
                    'message': "Database connection dropped during insertion"
                }, namespace='/scraping')
                return tenders  # Return tenders collected so far

            try:
                insert_tender_to_db(tender_data, db_connection)
                logger.info(f"Tender inserted into database: {title}")
                logger.info(f"Title: {title}\n"
                            f"Reference Number: {reference_number}\n"
                            f"Closing Date: {deadline_date}\n"
                            f"Status: {status}\n"
                            f"Source URL: {source_url}\n"
                            f"Format: {format_type}\n"
                            f"Tender Type: UNDP\n")
                logger.info("=" * 40)

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
                    'message': f"Processed tender: {title}"
                }, namespace='/scraping')
            except Exception as e:
                logger.error(f"Error inserting tender '{title}' into database: {e}")

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
            'message': "Completed scraping UNDP tenders"
        }, namespace='/scraping')

        logger.info("Scraping completed.")
        return tenders  # Return tenders on successful completion

    except Exception as e:
        logger.error(f"An error occurred while scraping: {e}")
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
            'message': f"Error scraping UNDP tenders: {str(e)}"
        }, namespace='/scraping')
        return tenders  # Return tenders collected so far
    finally:
        if db_connection:
            db_connection.close()

if __name__ == "__main__":
    tenders = scrape_undp_tenders()
    print(f"Scraped {len(tenders)} tenders")