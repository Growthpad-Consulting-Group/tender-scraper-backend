import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import logging
import time
import uuid
from webapp.config import get_db_connection
from webapp.routes.tenders.tender_utils import insert_tender_to_db
from webapp.db.db import get_relevant_keywords

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

def jobinrwanda_tenders(scraping_task_id=None, set_task_state=None, socketio=None):
    """Scrapes tenders from Job in Rwanda website and inserts them into the database based on specific keywords."""
    url = "https://www.jobinrwanda.com/jobs/tender"
    retry_attempts = 3

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
        'message': "Started scraping Job in Rwanda tenders"
    }, namespace='/scraping')

    tenders = []
    open_tenders = 0
    closed_tenders = 0
    visited_urls = [url]
    db_connection = None

    try:
        # Ensure we have a valid database connection
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

        # Fetch keywords from the database
        keywords = get_relevant_keywords(db_connection)
        if not keywords:
            logger.warning("No keywords found in relevant_keywords table. Aborting scrape.")
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
                'message': "No keywords found in relevant_keywords table"
            }, namespace='/scraping')
            return tenders  # Return empty tenders list
        keywords = [keyword.lower() for keyword in keywords]
        logger.info(f"Fetched {len(keywords)} keywords: {keywords}")

        with requests.Session() as session:
            for attempt in range(retry_attempts):
                try:
                    logger.info(f"Attempting to fetch tenders, attempt {attempt + 1}")
                    response = session.get(url, timeout=10)
                    if response.status_code != 200:
                        logger.error(f"Failed to retrieve tenders page, status code: {response.status_code}")
                        time.sleep(5)
                        continue

                    logger.info("Successfully retrieved tenders page.")
                    soup = BeautifulSoup(response.content, 'html.parser')
                    tender_cards = soup.find_all('article', class_='node--type-job')
                    logger.info(f"Found {len(tender_cards)} tenders.")

                    for card in tender_cards:
                        title_tag = card.find('h5', class_='card-title')
                        if title_tag:
                            anchor_tag = title_tag.find_parent('a')
                            if anchor_tag and 'href' in anchor_tag.attrs:
                                title = anchor_tag.find('span').get_text(strip=True).lower()
                                source_url = f"https://www.jobinrwanda.com{anchor_tag['href']}"
                                if source_url not in visited_urls:
                                    visited_urls.append(source_url)
                            else:
                                logger.warning("Anchor tag not found or href missing.")
                                continue
                        else:
                            logger.warning("Title tag not found.")
                            continue

                        # Check if the title contains any of the keywords
                        matched_keywords = [keyword for keyword in keywords if keyword in title]
                        if matched_keywords:
                            description_tag = card.find('p', class_='card-text')
                            description = description_tag.get_text(strip=True) if description_tag else "N/A"

                            deadline_tag = description_tag.find('time', class_='datetime') if description_tag else None
                            closing_date_str = deadline_tag['datetime'] if deadline_tag else None

                            if closing_date_str:
                                try:
                                    closing_date = datetime.fromisoformat(closing_date_str.replace('Z', '+00:00')).date()
                                    status = "open" if closing_date > datetime.now().date() else "closed"
                                    if status == "open":
                                        open_tenders += 1
                                    else:
                                        closed_tenders += 1
                                except ValueError as e:
                                    logger.warning(f"Invalid closing date format for tender '{title}': {closing_date_str}")
                                    continue
                            else:
                                logger.warning(f"No closing date found for tender '{title}'")
                                continue

                            format_type = get_format(source_url)
                            tender_data = {
                                'title': title,
                                'description': description,
                                'closing_date': closing_date,
                                'source_url': source_url,
                                'status': status,
                                'format': format_type,
                                'scraped_at': datetime.now().date(),
                                'tender_type': "Job in Rwanda",
                                'location': "Rwanda"
                            }
                            tenders.append(tender_data)

                            if insert_tender_to_db(tender_data, db_connection):
                                logger.info(f"Tender inserted into database: {title}")
                                logger.info(f"Matched keywords: {', '.join(matched_keywords)} for tender: {title}")

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
                                    'message': f"Processed tender: {title} (Matched keywords: {', '.join(matched_keywords)})"
                                }, namespace='/scraping')
                            else:
                                logger.error("Failed to insert tender. Checking database connection...")
                                db_connection = ensure_db_connection()
                                if not db_connection:
                                    logger.error("Database connection dropped. Skipping tender.")
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

                    break  # Exit retry loop after successful execution

                except requests.RequestException as e:
                    logger.error(f"A request error occurred during attempt {attempt + 1}: {str(e)}")
                    if attempt == retry_attempts - 1:
                        # If all retries fail, update state and return
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
                            'message': f"Failed to retrieve tenders page after {retry_attempts} attempts: {str(e)}"
                        }, namespace='/scraping')
                        return tenders  # Return tenders collected so far
                    time.sleep(5)

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
            'message': "Completed scraping Job in Rwanda tenders"
        }, namespace='/scraping')

        logger.info("Scraping completed.")
        return tenders  # Return tenders on successful completion

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
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
            'message': f"Error scraping Job in Rwanda tenders: {str(e)}"
        }, namespace='/scraping')
        return tenders  # Return tenders collected so far
    finally:
        if db_connection:
            db_connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    tenders = jobinrwanda_tenders()
    logger.info(f"Scraped {len(tenders)} tenders")