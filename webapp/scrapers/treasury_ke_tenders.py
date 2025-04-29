import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import uuid
from webapp.config import get_db_connection
from webapp.routes.tenders.tender_utils import insert_tender_to_db, parse_closing_date
from webapp.db.db import get_relevant_keywords

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_format(url):
    """Determine the document format based on the URL."""
    if url:
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

def treasury_ke_tenders(scraping_task_id=None, set_task_state=None, socketio=None):
    """Scrapes tenders from the Kenya Treasury website and inserts them into the database."""
    url = "https://www.treasury.go.ke/tenders/"

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
        'message': "Started scraping Kenya Treasury tenders"
    }, namespace='/scraping')

    tenders = []
    open_tenders = 0
    closed_tenders = 0
    visited_urls = [url]
    db_connection = None

    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to retrieve Kenya Treasury page, status code: {response.status_code}")
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
                'message': f"Failed to retrieve Kenya Treasury page, status code: {response.status_code}"
            }, namespace='/scraping')
            return

        logger.info("Successfully retrieved Kenya Treasury page.")
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find('table', {'id': 'tablepress-3'})
        if not table:
            logger.warning("The expected tender table was not found.")
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
                'message': "The expected tender table was not found"
            }, namespace='/scraping')
            return

        rows = table.find_all('tr')[1:]  # Skip header row
        logger.info(f"Found {len(rows)} rows in the tender table.")

        db_connection = ensure_db_connection()
        if not db_connection:
            logger.error("Failed to establish a database connection.")
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
            return

        keywords = get_relevant_keywords(db_connection)
        if not keywords:
            logger.warning("No keywords found for 'Kenya Treasury'. Aborting scrape.")
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
                'message': "No keywords found for 'Kenya Treasury'"
            }, namespace='/scraping')
            return
        keywords = [keyword.lower() for keyword in keywords]
        current_year = datetime.now().year

        for row in rows:
            columns = row.find_all('td')
            if len(columns) < 5:
                continue

            reference_number = columns[0].text.strip()
            title = columns[1].text.strip()
            document_url = columns[2].find('a')['href'] if columns[2].find('a') else None
            deadline_str = columns[4].text.strip()

            logger.info(f"Processing tender '{title}' with deadline: {deadline_str}")

            # Parse the deadline date using the utility function, passing db_connection
            deadline_date = parse_closing_date(deadline_str, db_connection)
            if not deadline_date:
                logger.warning(f"Skipping tender '{title}' due to unparsable or missing deadline: {deadline_str}")
                continue

            # Only process tenders with a deadline in the current year
            if deadline_date.year != current_year:
                logger.info(f"Skipping tender '{title}' with deadline {deadline_date} (not in current year {current_year})")
                continue

            # Check if the title contains any of the keywords
            if not any(keyword in title.lower() for keyword in keywords):
                logger.info(f"Skipping tender '{title}' (no matching keywords)")
                continue

            status = "open" if deadline_date > datetime.now().date() else "closed"
            if status == "open":
                open_tenders += 1
            else:
                closed_tenders += 1
            format_type = get_format(document_url)
            if document_url and document_url not in visited_urls:
                visited_urls.append(document_url)

            tender_data = {
                'title': title,
                'description': reference_number,
                'closing_date': deadline_date,
                'source_url': document_url,
                'status': status,
                'format': format_type,
                'scraped_at': datetime.now().date(),
                'tender_type': "Kenya Treasury",
                'location': "Kenya"
            }
            tenders.append(tender_data)

            db_connection = ensure_db_connection()
            if not db_connection:
                logger.error("Database connection dropped. Skipping insertion.")
                set_task_state(scraping_task_id, {
                    "status": "error",
                    "startTime": start_time,
                    "tenders": tenders,
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
                    'message': "Database connection dropped during insertion"
                }, namespace='/scraping')
                return

            try:
                insert_tender_to_db(tender_data, db_connection)
                logger.info(f"Tender inserted into database: {title}")
                logger.info(f"Title: {title}\n"
                            f"Reference Number: {reference_number}\n"
                            f"Closing Date: {deadline_date}\n"
                            f"Status: {status}\n"
                            f"Source URL: {document_url}\n"
                            f"Format: {format_type}\n"
                            f"Tender Type: Kenya Treasury\n")
                logger.info("=" * 40)

                # Emit an update with the current state
                set_task_state(scraping_task_id, {
                    "status": "running",
                    "startTime": start_time,
                    "tenders": tenders,
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
                    'tenders': tenders,
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

        # Emit completion event
        set_task_state(scraping_task_id, {
            "status": "complete",
            "startTime": start_time,
            "tenders": tenders,
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
            'tenders': tenders,
            'visitedUrls': visited_urls,
            'totalUrls': len(visited_urls),
            'summary': {
                "urlsVisited": len(visited_urls),
                "timeTaken": time_taken,
                "openTenders": open_tenders,
                "closedTenders": closed_tenders,
                "totalTenders": len(tenders)
            },
            'message': "Completed scraping Kenya Treasury tenders"
        }, namespace='/scraping')

        logger.info("Scraping completed.")

    except Exception as e:
        logger.error(f"An error occurred while scraping: {e}")
        set_task_state(scraping_task_id, {
            "status": "error",
            "startTime": start_time,
            "tenders": tenders,
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
            'message': f"Error scraping Kenya Treasury tenders: {str(e)}"
        }, namespace='/scraping')
    finally:
        if db_connection:
            db_connection.close()

if __name__ == "__main__":
    treasury_ke_tenders()