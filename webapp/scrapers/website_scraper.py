from webapp.config import get_db_connection
from datetime import datetime
from webapp.services.log import ScrapingLog
from webapp.scrapers.scraper_status import scraping_status
import logging
import uuid
from webapp.task_service.utils import set_task_state  # Updated import
from webapp.extensions import socketio  # Correct import for socketio
# from webapp.scrapers.scraper import scrape_tenders

def fetch_urls_and_terms(db_connection):
    """
    Retrieves URLs and search terms from the database.
    Args:
        db_connection: The active database connection object.
    Returns:
        tuple: A tuple containing a list of URLs and a list of search terms.
    """
    try:
        with db_connection.cursor() as cur:
            cur.execute("SELECT url FROM websites")
            urls = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT term FROM search_terms")
            search_terms = [row[0] for row in cur.fetchall()]
            ScrapingLog.add_log(f"Fetched URLs: {urls}")
            ScrapingLog.add_log(f"Fetched Search Terms: {search_terms}")
            return urls, search_terms
    except Exception as e:
        ScrapingLog.add_log(f"Error in fetch_urls_and_terms: {e}")
        return [], []

def scrape_tenders_from_websites(selected_engines=None, time_frame=None, file_type=None, terms=None, website=None, scraping_task_id=None):
    """
    Scrapes tenders from specified websites using search terms and stores results in the database.
    """
    db_connection = None
    global scraping_status
    scraping_status['tenders'] = []

    # Generate a scraping_task_id if not provided (for standalone execution)
    scraping_task_id = scraping_task_id or str(uuid.uuid4())
    start_time = datetime.now().isoformat()

    # Initialize task state
    set_task_state(scraping_task_id, {
        "status": "running",
        "startTime": start_time,
        "tenders": [],
        "visited_urls": [],
        "total_urls": 0,
        "summary": {}
    })
    socketio.emit('scrape_update', {
        'taskId': scraping_task_id,
        'status': 'running',
        'startTime': start_time,
        'tenders': [],
        'visitedUrls': [],
        'totalUrls': 0,
        'message': "Started scraping Website Tenders"
    }, namespace='/scraping')

    tenders = []
    open_tenders = 0
    closed_tenders = 0
    visited_urls = []

    try:
        terms = terms or []
        logging.info(f"Scraping function called with terms: {terms}")

        if not terms:
            ScrapingLog.add_log("Error: No search terms provided.")
            set_task_state(scraping_task_id, {
                "status": "error",
                "startTime": start_time,
                "tenders": [],
                "visited_urls": [],
                "total_urls": 0,
                "summary": {}
            })
            socketio.emit('scrape_update', {
                'taskId': scraping_task_id,
                'status': 'error',
                'startTime': start_time,
                'message': "No search terms provided"
            }, namespace='/scraping')
            return

        db_connection = get_db_connection()
        if not db_connection:
            ScrapingLog.add_log("Error: Failed to establish database connection.")
            set_task_state(scraping_task_id, {
                "status": "error",
                "startTime": start_time,
                "tenders": [],
                "visited_urls": [],
                "total_urls": 0,
                "summary": {}
            })
            socketio.emit('scrape_update', {
                'taskId': scraping_task_id,
                'status': 'error',
                'startTime': start_time,
                'message': "Failed to establish database connection"
            }, namespace='/scraping')
            return

        urls, search_terms = fetch_urls_and_terms(db_connection)
        if website:
            urls = [website]
            visited_urls.append(website)

        if not urls:
            ScrapingLog.add_log("Error: No URLs fetched from the database.")
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
                'message': "No URLs fetched from the database"
            }, namespace='/scraping')
            return

        current_year = datetime.now().year

        google_queries = [
            f'site:{url.split("//")[1].rstrip("/")} ' +
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&as_qdr={time_frame}" if time_frame != 'anytime' else '') +
            f"&as_eq=&as_nlo=&as_nhi=&lr=&" +
            (f"as_filetype={file_type}&" if file_type and file_type != 'any' else "") +
            f"as_occt=any&" +
            f"tbs="
            for url in urls
        ]

        bing_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&qft=+filterui:date:y" if time_frame == 'y' else '') +
            (f"&filter=all" if file_type and file_type != 'any' else '')
            # Removed region filter since 'region' parameter is not passed
        ]

        yahoo_queries = bing_queries

        duckduckgo_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&t=hg" if file_type and file_type != 'any' else '')
        ]

        ask_queries = [
            " OR ".join([f'"{term}"' for term in terms]) +
            (f" {current_year}" if time_frame == 'y' else '') +
            (f"&filetype={file_type}" if file_type and file_type != 'any' else '')
        ]

        all_queries = []
        if selected_engines:
            if 'Google' in selected_engines:
                all_queries.extend(google_queries)
            if 'Bing' in selected_engines:
                all_queries.extend(bing_queries)
            if 'Yahoo' in selected_engines:
                all_queries.extend(yahoo_queries)
            if 'DuckDuckGo' in selected_engines:
                all_queries.extend(duckduckgo_queries)
            if 'Ask' in selected_engines:
                all_queries.extend(ask_queries)
        else:
            ScrapingLog.add_log("Error: No search engines selected.")
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
                'message': "No search engines selected"
            }, namespace='/scraping')
            return

        ScrapingLog.clear_logs()
        ScrapingLog.add_log("Starting the scraping process.")

        for query in all_queries:
            ScrapingLog.add_log(f"Scraping for query: {query}")
            try:
                scraped_tenders = scrape_tenders(db_connection, query, selected_engines)
                if scraped_tenders is None:
                    ScrapingLog.add_log(f"No tenders returned for query: {query}")
                    continue

                for tender in scraped_tenders:
                    title = tender.get('title', 'Unknown')
                    source_url = tender.get('source_url', '')
                    status = tender.get('status', 'unknown').lower()
                    if source_url and source_url not in visited_urls:
                        visited_urls.append(source_url)
                    if status == "open":
                        open_tenders += 1
                    elif status == "closed":
                        closed_tenders += 1
                    tenders.append(tender)

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
                ScrapingLog.add_log(f"Error scraping for query {query}: {e}")

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
            'message': "Completed scraping Website Tenders"
        }, namespace='/scraping')

        scraping_status.update({
            'complete': True,
            'total_found': len(tenders),
            'relevant_count': sum(1 for t in tenders if t.get('is_relevant', 'No') == "Yes"),
            'irrelevant_count': sum(1 for t in tenders if t.get('is_relevant', 'No') == "No"),
            'open_count': open_tenders,
            'closed_count': closed_tenders
        })

    except Exception as e:
        ScrapingLog.add_log(f"An error occurred while scraping: {e}")
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
            'message': f"Error scraping Website Tenders: {str(e)}"
        }, namespace='/scraping')
    finally:
        if db_connection is not None:
            db_connection.close()
            ScrapingLog.add_log("Database connection closed.")

if __name__ == "__main__":
    scrape_tenders_from_websites()