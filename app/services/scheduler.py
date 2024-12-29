from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED  # Import the event codes
import logging
from datetime import datetime
from app.scrapers.scraper import scrape_tenders
from app.scrapers.ungm_tenders import scrape_ungm_tenders
from app.scrapers.undp_tenders import scrape_undp_tenders
from app.scrapers.reliefweb_tenders import fetch_reliefweb_tenders
from app.scrapers.scrape_jobinrwanda_tenders import scrape_jobinrwanda_tenders
from app.scrapers.scrape_treasury_ke_tenders import scrape_treasury_ke_tenders
from app.scrapers.website_scraper import scrape_tenders_from_websites
from app.scrapers.query_scraper import scrape_tenders_from_query


# Function to log job events
def job_listener(event):
    if event.exception:
        logging.error('Job %s failed: %s', event.job_id, event.exception)
    else:
        logging.info('Job %s completed successfully.', event.job_id)

# Initialize APScheduler
scheduler = BackgroundScheduler()

# Register the job listener for job execution events
scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

# Function to run all scraping tasks
def run_all_scraping():
    logging.info("Running all scraping tasks at: %s", datetime.now())

    # Move socketio import here
    from app import socketio  # Lazy import to avoid circular imports

    scraping_functions = [
        scrape_ungm_tenders,
        fetch_reliefweb_tenders,
        scrape_jobinrwanda_tenders,
        scrape_treasury_ke_tenders,
        scrape_undp_tenders,
        scrape_tenders_from_websites,
        scrape_tenders_from_query,
    ]

    for scrape_func in scraping_functions:
        try:
            logging.info("Starting scrape for: %s", scrape_func.__name__)
            scrape_func()  # Execute the scraping function
            logging.info("%s completed successfully.", scrape_func.__name__)
        except Exception as e:
            logging.error("Error in %s: %s", scrape_func.__name__, str(e))

def start_scheduler():
    # Schedule the job to run every 24 hours
    scheduler.add_job(func=run_all_scraping, trigger="interval", hours=24)
    scheduler.start()

def shutdown_scheduler():
    scheduler.shutdown()