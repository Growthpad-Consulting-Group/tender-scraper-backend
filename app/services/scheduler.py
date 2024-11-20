from apscheduler.schedulers.background import BackgroundScheduler
import logging
from datetime import datetime
from app.scrapers.scraper import scrape_tenders
from app.scrapers.ca_tenders import scrape_ca_tenders
from app.scrapers.undp_tenders import scrape_undp_tenders
from app.scrapers.reliefweb_tenders import fetch_reliefweb_tenders
from app.scrapers.scrape_jobinrwanda_tenders import scrape_jobinrwanda_tenders
from app.scrapers.scrape_treasury_ke_tenders import scrape_treasury_ke_tenders
from app.scrapers.website_scraper import scrape_tenders_from_websites

# Initialize APScheduler
scheduler = BackgroundScheduler()

# Function to run all scraping tasks
def run_all_scraping():
    logging.info("Running all scraping tasks at: %s", datetime.now())

    scraping_functions = [
        scrape_ca_tenders,
        fetch_reliefweb_tenders,
        scrape_jobinrwanda_tenders,
        scrape_treasury_ke_tenders,
        scrape_undp_tenders,
        scrape_tenders_from_websites,
        scrape_tenders,
    ]

    for scrape_func in scraping_functions:
        try:
            logging.info("Starting scrape for: %s", scrape_func.__name__)
            scrape_func()
            logging.info("%s completed successfully.", scrape_func.__name__)
        except Exception as e:
            logging.error("Error in %s: %s", scrape_func.__name__, str(e))

def start_scheduler():
    # Schedule the job to run every 24 hours
    scheduler.add_job(func=run_all_scraping, trigger="interval", hours=24)
    scheduler.start()

def shutdown_scheduler():
    scheduler.shutdown()