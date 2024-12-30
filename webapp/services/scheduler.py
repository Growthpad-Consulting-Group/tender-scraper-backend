from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
import logging
from datetime import datetime  # Ensure this import exists
from webapp.config import get_db_connection
from webapp.scrapers.ungm_tenders import scrape_ungm_tenders
from webapp.scrapers.undp_tenders import scrape_undp_tenders
from webapp.scrapers.reliefweb_tenders import fetch_reliefweb_tenders
from webapp.scrapers.scrape_jobinrwanda_tenders import scrape_jobinrwanda_tenders
from webapp.scrapers.scrape_treasury_ke_tenders import scrape_treasury_ke_tenders
from webapp.scrapers.website_scraper import scrape_tenders_from_websites
from webapp.scrapers.query_scraper import scrape_tenders_from_query

def job_listener(event):
    if event.exception:
        logging.error('Job %s failed: %s', event.job_id, event.exception)
    else:
        logging.info('Job %s completed successfully.', event.job_id)

# Initialize APScheduler
scheduler = BackgroundScheduler()
scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

def get_scraping_function(tender_type):
    mapping = {
        'UNGM Tenders': scrape_ungm_tenders,
        'ReliefWeb Jobs': fetch_reliefweb_tenders,
        'Job in Rwanda': scrape_jobinrwanda_tenders,
        'Kenya Treasury': scrape_treasury_ke_tenders,
        'UNDP': scrape_undp_tenders,
        'Website Tenders': scrape_tenders_from_websites,
        'Query Tenders': scrape_tenders_from_query
    }
    return mapping.get(tender_type)  # Return scraping function or None if not found

def load_scheduled_tasks():
    logging.info("Loading scheduled tasks from the database...")
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT task_id, user_id, frequency, start_time, tender_type 
            FROM scheduled_tasks 
            WHERE is_enabled = TRUE
        """)
        tasks = cur.fetchall()

        for task in tasks:
            task_id, user_id, frequency, start_time, tender_type = task

            # Fetch search terms for the current task
            cur.execute("SELECT term FROM task_search_terms WHERE task_id = %s", (task_id,))
            search_terms = [row[0] for row in cur.fetchall()]

            logging.info(f"Fetched search terms for task_id {task_id}: {search_terms}")

            if not search_terms:
                logging.warning(f'No search terms found for task_id {task_id}; skipping this task.')
                continue

            scraping_function = get_scraping_function(tender_type)

            if scraping_function:
                current_time = datetime.now()
                if current_time < start_time:
                    logging.info(f"Delaying scheduling of task ID {task_id} until {start_time}.")
                    delay = (start_time - current_time).total_seconds()
                    scheduler.add_job(schedule_task_scrape, 'date', run_date=start_time, args=[user_id, task_id, scraping_function, frequency, search_terms])
                else:
                    schedule_task_scrape(user_id, task_id, scraping_function, frequency, search_terms)
            else:
                logging.warning(f"No scraping function found for tender type: {tender_type}")

    except Exception as e:
        logging.error(f"Error loading scheduled tasks: {str(e)}")
    finally:
        cur.close()
        conn.close()


def schedule_task_scrape(user_id, task_id, job_function, frequency, search_terms):
    job_id = f"user_{user_id}_task_{task_id}"

    # Remove existing job if it exists
    existing_job = scheduler.get_job(job_id)
    if existing_job:
        scheduler.remove_job(job_id)

    if not search_terms:
        logging.warning(f'Cannot schedule job {job_id}; no search terms provided.')
        return

    # Schedule job based on their frequency
    trigger = 'interval'
    trigger_args = {
        'Hourly': {'hours': 1},
        'Every 3 Hours': {'hours': 3},
        'Daily': {'days': 1},
        'Every 12 Hours': {'hours': 12},
        'Weekly': {'weeks': 1},
        'Monthly': {'days': 30}
    }

    # Prepare the job wrapper with search terms check
    def job_wrapper():
        if not search_terms:
            logging.error(f"Job {job_id} has no search terms; cannot execute.")
            return
        logging.info(f"Executing job for task ID {task_id} with search terms: {search_terms}")
        job_function(selected_engines=None, time_frame=None, file_type=None, terms=search_terms)

    # Schedule the job based on the frequency
    if frequency in trigger_args:
        scheduler.add_job(job_wrapper, trigger, id=job_id, **trigger_args[frequency])
        logging.info(f'Scheduled job: {job_id} with terms: {search_terms}')
    else:
        logging.warning(f'Unsupported frequency found while scheduling job {job_id}.')

def start_scheduler():
    # Load existing scheduled tasks from the database
    load_scheduled_tasks()

    # Start the scheduler
    scheduler.start()

def shutdown_scheduler():
    scheduler.shutdown()