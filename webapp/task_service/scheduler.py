import logging
import uuid
from datetime import datetime
from webapp.config import get_db_connection, close_db_connection
from .utils import set_task_state, get_search_terms
from .notifications import add_notification
from .constants import TRIGGER_ARGS
from .exceptions import InvalidConfigurationError, UnsupportedFrequencyError

logger = logging.getLogger(__name__)

def generate_job_id(user_id, task_id):
    """
    Generate a unique job ID for a scheduled task.
    
    Args:
        user_id (str): The ID of the user.
        task_id (int): The ID of the task.
    
    Returns:
        str: The job ID.
    """
    return f"user_{user_id}_task_{task_id}"

def schedule_task_scrape(scheduler, socketio, user_id, task_id, job_function, frequency, tender_type=None, search_terms=None, search_engines=None):
    """
    Schedule a scraping task with APScheduler.
    
    Args:
        scheduler: The APScheduler instance.
        socketio: The Socket.IO instance.
        user_id (str): The ID of the user.
        task_id (int): The ID of the task.
        job_function (callable): The scraping function to schedule.
        frequency (str): The frequency of the task.
        tender_type (str, optional): The type of tender.
        search_terms (list, optional): List of search terms.
        search_engines (list, optional): List of search engines.
    """
    job_id = generate_job_id(user_id, task_id)
    existing_job = scheduler.get_job(job_id)
    if existing_job:
        scheduler.remove_job(job_id)

    if frequency not in TRIGGER_ARGS:
        logger.warning(f'Unsupported frequency: {frequency}')
        raise UnsupportedFrequencyError(f"Unsupported frequency: {frequency}")

    if tender_type == 'Search Query Tenders' and (search_terms is None or search_engines is None):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type, last_run, search_engines
                FROM scheduled_tasks
                WHERE task_id = %s AND user_id = %s
            """, (task_id, user_id))
            task = cur.fetchone()

            if not task:
                logger.error(f"Task {task_id} not found for user {user_id}")
                return

            # Call get_search_terms with the cursor explicitly
            cur.execute("SELECT term FROM task_search_terms WHERE task_id = %s", (task_id,))
            db_search_terms = [row[0] for row in cur.fetchall()]
            search_terms = search_terms if search_terms is not None else db_search_terms
            search_engines = search_engines if search_engines is not None else (task[9].split(',') if task[9] else [])

        finally:
            cur.close()
            close_db_connection(conn)

    if tender_type == 'Search Query Tenders' and job_function.__name__ == 'scrape_tenders_from_query':
        if not search_terms or not search_engines:
            logger.warning(f"Cannot schedule Search Query Tenders task {task_id}: Missing search terms or engines")
            raise InvalidConfigurationError("Missing search terms or engines for Search Query Tenders")

        def job_wrapper():
            scraping_task_id = str(uuid.uuid4())
            start_time = datetime.now().isoformat()
            db_connection = get_db_connection()
            try:
                query = ' '.join(search_terms)
                logger.info(f"Running scheduled Search Query Tenders task {task_id} (scraping_task_id: {scraping_task_id}) with query: {query}, engines: {search_engines}")
                set_task_state(scraping_task_id, {
                    "status": "running",
                    "startTime": start_time,
                    "cancel": False,
                    "tenders": [],
                    "visited_urls": [],
                    "total_urls": 0,
                    "summary": {}
                })
                socketio.emit('scrape_update', {
                    'taskId': scraping_task_id,
                    'status': 'running',
                    'startTime': start_time
                }, namespace='/scraping')
                job_function(db_connection, query, search_engines, scraping_task_id)
            except Exception as e:
                logger.error(f"Error in scheduled task {task_id} (scraping_task_id: {scraping_task_id}): {str(e)}")
                socketio.emit('scrape_update', {
                    'taskId': scraping_task_id,
                    'status': 'error',
                    'startTime': start_time
                }, namespace='/scraping')
                add_notification(user_id, f"Scheduled task '{task_id}' failed to run: {str(e)}")
            finally:
                close_db_connection(db_connection)

        scheduler.add_job(job_wrapper, 'interval', id=job_id, **TRIGGER_ARGS[frequency])
        logger.info(f'Scheduled Search Query Tenders job: {job_id} with query: {" ".join(search_terms)}')
    else:
        scheduler.add_job(job_function, 'interval', id=job_id, **TRIGGER_ARGS[frequency])
        logger.info(f'Scheduled job: {job_id}')

def job_listener(event):
    """
    Listener for APScheduler job events.
    
    Args:
        event: The APScheduler event.
    """
    if event.exception:
        logger.error('Job %s failed: %s', event.job_id, event.exception)
        user_id = event.job_id.split('_')[1]
        task_id = event.job_id.split('_')[3]
        add_notification(user_id, f"Scheduled job for task '{task_id}' failed: {str(event.exception)}")
    else:
        logger.info('Job %s completed successfully.', event.job_id)

def setup_scheduler(scheduler):
    """
    Set up recurring scheduler jobs.
    
    Args:
        scheduler: The APScheduler instance.
    """
    from webapp.services.delete_expired_tenders import delete_expired_tenders
    scheduler.add_job(
        delete_expired_tenders,
        trigger='cron',
        hour=0,
        minute=0,
        id='delete_expired_tenders',
        replace_existing=True
    )