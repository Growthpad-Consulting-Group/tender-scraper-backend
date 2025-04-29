from flask import Blueprint, request, jsonify, g
from flask_jwt_extended import create_access_token, JWTManager, jwt_required, get_jwt_identity
from webapp.config import get_db_connection, close_db_connection
from webapp.cache.redis_cache import get_cache, set_cache, delete_cache, redis_client
from webapp.services.email_notifications import notify_open_tenders
import logging
from datetime import datetime, timedelta
from dateutil import parser
from webapp.services.scheduler import scheduler
from webapp.scrapers.ungm_tenders import scrape_ungm_tenders
from webapp.scrapers.undp_tenders import scrape_undp_tenders
from webapp.scrapers.ppip_tenders import scrape_ppip_tenders
from webapp.scrapers.reliefweb_tenders import fetch_reliefweb_tenders
from webapp.scrapers.jobinrwanda_tenders import jobinrwanda_tenders
from webapp.scrapers.treasury_ke_tenders import treasury_ke_tenders
from webapp.services.delete_expired_tenders import delete_expired_tenders
from webapp.extensions import socketio
import json
import uuid
import threading
import re

task_service_bp = Blueprint('task_service', __name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def with_db_connection(func):
    """Decorator to handle database connection and cursor lifecycle."""
    def wrapper(*args, **kwargs):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            result = func(*args, **kwargs, conn=conn, cur=cur)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            close_db_connection(conn)
    return wrapper

def fetch_task_details(cur, task_id, user_id, fields="*"):
    """Fetch task details for a given task_id and user_id."""
    cur.execute(f"SELECT {fields} FROM scheduled_tasks WHERE task_id = %s AND user_id = %s", (task_id, user_id))
    task = cur.fetchone()
    if not task:
        logger.warning(f"Task {task_id} not found for user {user_id}")
        raise ValueError("Task not found or access denied.")
    return task

def format_task_response(task, search_terms=None, calculate_next=True):
    """Format a task response for API output."""
    task_dict = {
        "task_id": task[0],
        "name": task[1],
        "frequency": task[2],
        "start_time": task[3].isoformat() if task[3] else None,
        "end_time": task[4].isoformat() if task[4] else None,
        "priority": task[5],
        "is_enabled": task[6],
        "tender_type": task[7],
        "last_run": task[8].isoformat() if task[8] else None,
        "email_notifications_enabled": task[9] if len(task) > 9 else False,
        "sms_notifications_enabled": task[10] if len(task) > 10 else False,
        "slack_notifications_enabled": task[11] if len(task) > 11 else False,
        "custom_emails": task[12] if len(task) > 12 else "",
        "search_terms": search_terms if search_terms is not None else (task[13] if len(task) > 13 and task[13] else []),
        "engines": task[14].split(',') if len(task) > 14 and task[14] else [],
    }
    if calculate_next:
        task_dict["next_schedule"] = calculate_next_schedule(task[3], task[2], task[6])
    return task_dict

def add_notification(user_id, message):
    """Helper function to add a notification for a user."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO notifications (user_id, message, created_at, read) VALUES (%s, %s, %s, %s)",
            (user_id, message, datetime.now(), False)
        )
        conn.commit()
        logger.info(f"Notification added for user_id {user_id}: {message}")
    except Exception as e:
        logger.error(f"Error adding notification for user_id {user_id}: {str(e)}")
    finally:
        cur = conn.cursor()
        cur.close()
        close_db_connection(conn)

def set_task_state(task_id, state, expiry=3600):
    try:
        existing_state = get_task_state(task_id) or {}
        if "startTime" in existing_state and "startTime" not in state:
            state["startTime"] = existing_state["startTime"]
        redis_client.setex(f"scraping_task:{task_id}", expiry, json.dumps(state))
    except Exception as e:
        logger.error(f"Error setting task state in Redis for task_id {task_id}: {str(e)}")

def get_task_state(task_id):
    try:
        state = redis_client.get(f"scraping_task:{task_id}")
        return json.loads(state) if state else None
    except Exception as e:
        logger.error(f"Error getting task state from Redis for task_id {task_id}: {str(e)}")
        return None

def delete_task_state(task_id):
    try:
        redis_client.delete(f"scraping_task:{task_id}")
    except Exception as e:
        logger.error(f"Error deleting task state from Redis for task_id {task_id}: {str(e)}")

def get_search_terms(cur, task_id):
    cur.execute("SELECT term FROM task_search_terms WHERE task_id = %s", (task_id,))
    return [row[0] for row in cur.fetchall()]

def calculate_next_schedule(start_time, frequency, is_enabled):
    logger.info(f"Calculating next schedule: start_time={start_time}, frequency={frequency}, is_enabled={is_enabled}")
    
    if not is_enabled or not start_time:
        logger.info(f"Returning 'N/A' because is_enabled={is_enabled}, start_time={start_time}")
        return "N/A"

    now = datetime.now()

    if isinstance(start_time, str):
        try:
            start = parser.parse(start_time)
        except ValueError as e:
            logger.error(f"Failed to parse start_time '{start_time}': {str(e)}")
            return "N/A"
    else:
        start = start_time

    frequency = frequency.strip().title()
    logger.info(f"Normalized frequency: '{frequency}'")

    intervals = {
        'Hourly': timedelta(hours=1),
        'Every 3 Hours': timedelta(hours=3),
        'Daily': timedelta(days=1),
        'Every 12 Hours': timedelta(hours=12),
        'Weekly': timedelta(weeks=1),
        'Monthly': timedelta(days=30)
    }

    interval = intervals.get(frequency)
    if not interval:
        logger.warning(f"Frequency '{frequency}' not found in intervals. Returning 'N/A'")
        return "N/A"

    if start > now:
        return start.isoformat()

    time_diff = now - start
    intervals_passed = int(time_diff.total_seconds() // interval.total_seconds()) + 1
    next_schedule = start + (interval * intervals_passed)

    while next_schedule < now:
        next_schedule += interval

    logger.info(f"Calculated next_schedule: {next_schedule.isoformat()}")
    return next_schedule.isoformat()

def generate_job_id(user_id, task_id):
    return f"user_{user_id}_task_{task_id}"

def schedule_task_scrape(user_id, task_id, job_function, frequency, tender_type=None, search_terms=None, search_engines=None):
    from webapp.scrapers.run_query_scraper import scrape_tenders_from_query

    job_id = f"user_{user_id}_task_{task_id}"
    existing_job = scheduler.get_job(job_id)
    if existing_job:
        scheduler.remove_job(job_id)

    trigger = 'interval'
    trigger_args = {
        'Hourly': {'hours': 1},
        'Every 3 Hours': {'hours': 3},
        'Daily': {'days': 1},
        'Every 12 Hours': {'hours': 12},
        'Weekly': {'weeks': 1},
        'Monthly': {'days': 30}
    }

    if frequency not in trigger_args:
        logger.warning(f'Unsupported frequency: {frequency}')
        return

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

            cur.execute("SELECT term FROM task_search_terms WHERE task_id = %s", (task_id,))
            db_search_terms = [row[0] for row in cur.fetchall()]

            search_terms = search_terms if search_terms is not None else db_search_terms
            search_engines = search_engines if search_engines is not None else (task[9].split(',') if task[9] else [])

        except Exception as e:
            logger.error(f"Error fetching task data for task_id {task_id}: {str(e)}")
            return
        finally:
            cur.close()
            close_db_connection(conn)

    if tender_type == 'Search Query Tenders' and job_function == scrape_tenders_from_query:
        if not search_terms or not search_engines:
            logger.warning(f"Cannot schedule Search Query Tenders task {task_id}: Missing search terms or engines")
            return

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
                scrape_tenders_from_query(db_connection, query, search_engines, scraping_task_id)
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

        scheduler.add_job(job_wrapper, trigger, id=job_id, **trigger_args[frequency])
        logger.info(f'Scheduled Search Query Tenders job: {job_id} with query: {" ".join(search_terms)}')
    else:
        scheduler.add_job(job_function, trigger, id=job_id, **trigger_args[frequency])
        logger.info(f'Scheduled job: {job_id}')

def log_task_event(task_id, user_id, log_message, conn=None):
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True

    cur = conn.cursor()
    try:
        created_at = datetime.now().isoformat()
        cur.execute("INSERT INTO task_logs (task_id, user_id, log_entry, created_at) VALUES (%s, %s, %s, %s)",
                    (task_id, user_id, log_message, created_at))
        conn.commit()

        delete_cache(f"task_logs:user:{user_id}:task:{task_id}")
        delete_cache(f"all_task_logs:user:{user_id}")
    finally:
        cur.close()
        if close_conn:
            close_db_connection(conn)

def get_scraping_function(tender_type):
    mapping = {
        'UNGM Tenders': scrape_ungm_tenders,
        'ReliefWeb Jobs': fetch_reliefweb_tenders,
        'Job in Rwanda': jobinrwanda_tenders,
        'Kenya Treasury': treasury_ke_tenders,
        'UNDP': scrape_undp_tenders,
        'PPIP': scrape_ppip_tenders,
        'Search Query Tenders': None
    }
    return mapping.get(tender_type)

def job_listener(event):
    if event.exception:
        logger.error('Job %s failed: %s', event.job_id, event.exception)
        user_id = event.job_id.split('_')[1]  # Extract user_id from job_id (format: user_{user_id}_task_{task_id})
        task_id = event.job_id.split('_')[3]
        add_notification(user_id, f"Scheduled job for task '{task_id}' failed: {str(event.exception)}")
    else:
        logger.info('Job %s completed successfully.', event.job_id)

# --- Socket.IO Event Handlers ---

@socketio.on('join_task', namespace='/scraping')
def handle_join_task(data):
    task_id = data.get('taskId')
    logger.info(f"Received join_task event for task_id: {task_id}")
    
    task_state = get_task_state(task_id)
    if task_state:
        status = task_state.get('status', 'idle')
        tenders = task_state.get('tenders', [])
        visited_urls = task_state.get('visited_urls', [])
        total_urls = task_state.get('total_urls', 0)
        summary = task_state.get('summary', {})
        start_time = task_state.get('startTime', None)
        logger.info(f"Task {task_id} found in Redis: status={status}, startTime={start_time}")
        socketio.emit('scrape_update', {
            'taskId': task_id,
            'status': status,
            'tenders': tenders,
            'visitedUrls': visited_urls,
            'totalUrls': total_urls,
            'summary': summary,
            'startTime': start_time
        }, namespace='/scraping')
    else:
        logger.info(f"Task {task_id} not found in Redis, emitting idle status")
        socketio.emit('scrape_update', {
            'taskId': task_id,
            'status': 'idle',
            'startTime': None
        }, namespace='/scraping')

# --- API Endpoints ---





@task_service_bp.route('/api/add-task', methods=['POST'])
@jwt_required()
@with_db_connection
def add_task(conn, cur):
    current_user = get_jwt_identity()
    
    logger.info(f"Raw request headers: {request.headers}")
    logger.info(f"Raw request body: {request.get_data(as_text=True)}")

    try:
        data = request.get_json()
    except Exception as e:
        logger.error(f"Failed to parse JSON: {str(e)}")
        return jsonify({"msg": "Invalid JSON payload. Ensure Content-Type is application/json and the body is valid JSON."}), 400

    if data is None:
        logger.error("Request body is empty or not JSON.")
        return jsonify({"msg": "Request body must be valid JSON."}), 400

    logger.info(f"Parsed request data: {data}")

    name = data.get('name')
    frequency = data.get('frequency', 'Daily')
    priority = data.get('priority', 'Medium')
    tender_type = data.get('tenderType', 'All')
    selected_terms = data.get('search_terms', [])
    search_engines = data.get('engines', [])
    time_frame = data.get('timeFrame', None)
    file_type = data.get('fileType', None)
    selected_region = data.get('selectedRegion', None)

    if not name:
        return jsonify({"msg": "Task name is required."}), 400

    if tender_type == 'Search Query Tenders':
        if not selected_terms:
            return jsonify({"msg": "Search terms are required for Search Query Tenders."}), 400
        if not search_engines:
            return jsonify({"msg": "Search engines are required for Search Query Tenders."}), 400

    current_time = datetime.now()
    start_time = current_time
    end_time = start_time + timedelta(days=365)

    try:
        cur.execute("""
            INSERT INTO scheduled_tasks (
                user_id, name, frequency, start_time, end_time, 
                priority, is_enabled, tender_type, 
                search_engines, time_frame, file_type, selected_region
            )
            VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, 
                %s, %s, %s, %s
            )
            RETURNING task_id;
        """, (
            current_user, name, frequency, start_time, end_time,
            priority, True, tender_type,
            ','.join(search_engines), time_frame, file_type, selected_region))

        task_id = cur.fetchone()[0]
        logger.info(f"Inserted new task with task_id: {task_id}")

        if selected_terms:
            for term in selected_terms:
                try:
                    cur.execute("INSERT INTO task_search_terms (task_id, term) VALUES (%s, %s)", (task_id, term))
                except Exception as e:
                    logger.error(f"Error inserting term '{term}' for task_id {task_id}: {str(e)}")

        task = fetch_task_details(cur, task_id, current_user)
        search_terms = get_search_terms(cur, task_id)
        task_dict = format_task_response(task, search_terms)

        scraping_function = get_scraping_function(tender_type)
        if scraping_function:
            schedule_task_scrape(
                current_user,
                task_id,
                scraping_function,
                frequency,
                tender_type=tender_type,
                search_terms=search_terms,
                search_engines=task_dict["search_engines"]
            )

        log_task_event(task_id, current_user, f'Task "{name}" added successfully.', conn=conn)
        add_notification(current_user, f"Task '{name}' created successfully.")

        delete_cache(f"scraping_tasks:user:{current_user}")

        response = {
            "msg": "Task added successfully.",
            "task_id": task_id,
            "task": task_dict
        }
        logger.info(f"Sending response: {response}")
        return jsonify(response), 201
    except Exception as e:
        logger.error(f"Error adding task: {str(e)}")
        return jsonify({"msg": f"Error adding task: {str(e)}"}), 500

@task_service_bp.route('/api/run-scheduled-task/<int:task_id>', methods=['POST'])
@jwt_required()
@with_db_connection
def run_scheduled_task(task_id, conn, cur):
    current_user = get_jwt_identity()
    logger.info(f"User {current_user} is attempting to manually run task ID {task_id}.")

    try:
        task = fetch_task_details(cur, task_id, current_user, """
            task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type, last_run, 
            search_engines, time_frame, file_type, selected_region, email_notifications_enabled
        """)

        tender_type = task[7]
        search_engines = task[9].split(',') if task[9] else []
        time_frame = task[10]
        file_type = task[11]
        selected_region = task[12]
        email_notifications_enabled = task[13]

        search_terms = get_search_terms(cur, task_id)
        scraping_function = get_scraping_function(tender_type)

        if not scraping_function and tender_type != 'Search Query Tenders':
            logger.warning(f"No scraping function found for tender type: {tender_type}")
            return jsonify({"msg": "Manual run not supported for this tender type."}), 400

        scraping_task_id = str(uuid.uuid4())
        start_time = datetime.now().isoformat()
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
            'startTime': start_time,
            'message': f"Started scraping for task: {task[1]}"
        }, namespace='/scraping')

        def run_scraping_task():
            db_connection = get_db_connection()
            tenders = []
            try:
                if tender_type == 'Search Query Tenders':
                    from webapp.scrapers.run_query_scraper import scrape_tenders_from_query
                    query = ' '.join(search_terms) if search_terms else ''
                    if not query or not search_engines:
                        logger.warning(f"Cannot run Search Query Tenders task {task_id}: Missing search terms or engines")
                        socketio.emit('scrape_update', {
                            'taskId': scraping_task_id,
                            'status': 'error',
                            'startTime': start_time,
                            'message': "Missing search terms or engines."
                        }, namespace='/scraping')
                        add_notification(
                            current_user,
                            f"Task '{task[1]}' failed to run: Missing search terms or engines."
                        )
                        return
                    logger.info(f"Starting manual scraping task for task_id {task_id} with scraping_task_id: {scraping_task_id}, query: {query}, engines: {search_engines}")
                    tenders = scrape_tenders_from_query(db_connection, query, search_engines, scraping_task_id)
                else:
                    logger.info(f"Starting manual scraping task for task_id {task_id} with scraping_task_id: {scraping_task_id}, tender_type: {tender_type}")
                    if scraping_function in [scrape_ungm_tenders, fetch_reliefweb_tenders, jobinrwanda_tenders,
                                            treasury_ke_tenders, scrape_undp_tenders, scrape_ppip_tenders]:
                        scraping_function(
                            scraping_task_id=scraping_task_id,
                            set_task_state=set_task_state,
                            socketio=socketio
                        )
                        task_state = get_task_state(scraping_task_id)
                        tenders = task_state.get("tenders", []) if task_state else []
                    else:
                        scraping_function(
                            selected_engines=search_engines,
                            time_frame=time_frame,
                            file_type=file_type,
                            region=selected_region,
                            terms=search_terms
                        )

                if email_notifications_enabled and tenders:
                    logger.info(f"Email notifications enabled for task {task_id}. Checking for open tenders...")
                    notify_open_tenders(tenders, task_id, recipient_email="kwamevaughan@gmail.com")
                    open_tenders_count = len([t for t in tenders if t.get('status') == 'open'])
                    if open_tenders_count > 0:
                        add_notification(
                            current_user,
                            f"Task '{task[1]}' found {open_tenders_count} new open tender(s)."
                        )

            except Exception as e:
                logger.error(f"Error in background scraping task for task_id {task_id} (scraping_task_id: {scraping_task_id}): {str(e)}")
                socketio.emit('scrape_update', {
                    'taskId': scraping_task_id,
                    'status': 'error',
                    'startTime': start_time,
                    'message': f"Error: {str(e)}"
                }, namespace='/scraping')
                add_notification(
                    current_user,
                    f"Task '{task[1]}' failed to run: {str(e)}"
                )
            finally:
                close_db_connection(db_connection)

        threading.Thread(target=run_scraping_task, daemon=True).start()

        cur.execute("UPDATE scheduled_tasks SET last_run = %s WHERE task_id = %s", (datetime.now(), task_id))
        log_task_event(task_id, current_user, f'Task "{task[1]}" manually started with scraping_task_id {scraping_task_id}.', conn=conn)
        return jsonify({
            "msg": "Task started successfully.",
            "scraping_task_id": scraping_task_id
        }), 200

    except ValueError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error running task {task_id}: {str(e)}")
        return jsonify({"msg": "Error running task."}), 500

@task_service_bp.route('/api/run-task/<int:task_id>', methods=['POST'])
@jwt_required()
@with_db_connection
def run_task(task_id, conn, cur):
    from webapp.scrapers.run_query_scraper import scrape_tenders_from_query

    current_user = get_jwt_identity()
    logger.info(f"User {current_user} requested to run task ID {task_id}")

    try:
        task = fetch_task_details(cur, task_id, current_user, """
            user_id, name, tender_type, frequency, search_engines, time_frame, file_type, selected_region, email_notifications_enabled
        """)

        search_terms = get_search_terms(cur, task_id)
        selected_engines = task[4].split(',') if task[4] else []
        time_frame = task[5]
        file_type = task[6]
        selected_region = task[7]
        email_notifications_enabled = task[8]

        scraping_function = get_scraping_function(task[2])
        if scraping_function:
            try:
                logger.info(f"Running task '{task[1]}' with search terms: {search_terms}.")
                tenders = []
                if scraping_function in [scrape_ungm_tenders, fetch_reliefweb_tenders, jobinrwanda_tenders,
                                        treasury_ke_tenders, scrape_undp_tenders, scrape_ppip_tenders]:
                    scraping_function()
                elif scraping_function == scrape_tenders_from_query or task[2] == 'Search Query Tenders':
                    query = ' '.join(search_terms) if search_terms else ''
                    if not query:
                        add_notification(current_user, f"Task '{task[1]}' failed to run: No search terms provided.")
                        return jsonify({"msg": "No search terms provided for Search Query Tenders."}), 400
                    tenders = scrape_tenders_from_query(conn, query, selected_engines, task_id)
                else:
                    scraping_function(selected_engines=selected_engines, time_frame=time_frame, file_type=file_type, region=selected_region, terms=search_terms)

                if email_notifications_enabled and tenders:
                    logger.info(f"Email notifications enabled for task {task_id}. Checking for open tenders...")
                    notify_open_tenders(tenders, task_id, recipient_email="kwamevaughan@gmail.com")
                    open_tenders_count = len([t for t in tenders if t.get('status') == 'open'])
                    if open_tenders_count > 0:
                        add_notification(
                            current_user,
                            f"Task '{task[1]}' found {open_tenders_count} new open tender(s)."
                        )

            except Exception as e:
                logger.error(f"Error running task '{task[1]}': {str(e)}")
                add_notification(current_user, f"Task '{task[1]}' failed to run: {str(e)}")
                return jsonify({"msg": f"Error running task: {str(e)}"}), 500

            cur.execute("UPDATE scheduled_tasks SET last_run = %s WHERE task_id = %s", (datetime.now(), task_id))

            scraping_function = get_scraping_function(task[2])
            schedule_task_scrape(
                current_user,
                task_id,
                scraping_function if scraping_function else scrape_tenders_from_query,
                task[3],
                tender_type=task[2],
                search_terms=search_terms,
                search_engines=selected_engines
            )

        log_task_event(task_id, current_user, f"Task '{task[1]}' has been executed.", conn=conn)
        delete_cache(f"scraping_tasks:user:{current_user}")
        return jsonify({"msg": f"Task '{task[1]}' has been executed."}), 200
    except ValueError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error running task {task_id}: {str(e)}")
        return jsonify({"msg": "Error running task."}), 500

@task_service_bp.route('/api/clear-logs/<int:task_id>', methods=['DELETE'])
@jwt_required()
@with_db_connection
def clear_logs(task_id, conn, cur):
    current_user = get_jwt_identity()
    logger.info(f"User {current_user} is attempting to clear logs for task ID {task_id}.")

    try:
        task = fetch_task_details(cur, task_id, current_user, "user_id, name")
        logger.info(f"Deleting logs for task ID {task_id} for user {current_user}.")

        cur.execute("DELETE FROM task_logs WHERE task_id = %s AND user_id = %s", (task_id, current_user))
        delete_cache(f"task_logs:user:{current_user}:task:{task_id}")
        delete_cache(f"all_task_logs:user:{current_user}")

        add_notification(current_user, f"Logs cleared for task '{task[1]}'.")
        logger.info(f"Logs cleared successfully for task ID {task_id}.")
        return jsonify({"msg": "Logs cleared successfully."}), 200
    except ValueError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error clearing logs for task {task_id}: {str(e)}")
        return jsonify({"msg": "Error clearing logs."}), 500

@task_service_bp.route('/api/task-logs/<int:task_id>', methods=['GET'])
@jwt_required()
@with_db_connection
def get_task_logs(task_id, conn, cur):
    current_user = get_jwt_identity()

    cache_key = f"task_logs:user:{current_user}:task:{task_id}"
    cached_logs = get_cache(cache_key)
    if cached_logs is not None:
        return jsonify({"logs": cached_logs}), 200

    try:
        cur.execute("SELECT log_entry, created_at FROM task_logs WHERE task_id = %s AND user_id = %s",
                    (task_id, current_user))
        logs = cur.fetchall()

        if not logs:
            return jsonify({"msg": "No logs found for this task."}), 404

        logs_list = [{"log_entry": log[0], "created_at": log[1].isoformat() if log[1] else None} for log in logs]
        set_cache(cache_key, logs_list, expiry=60)
        return jsonify({"logs": logs_list}), 200
    except Exception as e:
        logger.error(f"Error fetching logs for task {task_id}: {str(e)}")
        return jsonify({"msg": "Error fetching logs."}), 500

@task_service_bp.route('/api/all-task-logs', methods=['GET'])
@jwt_required()
@with_db_connection
def get_all_task_logs(conn, cur):
    current_user = get_jwt_identity()

    cache_key = f"all_task_logs:user:{current_user}"
    cached_logs = get_cache(cache_key)
    if cached_logs is not None:
        return jsonify({"logs": cached_logs}), 200

    try:
        cur.execute("SELECT task_id, log_entry, created_at FROM task_logs WHERE user_id = %s", (current_user,))
        logs = cur.fetchall()

        if not logs:
            return jsonify({"msg": "No logs found for this user."}), 404

        logs_list = [{"task_id": log[0], "log_entry": log[1], "created_at": log[2].isoformat() if log[2] else None} for log in logs]
        set_cache(cache_key, logs_list, expiry=60)
        return jsonify({"logs": logs_list}), 200
    except Exception as e:
        logger.error(f"Error fetching all logs: {str(e)}")
        return jsonify({"msg": "Error fetching logs."}), 500

@task_service_bp.route('/api/cancel-task/<int:task_id>', methods=['DELETE'])
@jwt_required()
@with_db_connection
def cancel_task(task_id, conn, cur):
    current_user = get_jwt_identity()

    try:
        task = fetch_task_details(cur, task_id, current_user, "user_id, name")
        cur.execute("DELETE FROM task_search_terms WHERE task_id = %s", (task_id,))
        cur.execute("DELETE FROM scheduled_tasks WHERE task_id = %s", (task_id,))

        delete_cache(f"scraping_tasks:user:{current_user}")
        add_notification(current_user, f"Task '{task[1]}' canceled successfully.")
        return jsonify({"msg": "Task canceled successfully."}), 200
    except ValueError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error canceling task {task_id}: {str(e)}")
        return jsonify({"msg": "Error canceling task."}), 500

@task_service_bp.route('/api/toggle-task-status/<int:task_id>', methods=['PATCH'])
@jwt_required()
@with_db_connection
def toggle_task_status(task_id, conn, cur):
    current_user = get_jwt_identity()

    try:
        task = fetch_task_details(cur, task_id, current_user, "user_id, is_enabled, name")
        new_status = not task[1]
        cur.execute("UPDATE scheduled_tasks SET is_enabled = %s WHERE task_id = %s", (new_status, task_id))

        status_message = 'enabled' if new_status else 'disabled'
        log_task_event(task_id, current_user, f'Task "{task[2]}" has been {status_message} successfully.', conn=conn)
        add_notification(current_user, f"Task '{task[2]}' {status_message} successfully.")
        delete_cache(f"scraping_tasks:user:{current_user}")
        return jsonify({"msg": f"Task {task_id} {status_message} successfully."}), 200
    except ValueError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error toggling task status for task {task_id}: {str(e)}")
        return jsonify({"msg": "Error toggling task status."}), 500

@task_service_bp.route('/api/edit-task/<int:task_id>', methods=['PUT'])
@jwt_required()
@with_db_connection
def edit_task(task_id, conn, cur):
    current_user = get_jwt_identity()
    data = request.get_json()

    frequency = data.get('frequency', 'Daily')
    priority = data.get('priority', 'Medium')
    task_name = data.get('name')
    tender_type = data.get('tenderType', 'All')
    email_notifications_enabled = data.get('email_notifications_enabled', False)

    if data.get('startTime') and data.get('endTime'):
        try:
            start_time = parser.parse(data.get('startTime'))
            end_time = parser.parse(data.get('endTime'))
        except ValueError:
            return jsonify({"msg": "Invalid date format for start time or end time."}), 400
    else:
        current_time = datetime.now()
        if frequency == 'Hourly':
            start_time = current_time.replace(minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(hours=1)
        elif frequency == 'Every 3 Hours':
            start_time = current_time.replace(minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(hours=3)
        elif frequency == 'Every 12 Hours':
            start_time = current_time.replace(hour=current_time.hour // 12 * 12, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(hours=12)
        elif frequency == 'Daily':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
        elif frequency == 'Weekly':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(weeks=1)
        elif frequency == 'Monthly':
            start_time = current_time.replace(day=1, hour=10, minute=0, second=0, microsecond=0)
            if current_time.month == 12:
                start_time = start_time.replace(year=current_time.year + 1, month=1)
            else:
                start_time = start_time.replace(month=current_time.month + 1)
            end_time = start_time + timedelta(days=31)
        else:
            return jsonify({"msg": "Unsupported frequency provided."}), 400

    try:
        task = fetch_task_details(cur, task_id, current_user, "user_id, name, frequency, start_time, end_time, priority, tender_type, email_notifications_enabled")

        changes = []
        if task_name != task[1]:
            changes.append(f'Task name changed from "{task[1]}" to "{task_name}"')
        if frequency != task[2]:
            changes.append(f'Frequency changed from "{task[2]}" to "{frequency}"')
        if start_time != task[3]:
            changes.append(f'Start time changed from "{task[3]}" to "{start_time}"')
        if end_time != task[4]:
            changes.append(f'End time changed from "{task[4]}" to "{end_time}"')
        if priority != task[5]:
            changes.append(f'Priority changed from "{task[5]}" to "{priority}"')
        if tender_type != task[6]:
            changes.append(f'Tender type changed from "{task[6]}" to "{tender_type}"')
        if email_notifications_enabled != task[7]:
            changes.append(f'Email notifications enabled changed from "{task[7]}" to "{email_notifications_enabled}"')

        cur.execute("""
            UPDATE scheduled_tasks SET name = %s, frequency = %s, start_time = %s, end_time = %s, priority = %s, tender_type = %s, email_notifications_enabled = %s
            WHERE task_id = %s
        """, (task_name, frequency, start_time, end_time, priority, tender_type, email_notifications_enabled, task_id))

        log_message = ' and '.join(changes) if changes else 'Task updated with no changes.'
        log_task_event(task_id, current_user, log_message, conn=conn)
        add_notification(current_user, f"Task '{task_name}' updated: {log_message}")
        delete_cache(f"scraping_tasks:user:{current_user}")
        return jsonify({"msg": "Task edited successfully."}), 200
    except ValueError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error editing task {task_id}: {str(e)}")
        return jsonify({"msg": "Error editing task."}), 500

@task_service_bp.route('/api/next-schedule', methods=['GET'])
@jwt_required()
@with_db_connection
def get_next_schedule(conn, cur):
    current_user = get_jwt_identity()

    try:
        cur.execute("""
            SELECT start_time 
            FROM scheduled_tasks 
            WHERE user_id = %s AND is_enabled = TRUE 
            ORDER BY start_time ASC 
            LIMIT 1;
        """, (current_user,))
        result = cur.fetchone()

        return jsonify({"next_schedule": result[0].isoformat() if result and result[0] else "N/A"}), 200
    except Exception as e:
        logger.error(f"Error fetching next schedule: {str(e)}")
        return jsonify({"msg": "Error fetching next schedule."}), 500

# --- Scheduler Setup ---

scheduler.add_job(
    delete_expired_tenders,
    trigger='cron',
    hour=0,
    minute=0,
    id='delete_expired_tenders',
    replace_existing=True
)