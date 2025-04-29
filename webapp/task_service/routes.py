import logging
import uuid
import threading
import re
from . import task_service_bp
from datetime import datetime, timedelta
from dateutil import parser
from flask import request, jsonify, g
from flask_jwt_extended import jwt_required, get_jwt_identity
from webapp.config import get_db_connection, close_db_connection
from webapp.scrapers.ungm_tenders import scrape_ungm_tenders
from webapp.scrapers.undp_tenders import scrape_undp_tenders
from webapp.scrapers.ppip_tenders import scrape_ppip_tenders
from webapp.scrapers.reliefweb_tenders import fetch_reliefweb_tenders
from webapp.scrapers.jobinrwanda_tenders import jobinrwanda_tenders
from webapp.scrapers.treasury_ke_tenders import treasury_ke_tenders
from webapp.services.email_notifications import notify_open_tenders
from webapp.extensions import socketio
from webapp.cache.redis_cache import get_cache, set_cache, delete_cache, redis_client
from .notifications import add_notification
from .scheduler import schedule_task_scrape, generate_job_id
from .constants import SCRAPING_FUNCTIONS
from .exceptions import TaskNotFoundError, InvalidConfigurationError
from .utils import format_task_response, fetch_task_details, get_search_terms, set_task_state, get_task_state, delete_task_state
from psycopg2.extras import Json
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()
DEFAULT_RECIPIENT_EMAIL = os.getenv("DEFAULT_RECIPIENT_EMAIL")


# --- Database Connection Management ---

@task_service_bp.before_request
def before_request():
    """
    Set up a database connection and cursor before each request.
    """
    g.conn = get_db_connection()
    g.cur = g.conn.cursor()
    logger.debug("Database connection opened for request.")

@task_service_bp.after_request
def after_request(response):
    """
    Commit the database transaction and close the connection after each request, unless already committed.
    
    Args:
        response: The response object.
    
    Returns:
        The response object.
    """
    # Skip commit for PATCH requests to /api/toggle-task-status to avoid redundant commits
    if request.method == 'PATCH' and request.path.startswith('/api/toggle-task-status'):
        logger.debug("Skipping commit in after_request for toggle-task-status endpoint.")
    else:
        try:
            g.conn.commit()
        except Exception as e:
            logger.error(f"Error committing database transaction: {str(e)}")
            g.conn.rollback()

    # Close cursor and connection
    if hasattr(g, 'cur'):
        g.cur.close()
    if hasattr(g, 'conn'):
        close_db_connection(g.conn)
    logger.debug("Database connection closed after request.")
    return response

@task_service_bp.teardown_request
def teardown_request(exception):
    """
    Ensure the database connection is closed in case of an exception.
    
    Args:
        exception: The exception that occurred, if any.
    """
    if hasattr(g, 'cur'):
        g.cur.close()
    if hasattr(g, 'conn'):
        close_db_connection(g.conn)
    logger.debug("Database connection closed during teardown.")

# --- Socket.IO Event Handlers ---

@socketio.on('join_task', namespace='/scraping')
def handle_join_task(data):
    """
    Handle Socket.IO 'join_task' event to provide scraping task updates.
    
    Args:
        data (dict): The event data containing the task ID.
    """
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

@task_service_bp.route('/api/scraping-tasks', methods=['GET'])
@jwt_required()
def get_scraping_tasks():
    current_user = get_jwt_identity()
    logger.info(f"Fetching tasks for user_id: {current_user}")

    cache_key = f"scraping_tasks:user:{current_user}"
    cached_tasks = get_cache(cache_key)
    if cached_tasks is not None:
        return jsonify({"tasks": cached_tasks}), 200

    try:
        # Use g.conn and g.cur instead of passing them
        g.cur.execute("""
            SELECT task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type, last_run, 
                   email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled, custom_emails, 
                   search_terms, engines
            FROM scheduled_tasks
            WHERE user_id = %s
        """, (current_user,))

        tasks = g.cur.fetchall()
        logger.info(f"Fetched tasks from DB: {tasks}")
        task_list = []
        for task in tasks:
            logger.info(f"Processing task {task[0]}: frequency={task[2]}, start_time={task[3]}, is_enabled={task[6]}")
            task_dict = format_task_response(task)
            task_list.append(task_dict)

        set_cache(cache_key, task_list, expiry=300)
        return jsonify({"tasks": task_list}), 200
    except Exception as e:
        logger.error(f"Error fetching tasks: {str(e)}")
        return jsonify({"msg": "Error fetching tasks."}), 500

@task_service_bp.route('/api/scraping-tasks', methods=['POST'])
@jwt_required()
def create_scraping_task():
    current_user = get_jwt_identity()
    logger.info(f"Creating task for user_id: {current_user}")
    try:
        data = request.get_json()
        logger.debug(f"Received data: {data}")
        
        task_name = data.get('name')
        frequency = data.get('frequency')
        tender_type = data.get('tender_type')
        priority = data.get('priority')
        start_time = data.get('start_time', datetime.utcnow().isoformat())
        end_time = data.get('end_time', (datetime.utcnow() + timedelta(days=365)).isoformat())
        email_notifications_enabled = data.get('email_notifications_enabled', False)
        sms_notifications_enabled = data.get('sms_notifications_enabled', False)
        slack_notifications_enabled = data.get('slack_notifications_enabled', False)
        custom_emails = data.get('custom_emails', '')
        search_terms = data.get('search_terms', [])
        engines = data.get('engines', [])

        if not task_name or not frequency or not tender_type:
            return jsonify({"msg": "Missing required fields"}), 400

        # Handle custom_emails: convert to string if it's a list
        if isinstance(custom_emails, list):
            custom_emails = ','.join([email.strip() for email in custom_emails if email.strip()])
        elif not isinstance(custom_emails, str):
            custom_emails = ''  # Fallback to empty string if invalid type

        # Validate emails if custom_emails is non-empty
        email_list = []
        if custom_emails:
            email_list = [email.strip() for email in custom_emails.split(",") if email.strip()]
            email_regex = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
            invalid_emails = [email for email in email_list if not re.match(email_regex, email)]
            if invalid_emails:
                return jsonify({"msg": f"Invalid email addresses: {', '.join(invalid_emails)}"}), 400

        # Execute the query
        logger.debug("Executing database insert query")
        g.cur.execute("""
            INSERT INTO scheduled_tasks (
                user_id, name, frequency, start_time, end_time, priority, tender_type,
                email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled,
                custom_emails, search_terms, engines
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING task_id, name, frequency, start_time, end_time, priority, tender_type,
                     email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled,
                     custom_emails, search_terms, engines
        """, (
            current_user, task_name, frequency, start_time, end_time, priority, tender_type,
            email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled,
            custom_emails, search_terms, engines
        ))
        
        task = g.cur.fetchone()
        logger.debug(f"Database returned: {task}")
        
        g.conn.commit()
        
        if task is None:
            logger.error("Database query returned no results")
            return jsonify({"msg": "Failed to create task. Database returned no results."}), 500
            
        task_response = {
            "task_id": task[0],
            "name": task[1],
            "frequency": task[2],
            "start_time": task[3],
            "end_time": task[4],
            "priority": task[5],
            "tender_type": task[6],
            "email_notifications_enabled": task[7],
            "sms_notifications_enabled": task[8],
            "slack_notifications_enabled": task[9],
            "custom_emails": task[10],
            "search_terms": task[11],
            "engines": task[12],
        }
        
        cache_key = f"scraping_tasks:user:{current_user}"
        delete_cache(cache_key)
        
        return jsonify({
            "msg": "Task added successfully.",
            "task": task_response
        }), 201
    except Exception as e:
        logger.error(f"Error creating task: {str(e)}")
        return jsonify({"msg": f"Error creating task: {str(e)}"}), 500


        task_response = format_task_response(task)

        # Clear cache
        cache_key = f"scraping_tasks:user:{current_user}"
        delete_cache(cache_key)

        return jsonify({
            "msg": "Task added successfully.",
            "task": task_response
        }), 201

    except Exception as e:
        logger.error(f"Error creating task: {str(e)}")
        return jsonify({"msg": "Error creating task."}), 500
    
    
@task_service_bp.route('/api/tasks', methods=['GET'], endpoint='get_tasks')
@jwt_required()
def get_tasks():
    """
    Fetch all scraping tasks for the authenticated user.
    
    Returns:
        JSON response with the list of tasks.
    """
    current_user = get_jwt_identity()
    logger.info(f"Fetching tasks for user_id: {current_user}")

    cache_key = f"scraping_tasks:user:{current_user}"
    cached_tasks = get_cache(cache_key)
    if cached_tasks is not None:
        return jsonify({"tasks": cached_tasks}), 200

    try:
        g.cur.execute("""
            SELECT task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type, last_run, 
                   email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled, custom_emails, 
                   search_terms, engines
            FROM scheduled_tasks
            WHERE user_id = %s
        """, (current_user,))

        tasks = g.cur.fetchall()
        logger.info(f"Fetched tasks from DB: {tasks}")
        task_list = [format_task_response(task) for task in tasks]

        set_cache(cache_key, task_list, expiry=300)
        return jsonify({"tasks": task_list}), 200
    except Exception as e:
        logger.error(f"Error fetching tasks: {str(e)}")
        return jsonify({"msg": "Error fetching tasks."}), 500

@task_service_bp.route('/api/tasks', methods=['POST'], endpoint='create_task')
@jwt_required()
def create_task():
    """
    Create a new scraping task for the authenticated user.
    
    Returns:
        JSON response with the created task details.
    """
    current_user = get_jwt_identity()
    logger.info(f"Creating task for user_id: {current_user}")

    try:
        data = request.get_json()
    except Exception as e:
        logger.error(f"Failed to parse JSON: {str(e)}")
        return jsonify({"msg": "Invalid JSON payload. Ensure Content-Type is application/json and the body is valid JSON."}), 400

    if data is None:
        logger.error("Request body is empty or not JSON.")
        return jsonify({"msg": "Request body must be valid JSON."}), 400

    name = data.get('name')
    frequency = data.get('frequency', 'Daily')
    priority = data.get('priority', 'Medium')
    tender_type = data.get('tenderType', 'All')
    search_terms = data.get('search_terms', [])
    engines = data.get('engines', [])
    time_frame = data.get('timeFrame', None)
    file_type = data.get('fileType', None)
    selected_region = data.get('selectedRegion', None)
    email_notifications_enabled = data.get('email_notifications_enabled', False)
    sms_notifications_enabled = data.get('sms_notifications_enabled', False)
    slack_notifications_enabled = data.get('slack_notifications_enabled', False)
    custom_emails = data.get('custom_emails', '')

    if not name:
        return jsonify({"msg": "Task name is required."}), 400

    if tender_type == 'Search Query Tenders':
        if not search_terms:
            return jsonify({"msg": "Search terms are required for Search Query Tenders."}), 400
        if not engines:
            return jsonify({"msg": "Search engines are required for Search Query Tenders."}), 400

    if custom_emails:
        email_list = [email.strip() for email in custom_emails.split(",")]
        email_regex = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
        invalid_emails = [email for email in email_list if not re.match(email_regex, email)]
        if invalid_emails:
            return jsonify({"msg": f"Invalid email addresses: {', '.join(invalid_emails)}"}), 400

    current_time = datetime.now()
    start_time = current_time
    end_time = start_time + timedelta(days=365)

    try:
        g.cur.execute("""
            INSERT INTO scheduled_tasks (
                user_id, name, frequency, start_time, end_time, 
                priority, is_enabled, tender_type, 
                search_engines, time_frame, file_type, selected_region,
                email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled, custom_emails
            )
            VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, 
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            RETURNING task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type, last_run, 
                      email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled, custom_emails, 
                      search_terms, engines;
        """, (
            current_user, name, frequency, start_time, end_time,
            priority, True, tender_type,
            ','.join(engines), time_frame, file_type, selected_region,
            email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled, custom_emails
        ))

        task = g.cur.fetchone()
        task_id = task[0]
        logger.info(f"Inserted new task with task_id: {task_id}")

        if search_terms:
            for term in search_terms:
                try:
                    g.cur.execute("INSERT INTO task_search_terms (task_id, term) VALUES (%s, %s)", (task_id, term))
                except Exception as e:
                    logger.error(f"Error inserting term '{term}' for task_id {task_id}: {str(e)}")

        task_dict = format_task_response(task, search_terms)

        from webapp.services.scheduler import scheduler
        scraping_function_name = SCRAPING_FUNCTIONS.get(tender_type)
        if scraping_function_name:
            scraping_function = globals().get(scraping_function_name)
            if scraping_function:
                schedule_task_scrape(
                    scheduler, socketio, current_user, task_id, scraping_function, frequency,
                    tender_type=tender_type, search_terms=search_terms, search_engines=engines
                )

        log_task_event(task_id, current_user, f'Task "{name}" created successfully.')
        add_notification(current_user, f"Task '{name}' created successfully.")

        delete_cache(f"scraping_tasks:user:{current_user}")

        return jsonify({
            "msg": "Task created successfully.",
            "task_id": task_id,
            "task": task_dict
        }), 201
    except Exception as e:
        logger.error(f"Error creating task: {str(e)}")
        return jsonify({"msg": f"Error creating task: {str(e)}"}), 500

@task_service_bp.route('/api/run-scheduled-task/<int:task_id>', methods=['POST'], endpoint='run_scheduled_task')
@jwt_required()
def run_scheduled_task(task_id):
    """
    Manually run a scheduled scraping task in the background.
    
    Args:
        task_id (int): The ID of the task to run.
    
    Returns:
        JSON response with the scraping task ID.
    """
    current_user = get_jwt_identity()
    logger.info(f"User {current_user} is attempting to manually run task ID {task_id}.")

    try:
        task = fetch_task_details(task_id, current_user, """
            task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type, last_run, 
            search_engines, time_frame, file_type, selected_region, email_notifications_enabled, custom_emails
        """)

        tender_type = task[7]
        search_engines = task[9].split(',') if task[9] else []
        time_frame = task[10]
        file_type = task[11]
        selected_region = task[12]
        email_notifications_enabled = task[13]
        custom_emails = task[14] or ""

        search_terms = get_search_terms(task_id)
        scraping_function_name = SCRAPING_FUNCTIONS.get(tender_type)
        scraping_function = globals().get(scraping_function_name) if scraping_function_name else None

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
            from webapp.config import get_db_connection, close_db_connection
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
                    logger.info(f"Email notifications enabled for task {task_id}. Sending notifications to: {custom_emails}")
                    recipient_emails = custom_emails if custom_emails else DEFAULT_RECIPIENT_EMAIL
                    notify_open_tenders(tenders, task_id, recipient_emails=recipient_emails)
                    open_tenders_count = len([t for t in tenders if t.get('status') == 'open'])
                    if open_tenders_count > 0:
                        add_notification(
                            current_user,
                            f"Task '{task[1]}' found {open_tenders_count} new open tender(s)."
                        )

                # Log final tender count
                task_state = get_task_state(scraping_task_id)
                open_tenders_count = len([t for t in tenders if t.get('status') == 'open'])
                expired_tenders_count = task_state.get('summary', {}).get('closedTenders', 0) if task_state else 0
                total_tenders_count = task_state.get('summary', {}).get('totalTenders', 0) if task_state else 0
                logger.info(f"Scraping completed for task {task_id} (scraping_task_id: {scraping_task_id}). Total tenders found: {total_tenders_count}, Open: {open_tenders_count}, Expired: {expired_tenders_count}")

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

        g.cur.execute("UPDATE scheduled_tasks SET last_run = %s WHERE task_id = %s", (datetime.now(), task_id))
        log_task_event(task_id, current_user, f'Task "{task[1]}" manually started with scraping_task_id {scraping_task_id}.')
        return jsonify({
            "msg": "Task started successfully.",
            "scraping_task_id": scraping_task_id
        }), 200

    except TaskNotFoundError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error running task {task_id}: {str(e)}")
        return jsonify({"msg": "Error running task."}), 500
    
@task_service_bp.route('/api/run-task/<int:task_id>', methods=['POST'], endpoint='run_task')
@jwt_required()
def run_task(task_id):
    """
    Run a scraping task synchronously.
    
    Args:
        task_id (int): The ID of the task to run.
    
    Returns:
        JSON response indicating success or failure.
    """
    current_user = get_jwt_identity()
    logger.info(f"User {current_user} requested to run task ID {task_id}")

    try:
        task = fetch_task_details(task_id, current_user, """
            user_id, name, tender_type, frequency, search_engines, time_frame, file_type, selected_region, 
            email_notifications_enabled, custom_emails
        """)

        search_terms = get_search_terms(task_id)
        selected_engines = task[4].split(',') if task[4] else []
        time_frame = task[5]
        file_type = task[6]
        selected_region = task[7]
        email_notifications_enabled = task[8]
        custom_emails = task[9] or ""

        scraping_function_name = SCRAPING_FUNCTIONS.get(task[2])
        scraping_function = globals().get(scraping_function_name) if scraping_function_name else None
        if scraping_function:
            try:
                logger.info(f"Running task '{task[1]}' with search terms: {search_terms}.")
                tenders = []
                if scraping_function in [scrape_ungm_tenders, fetch_reliefweb_tenders, jobinrwanda_tenders,
                                        treasury_ke_tenders, scrape_undp_tenders, scrape_ppip_tenders]:
                    scraping_function()
                elif task[2] == 'Search Query Tenders':
                    from webapp.scrapers.run_query_scraper import scrape_tenders_from_query
                    query = ' '.join(search_terms) if search_terms else ''
                    if not query:
                        add_notification(current_user, f"Task '{task[1]}' failed to run: No search terms provided.")
                        return jsonify({"msg": "No search terms provided for Search Query Tenders."}), 400
                    tenders = scrape_tenders_from_query(g.conn, query, selected_engines, task_id)
                else:
                    scraping_function(selected_engines=selected_engines, time_frame=time_frame, file_type=file_type, region=selected_region, terms=search_terms)

                if email_notifications_enabled and tenders:
                    logger.info(f"Email notifications enabled for task {task_id}. Sending notifications to: {custom_emails}")
                    recipient_emails = custom_emails if custom_emails else DEFAULT_RECIPIENT_EMAIL
                    notify_open_tenders(tenders, task_id, recipient_emails=recipient_emails)
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

            g.cur.execute("UPDATE scheduled_tasks SET last_run = %s WHERE task_id = %s", (datetime.now(), task_id))

            from webapp.services.scheduler import scheduler
            schedule_task_scrape(
                scheduler, socketio, current_user, task_id,
                scraping_function if scraping_function else scrape_tenders_from_query,
                task[3], tender_type=task[2], search_terms=search_terms, search_engines=selected_engines
            )

        log_task_event(task_id, current_user, f"Task '{task[1]}' has been executed.")
        delete_cache(f"scraping_tasks:user:{current_user}")
        return jsonify({"msg": f"Task '{task[1]}' has been executed."}), 200
    except TaskNotFoundError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error running task {task_id}: {str(e)}")
        return jsonify({"msg": "Error running task."}), 500

        g.cur.execute("UPDATE scheduled_tasks SET last_run = %s WHERE task_id = %s", (datetime.now(), task_id))

        from webapp.services.scheduler import scheduler
        schedule_task_scrape(
            scheduler, socketio, current_user, task_id,
            scraping_function if scraping_function else scrape_tenders_from_query,
            task[3], tender_type=task[2], search_terms=search_terms, search_engines=selected_engines
        )

        log_task_event(task_id, current_user, f"Task '{task[1]}' has been executed.")
        delete_cache(f"scraping_tasks:user:{current_user}")
        return jsonify({"msg": f"Task '{task[1]}' has been executed."}), 200
    except TaskNotFoundError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error running task {task_id}: {str(e)}")
        return jsonify({"msg": "Error running task."}), 500

@task_service_bp.route('/api/task-logs/<int:task_id>', methods=['GET'], endpoint='get_task_logs')
@jwt_required()
def get_task_logs(task_id):
    """
    Fetch logs for a specific task.
    
    Args:
        task_id (int): The ID of the task.
    
    Returns:
        JSON response with the task logs.
    """
    current_user = get_jwt_identity()

    cache_key = f"task_logs:user:{current_user}:task:{task_id}"
    cached_logs = get_cache(cache_key)
    if cached_logs is not None:
        return jsonify({"logs": cached_logs}), 200

    try:
        g.cur.execute("SELECT log_entry, created_at FROM task_logs WHERE task_id = %s AND user_id = %s",
                    (task_id, current_user))
        logs = g.cur.fetchall()

        if not logs:
            return jsonify({"msg": "No logs found for this task."}), 404

        logs_list = [{"log_entry": log[0], "created_at": log[1].isoformat() if log[1] else None} for log in logs]
        set_cache(cache_key, logs_list, expiry=60)
        return jsonify({"logs": logs_list}), 200
    except Exception as e:
        logger.error(f"Error fetching logs for task {task_id}: {str(e)}")
        return jsonify({"msg": "Error fetching logs."}), 500

@task_service_bp.route('/api/all-task-logs', methods=['GET'], endpoint='get_all_task_logs')
@jwt_required()
def get_all_task_logs():
    """
    Fetch all task logs for the authenticated user.
    
    Returns:
        JSON response with all task logs.
    """
    current_user = get_jwt_identity()

    cache_key = f"all_task_logs:user:{current_user}"
    cached_logs = get_cache(cache_key)
    if cached_logs is not None:
        return jsonify({"logs": cached_logs}), 200

    try:
        g.cur.execute("SELECT task_id, log_entry, created_at FROM task_logs WHERE user_id = %s", (current_user,))
        logs = g.cur.fetchall()

        if not logs:
            return jsonify({"msg": "No logs found for this user."}), 404

        logs_list = [{"task_id": log[0], "log_entry": log[1], "created_at": log[2].isoformat() if log[2] else None} for log in logs]
        set_cache(cache_key, logs_list, expiry=60)
        return jsonify({"logs": logs_list}), 200
    except Exception as e:
        logger.error(f"Error fetching all logs: {str(e)}")
        return jsonify({"msg": "Error fetching logs."}), 500

@task_service_bp.route('/api/clear-logs/<int:task_id>', methods=['DELETE'], endpoint='clear_logs')
@jwt_required()
def clear_logs(task_id):
    """
    Clear logs for a specific task.
    
    Args:
        task_id (int): The ID of the task.
    
    Returns:
        JSON response indicating success.
    """
    current_user = get_jwt_identity()
    logger.info(f"User {current_user} is attempting to clear logs for task ID {task_id}.")

    try:
        task = fetch_task_details(task_id, current_user, "user_id, name")
        logger.info(f"Deleting logs for task ID {task_id} for user {current_user}.")

        g.cur.execute("DELETE FROM task_logs WHERE task_id = %s AND user_id = %s", (task_id, current_user))
        delete_cache(f"task_logs:user:{current_user}:task:{task_id}")
        delete_cache(f"all_task_logs:user:{current_user}")

        add_notification(current_user, f"Logs cleared for task '{task[1]}'.")
        logger.info(f"Logs cleared successfully for task ID {task_id}.")
        return jsonify({"msg": "Logs cleared successfully."}), 200
    except TaskNotFoundError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error clearing logs for task {task_id}: {str(e)}")
        return jsonify({"msg": "Error clearing logs."}), 500

@task_service_bp.route('/api/cancel-task/<int:task_id>', methods=['DELETE'], endpoint='cancel_task')
@jwt_required()
def cancel_task(task_id):
    """
    Cancel (delete) a scheduled task.
    
    Args:
        task_id (int): The ID of the task to cancel.
    
    Returns:
        JSON response indicating success.
    """
    current_user = get_jwt_identity()

    try:
        task = fetch_task_details(task_id, current_user, "user_id, name")
        g.cur.execute("DELETE FROM task_search_terms WHERE task_id = %s", (task_id,))
        g.cur.execute("DELETE FROM scheduled_tasks WHERE task_id = %s", (task_id,))

        # Remove the scheduled job if it exists
        from webapp.services.scheduler import scheduler
        job_id = generate_job_id(current_user, task_id)
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)

        delete_cache(f"scraping_tasks:user:{current_user}")
        add_notification(current_user, f"Task '{task[1]}' canceled successfully.")
        return jsonify({"msg": "Task canceled successfully."}), 200
    except TaskNotFoundError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error canceling task {task_id}: {str(e)}")
        return jsonify({"msg": "Error canceling task."}), 500

@task_service_bp.route('/api/toggle-task-status/<int:task_id>', methods=['PATCH'], endpoint='toggle_task_status')
@jwt_required()
def toggle_task_status(task_id):
    """
    Toggle the enabled/disabled status of a task.
    
    Args:
        task_id (int): The ID of the task to toggle.
    
    Returns:
        JSON response indicating success or failure.
    """
    current_user = get_jwt_identity()

    try:
        # Fetch task details
        task = fetch_task_details(task_id, current_user, "user_id, is_enabled, name")
        if not task or len(task) < 3:
            raise ValueError(f"Invalid task data returned for task_id {task_id}: {task}")

        new_status = not task[1]  # Toggle is_enabled
        # Execute update query
        g.cur.execute(
            "UPDATE scheduled_tasks SET is_enabled = %s WHERE task_id = %s",
            (new_status, task_id)
        )
        # Commit the transaction
        g.conn.commit()  # Use g.conn instead of g.db

        status_message = 'enabled' if new_status else 'disabled'
        log_task_event(task_id, current_user, f'Task "{task[2]}" has been {status_message} successfully.')
        add_notification(current_user, f"Task '{task[2]}' {status_message} successfully.")
        delete_cache(f"scraping_tasks:user:{current_user}")
        return jsonify({"msg": f"Task {task_id} {status_message} successfully."}), 200

    except TaskNotFoundError as e:
        logger.warning(f"Task not found: {str(e)}")
        return jsonify({"msg": str(e)}), 404
    except ValueError as e:
        logger.error(f"Validation error for task {task_id}: {str(e)}", exc_info=True)
        return jsonify({"msg": f"Invalid task data: {str(e)}"}), 400
    except Exception as e:
        logger.error(f"Error toggling task status for task {task_id}: {str(e)}", exc_info=True)
        return jsonify({"msg": f"Error toggling task status: {str(e)}"}), 500

@task_service_bp.route('/api/edit-task/<int:task_id>', methods=['PUT'], endpoint='edit_task')
@jwt_required()
def edit_task(task_id):
    """
    Edit an existing scheduled task.
    
    Args:
        task_id (int): The ID of the task to edit.
    
    Returns:
        JSON response indicating success.
    """
    current_user = get_jwt_identity()
    data = request.get_json()
    logger.debug(f"Received data for task {task_id}: {data}")

    # Extract fields from payload
    task_name = data.get('name')
    frequency = data.get('frequency', 'Daily')
    priority = data.get('priority', 'Medium')
    tender_type = data.get('tender_type', 'All')  # Use snake_case to match frontend
    search_terms = data.get('search_terms', [])
    engines = data.get('engines', [])
    email_notifications_enabled = data.get('email_notifications_enabled', False)
    sms_notifications_enabled = data.get('sms_notifications_enabled', False)
    slack_notifications_enabled = data.get('slack_notifications_enabled', False)
    custom_emails = data.get('custom_emails', '')

    # Validate required fields
    if not task_name or not frequency or not tender_type:
        return jsonify({"msg": "Missing required fields"}), 400

    # Handle start_time and end_time
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

    # Handle custom_emails: convert to string if it's a list
    if isinstance(custom_emails, list):
        custom_emails = ','.join([email.strip() for email in custom_emails if email.strip()])
        logger.warning(f"Received custom_emails as list: {custom_emails}, converted to string")
    elif not isinstance(custom_emails, str):
        custom_emails = ''

    # Validate emails if custom_emails is non-empty
    email_list = []
    if custom_emails:
        email_list = [email.strip() for email in custom_emails.split(",") if email.strip()]
        email_regex = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
        invalid_emails = [email for email in email_list if not re.match(email_regex, email)]
        if invalid_emails:
            return jsonify({"msg": f"Invalid email addresses: {', '.join(invalid_emails)}"}), 400

    try:
        # Fetch existing task details for change logging
        task = fetch_task_details(
            task_id,
            current_user,
            "user_id, name, frequency, start_time, end_time, priority, tender_type, "
            "email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled, "
            "custom_emails, search_terms, engines"
        )

        # Log changes
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
        if sms_notifications_enabled != task[8]:
            changes.append(f'SMS notifications enabled changed from "{task[8]}" to "{sms_notifications_enabled}"')
        if slack_notifications_enabled != task[9]:
            changes.append(f'Slack notifications enabled changed from "{task[9]}" to "{slack_notifications_enabled}"')
        if custom_emails != task[10]:
            changes.append(f'Custom emails changed from "{task[10]}" to "{custom_emails}"')
        if search_terms != task[11]:
            changes.append(f'Search terms changed from "{task[11]}" to "{search_terms}"')
        if engines != task[12]:
            changes.append(f'Engines changed from "{task[12]}" to "{engines}"')

        # Update the task
        g.cur.execute("""
            UPDATE scheduled_tasks
            SET name = %s, frequency = %s, start_time = %s, end_time = %s, priority = %s,
                tender_type = %s, email_notifications_enabled = %s,
                sms_notifications_enabled = %s, slack_notifications_enabled = %s,
                custom_emails = %s, search_terms = %s, engines = %s
            WHERE task_id = %s AND user_id = %s
            RETURNING task_id, name, frequency, start_time, end_time, priority, tender_type,
                      email_notifications_enabled, sms_notifications_enabled,
                      slack_notifications_enabled, custom_emails, search_terms, engines
        """, (
            task_name, frequency, start_time, end_time, priority, tender_type,
            email_notifications_enabled, sms_notifications_enabled, slack_notifications_enabled,
            custom_emails, search_terms, engines,
            task_id, current_user
        ))

        updated_task = g.cur.fetchone()
        g.conn.commit()

        if updated_task is None:
            logger.error(f"Task {task_id} not found or user not authorized")
            return jsonify({"msg": "Task not found or unauthorized"}), 404

        # Prepare response
        task_response = {
            "task_id": updated_task[0],
            "name": updated_task[1],
            "frequency": updated_task[2],
            "start_time": updated_task[3].isoformat(),
            "end_time": updated_task[4].isoformat(),
            "priority": updated_task[5],
            "tender_type": updated_task[6],
            "email_notifications_enabled": updated_task[7],
            "sms_notifications_enabled": updated_task[8],
            "slack_notifications_enabled": updated_task[9],
            "custom_emails": updated_task[10],
            "search_terms": updated_task[11],
            "engines": updated_task[12],
        }

        # Log changes
        log_message = ' and '.join(changes) if changes else 'Task updated with no changes.'
        log_task_event(task_id, current_user, log_message)
        add_notification(current_user, f"Task '{task_name}' updated: {log_message}")
        delete_cache(f"scraping_tasks:user:{current_user}")

        return jsonify({
            "msg": "Task edited successfully.",
            "task": task_response
        }), 200
    except TaskNotFoundError as e:
        return jsonify({"msg": str(e)}), 404
    except Exception as e:
        logger.error(f"Error editing task {task_id}: {str(e)}")
        return jsonify({"msg": f"Error editing task: {str(e)}"}), 500

@task_service_bp.route('/api/next-schedule', methods=['GET'], endpoint='get_next_schedule')
@jwt_required()
def get_next_schedule():
    """
    Fetch the next scheduled task time for the authenticated user.
    
    Returns:
        JSON response with the next scheduled time.
    """
    current_user = get_jwt_identity()

    try:
        g.cur.execute("""
            SELECT start_time 
            FROM scheduled_tasks 
            WHERE user_id = %s AND is_enabled = TRUE 
            ORDER BY start_time ASC 
            LIMIT 1;
        """, (current_user,))
        result = g.cur.fetchone()

        return jsonify({"next_schedule": result[0].isoformat() if result and result[0] else "N/A"}), 200
    except Exception as e:
        logger.error(f"Error fetching next schedule: {str(e)}")
        return jsonify({"msg": "Error fetching next schedule."}), 500

# --- Helper Functions ---

def log_task_event(task_id, user_id, log_message):
    """
    Log a task event to the database.
    
    Args:
        task_id (int): The ID of the task.
        user_id (str): The ID of the user.
        log_message (str): The log message.
    """
    try:
        created_at = datetime.now().isoformat()
        g.cur.execute("INSERT INTO task_logs (task_id, user_id, log_entry, created_at) VALUES (%s, %s, %s, %s)",
                    (task_id, user_id, log_message, created_at))
        delete_cache(f"task_logs:user:{user_id}:task:{task_id}")
        delete_cache(f"all_task_logs:user:{user_id}")
    except Exception as e:
        logger.error(f"Error logging task event for task_id {task_id}: {str(e)}")