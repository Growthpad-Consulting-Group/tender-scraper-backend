from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, JWTManager, jwt_required, get_jwt_identity
from webapp.config import get_db_connection
import logging
from datetime import datetime, timedelta
from dateutil import parser
from webapp.services.scheduler import scheduler
from webapp.scrapers.ungm_tenders import scrape_ungm_tenders
from webapp.scrapers.undp_tenders import scrape_undp_tenders
from webapp.scrapers.reliefweb_tenders import fetch_reliefweb_tenders
from webapp.scrapers.scrape_jobinrwanda_tenders import scrape_jobinrwanda_tenders
from webapp.scrapers.scrape_treasury_ke_tenders import scrape_treasury_ke_tenders
from webapp.scrapers.website_scraper import scrape_tenders_from_websites
from webapp.scrapers.query_scraper import scrape_tenders_from_query

task_manager_bp = Blueprint('task_manager', __name__)

def get_search_terms(cur, task_id):
    """Fetch search terms associated with a task by task_id."""
    cur.execute("SELECT term FROM task_search_terms WHERE task_id = %s", (task_id,))
    return [row[0] for row in cur.fetchall()]

# Route to get all scraping tasks
@task_manager_bp.route('/api/scraping-tasks', methods=['GET'])
@jwt_required()
def get_scraping_tasks():
    current_user = get_jwt_identity()
    logging.info(f"Fetching tasks for user_id: {current_user}")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type, last_run 
            FROM scheduled_tasks
            WHERE user_id = %s
        """, (current_user,))

        tasks = cur.fetchall()
        task_list = []
        for task in tasks:
            task_dict = {
                "task_id": task[0],
                "name": task[1],
                "frequency": task[2],
                "priority": task[5],
                "is_enabled": task[6],
                "tender_type": task[7],
                "start_time": task[3],
                "end_time": task[4],
                "last_run": task[8]
            }

            for key in ["start_time", "end_time", "last_run"]:
                if task_dict[key] is not None:
                    task_dict[key] = task_dict[key].isoformat()

            task_list.append(task_dict)

        return jsonify({"tasks": task_list}), 200
    except Exception as e:
        logging.error(f"Error fetching tasks: {str(e)}")
        return jsonify({"msg": "Error fetching tasks."}), 500
    finally:
        cur.close()  # Ensure cursor is closed


# Task ID Along with User ID
def generate_job_id(user_id, task_id):
    return f"user_{user_id}_task_{task_id}"

def schedule_task_scrape(user_id, task_id, job_function, frequency):
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
        'Monthly': {'days': 30}  # Changed months to days (approximation)
    }

    if frequency in trigger_args:
        scheduler.add_job(job_function, trigger, id=job_id, **trigger_args[frequency])
        logging.info(f'Scheduled job: {job_id}')
    else:
        logging.warning(f'Unsupported frequency: {frequency}')


# Function to log job events
def log_task_event(task_id, user_id, log_message):
    conn = get_db_connection()
    cur = conn.cursor()
    created_at = datetime.now().isoformat()
    cur.execute("INSERT INTO task_logs (task_id, user_id, log_entry, created_at) VALUES (%s, %s, %s, %s)",
                (task_id, user_id, log_message, created_at))
    conn.commit()
    cur.close()  # Ensure cursor is closed


# Function to log job events
def job_listener(event):
    if event.exception:
        logging.error('Job %s failed: %s', event.job_id, event.exception)
    else:
        logging.info('Job %s completed successfully.', event.job_id)


    # Route to clear logs


@task_manager_bp.route('/api/clear-logs/<int:task_id>', methods=['DELETE'])
@jwt_required()
def clear_logs(task_id):
    current_user = get_jwt_identity()
    logging.info(f"User {current_user} is attempting to clear logs for task ID {task_id}.")

    conn = get_db_connection()
    cur = conn.cursor()

    # Check user permissions
    cur.execute("SELECT user_id FROM scheduled_tasks WHERE task_id = %s", (task_id,))
    task = cur.fetchone()

    if task is None:
        logging.warning(f"No task found for task ID {task_id}.")
        return jsonify({"msg": "Task not found."}), 404

    if task[0] != current_user:
        logging.warning(f"User {current_user} is not authorized to clear logs for task ID {task_id}.")
        return jsonify({"msg": "You do not have permission to clear logs for this task."}), 403

    # Log before deleting
    logging.info(f"Deleting logs for task ID {task_id} for user {current_user}.")

    # Delete logs for the specific task
    cur.execute("DELETE FROM task_logs WHERE task_id = %s AND user_id = %s", (task_id, current_user))
    conn.commit()

    logging.info(f"Logs cleared successfully for task ID {task_id}.")
    return jsonify({"msg": "Logs cleared successfully."}), 200


# Route to add a new scheduled task
@task_manager_bp.route('/api/add-task', methods=['POST'])
@jwt_required()
def add_task():
    current_user = get_jwt_identity()
    data = request.get_json()

    name = data.get('name')
    frequency = data.get('frequency', 'Daily')
    priority = data.get('priority', 'Medium')
    tender_type = data.get('tenderType', 'All')
    selected_terms = data.get('searchTerms', [])
    search_engines = data.get('SEARCH_ENGINES', [])
    time_frame = data.get('timeFrame', None)
    file_type = data.get('fileType', None)
    selected_region = data.get('selectedRegion', None)  # New: Capture selected region

    if not name:
        return jsonify({"msg": "Task name is required."}), 400

    current_time = datetime.now()
    start_time = current_time
    end_time = start_time + timedelta(days=365)

    conn = get_db_connection()
    cur = conn.cursor()

    # Update SQL to include selected region
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
    conn.commit()

    # Insert search terms after task creation
    if selected_terms:
        for term in selected_terms:
            try:
                cur.execute("INSERT INTO task_search_terms (task_id, term) VALUES (%s, %s)", (task_id, term))
            except Exception as e:
                logging.error(f"Error inserting term '{term}' for task_id {task_id}: {str(e)}")

    conn.commit()

    # Optional: Call scraping function
    scraping_function = get_scraping_function(tender_type)
    if scraping_function:
        schedule_task_scrape(current_user, task_id, scraping_function, frequency)

    log_task_event(task_id, current_user, f'Task "{name}" added successfully.')
    cur.close()
    return jsonify({"msg": "Task added successfully.", "task_id": task_id}), 201

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
    return mapping.get(tender_type)  # Return scraping function, or None if not found




# Route to fetch logs
@task_manager_bp.route('/api/task-logs/<int:task_id>', methods=['GET'])
@jwt_required()
def get_task_logs(task_id):
    current_user = get_jwt_identity()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT log_entry, created_at FROM task_logs WHERE task_id = %s AND user_id = %s",
                (task_id, current_user))
    logs = cur.fetchall()

    if not logs:
        return jsonify({"msg": "No logs found for this task."}), 404  # Return a 404 if there are no logs

    logs_list = []
    for log in logs:
        logs_list.append({
            "log_entry": log[0],
            "created_at": log[1]  # Add the created_at field to the response
        })

    return jsonify({"logs": logs_list}), 200


# Route to fetch all task logs for the authenticated user
@task_manager_bp.route('/api/all-task-logs', methods=['GET'])
@jwt_required()
def get_all_task_logs():
    current_user = get_jwt_identity()

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Fetch all logs for the current user
        cur.execute("SELECT task_id, log_entry, created_at FROM task_logs WHERE user_id = %s", (current_user,))
        logs = cur.fetchall()

        if not logs:
            return jsonify({"msg": "No logs found for this user."}), 404  # Return a 404 if there are no logs

        logs_list = []
        for log in logs:
            logs_list.append({
                "task_id": log[0],  # Include task_id for reference
                "log_entry": log[1],
                "created_at": log[2]  # Add the created_at field to the response
            })

        return jsonify({"logs": logs_list}), 200

    except Exception as e:
        logging.error("Error fetching logs: %s", str(e))
        return jsonify({"error": "An error occurred while fetching logs."}), 500
    finally:
        cur.close()
        conn.close()



# Route to cancel a scheduled task
@task_manager_bp.route('/api/cancel-task/<int:task_id>', methods=['DELETE'])
@jwt_required()
def cancel_task(task_id):
    current_user = get_jwt_identity()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT user_id FROM scheduled_tasks WHERE task_id = %s", (task_id,))
    task = cur.fetchone()

    if task is None:
        return jsonify({"msg": "Task not found."}), 404

    if task[0] != current_user:
        return jsonify({"msg": "You do not have permission to cancel this task."}), 403

    # Delete associated search terms first
    cur.execute("DELETE FROM task_search_terms WHERE task_id = %s", (task_id,))

    # Now delete the task itself
    cur.execute("DELETE FROM scheduled_tasks WHERE task_id = %s", (task_id,))
    conn.commit()
    cur.close()  # Ensure cursor is closed

    return jsonify({"msg": "Task canceled successfully."}), 200


# Route to toggle task status (Enable/Disable)
@task_manager_bp.route('/api/toggle-task-status/<int:task_id>', methods=['PATCH'])
@jwt_required()
def toggle_task_status(task_id):
    current_user = get_jwt_identity()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, is_enabled FROM scheduled_tasks WHERE task_id = %s", (task_id,))
    task = cur.fetchone()

    if task is None:
        return jsonify({"msg": "Task not found."}), 404
    if task[0] != current_user:
        return jsonify({"msg": "You do not have permission to toggle this task's status."}), 403

    new_status = not task[1]
    cur.execute("UPDATE scheduled_tasks SET is_enabled = %s WHERE task_id = %s", (new_status, task_id))
    conn.commit()

    status_message = 'enabled' if new_status else 'disabled'
    log_task_event(task_id, current_user, f'Task "{task_id}" has been {status_message} successfully.')

    return jsonify({"msg": f"Task {task_id} {status_message} successfully."}), 200


# Route to edit a scheduled task
@task_manager_bp.route('/api/edit-task/<int:task_id>', methods=['PUT'])
@jwt_required()
def edit_task(task_id):
    current_user = get_jwt_identity()
    data = request.get_json()

    frequency = data.get('frequency', 'Daily')
    priority = data.get('priority', 'Medium')
    task_name = data.get('name')
    tender_type = data.get('tenderType', 'All')  # Handle tender type

    # Check if start and end times are provided, else set defaults based on frequency
    if data.get('startTime') and data.get('endTime'):
        start_time_str = data.get('startTime')
        end_time_str = data.get('endTime')
        try:
            start_time = parser.parse(start_time_str)
            end_time = parser.parse(end_time_str)
        except ValueError:
            return jsonify({"msg": "Invalid date format for start time or end time."}), 400
    else:
        current_time = datetime.now()
        if frequency == 'Hourly':
            start_time = current_time.replace(minute=0, second=0, microsecond=0)  # Start at the top of the hour
            end_time = start_time + timedelta(hours=1)  # End after 1 hour
        elif frequency == 'Every 3 Hours':
            start_time = current_time.replace(minute=0, second=0, microsecond=0)  # Start at the top of the hour
            end_time = start_time + timedelta(hours=3)  # End after 3 hours
        elif frequency == 'Every 12 Hours':
            start_time = current_time.replace(hour=current_time.hour // 12 * 12, minute=0, second=0, microsecond=0)  # Start at noon or midnight.
            end_time = start_time + timedelta(hours=12)  # End after 12 hours
        elif frequency == 'Daily':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)  # Fixed time
            end_time = start_time + timedelta(days=1)  # End next day
        elif frequency == 'Weekly':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)  # Fixed time
            end_time = start_time + timedelta(weeks=1)  # End next week
        elif frequency == 'Monthly':
            start_time = current_time.replace(day=1, hour=10, minute=0, second=0, microsecond=0)  # Start on the first day of the next month at 10 AM
            if current_time.month == 12:
                start_time = start_time.replace(year=current_time.year + 1, month=1)  # Adjust for January
            else:
                start_time = start_time.replace(month=current_time.month + 1)  # Move to the next month
            end_time = start_time + timedelta(days=31)  # Set end time roughly 31 days later for simplicity
        else:
            return jsonify({"msg": "Unsupported frequency provided."}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, name, frequency, start_time, end_time, priority, tender_type FROM scheduled_tasks WHERE task_id = %s",
        (task_id,))
    task = cur.fetchone()

    if task is None:
        return jsonify({"msg": "Task not found."}), 404
    if task[0] != current_user:
        return jsonify({"msg": "You do not have permission to edit this task."}), 403

    # Create log entry for previous and new values
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

    # Execute the update including the tender_type
    cur.execute("""
        UPDATE scheduled_tasks SET name = %s, frequency = %s, start_time = %s, end_time = %s, priority = %s, tender_type = %s
        WHERE task_id = %s
    """, (task_name, frequency, start_time, end_time, priority, tender_type, task_id))
    conn.commit()

    # Log all the changes
    log_message = ' and '.join(changes) if changes else 'Task updated with no changes.'
    log_task_event(task_id, current_user, log_message)

    return jsonify({"msg": "Task edited successfully."}), 200


# Route to fetch the next scheduled task for the authenticated user
@task_manager_bp.route('/api/next-schedule', methods=['GET'])
@jwt_required()
def get_next_schedule():
    current_user = get_jwt_identity()

    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch the next scheduled task that is enabled
    cur.execute("""
        SELECT start_time 
        FROM scheduled_tasks 
        WHERE user_id = %s AND is_enabled = TRUE 
        ORDER BY start_time ASC 
        LIMIT 1;
    """, (current_user,))

    result = cur.fetchone()

    # If there is a result, return it, otherwise return "N/A"
    if result:
        return jsonify({"next_schedule": result[0]}), 200
    else:
        return jsonify({"next_schedule": "N/A"}), 200


@task_manager_bp.route('/api/run-task/<int:task_id>', methods=['POST'])
@jwt_required()
def run_task(task_id):
    current_user = get_jwt_identity()
    logging.info(f"User {current_user} requested to run task ID {task_id}")

    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch task details including search engines, time frame, and file type
    cur.execute("""
        SELECT user_id, name, tender_type, frequency, search_engines, time_frame, file_type, selected_region 
        FROM scheduled_tasks 
        WHERE task_id = %s
    """, (task_id,))
    task = cur.fetchone()

    if task is None or task[0] != current_user:
        return jsonify({"msg": "Task not found or access denied."}), 404

    search_terms = get_search_terms(cur, task_id)

    selected_engines = task[4].split(',')  # Convert back to list
    time_frame = task[5]  # Index 5 for time frame
    file_type = task[6]   # Index 6 for file type
    selected_region = task[7]  # New: Capture selected region

    scraping_function = get_scraping_function(task[2])
    if scraping_function:
        try:
            logging.info(f"Running task '{task[1]}' with selected region '{selected_region}' and search terms: {search_terms}.")
            # Call the scraping function with all required parameters
            scraping_function(selected_engines=selected_engines, time_frame=time_frame, file_type=file_type, region=selected_region, terms=search_terms)
            logging.info(f"Task '{task[1]}' executed successfully.")
        except Exception as e:
            logging.error(f"Error running task '{task[1]}': {str(e)}")
            return jsonify({"msg": f"Error running task: {str(e)}"}), 500

        # Update last_run timestamp and commit
        cur.execute("UPDATE scheduled_tasks SET last_run = %s WHERE task_id = %s", (datetime.now(), task_id))
        conn.commit()

        # Optional: Reschedule
        schedule_task_scrape(current_user, task_id, scraping_function, task[3])

    return jsonify({"msg": f"Task '{task[1]}' has been executed."}), 200