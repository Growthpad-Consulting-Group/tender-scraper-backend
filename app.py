from flask import Flask, request, jsonify
from flask_socketio import SocketIO
import bcrypt
from config import get_db_connection
from flask_jwt_extended import create_access_token, JWTManager, jwt_required, get_jwt_identity
from flask_cors import CORS
from keyword_routes import keyword_bp
from search_terms import search_terms_bp
from user_preferences_routes import user_preferences_bp
from upload_routes import upload_bp
from tenders_routes import tenders_bp
from scraper import scrape_tenders
from ca_tenders import scrape_ca_tenders
from undp_tenders import scrape_undp_tenders
from reliefweb_tenders import fetch_reliefweb_tenders
from scrape_jobinrwanda_tenders import scrape_jobinrwanda_tenders
from scrape_treasury_ke_tenders import scrape_treasury_ke_tenders
from website_scraper import scrape_tenders_from_websites
from datetime import datetime
from dateutil import parser
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import requests
import logging
from threading import Thread

app = Flask(__name__)
CORS(app, supports_credentials=True)  # Allow credentials for your requests

# Initialize SocketIO
socketio = SocketIO(app, cors_allowed_origins='*')

def run_scraping_with_progress(tender_types):
    scraping_functions = {
        'CA Tenders': scrape_ca_tenders,
        'ReliefWeb Jobs': fetch_reliefweb_tenders,
        'Job in Rwanda': scrape_jobinrwanda_tenders,
        'Kenya Treasury': scrape_treasury_ke_tenders,
        'UNDP': scrape_undp_tenders,
        'Website Tenders': scrape_tenders_from_websites,
        'General Tenders': scrape_tenders,
    }

    total_tasks = len(tender_types)
    for i, tender_type in enumerate(tender_types):
        # Find the matching scraping function based on the user-friendly name
        function = scraping_functions.get(tender_type)
        if function:
            try:
                function()  # Call the actual scraping function
                print(f"{function.__name__} completed successfully.")
            except Exception as e:
                print(f"Error in {function.__name__}: {str(e)}")

            # Calculate and emit progress
            progress = int((i + 1) / total_tasks * 100)
            socketio.emit('progress', {'progress': progress})  # Send progress to client

    socketio.emit('scan-complete')  # Notify the client that the scan is complete




# JWT setup
app.config['JWT_SECRET_KEY'] = 'your_secret_key'  # Change this to a strong secret key
jwt = JWTManager(app)

# Setup logging
logging.basicConfig(level=logging.INFO)

# User login route
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    recaptcha_token = data.get('recaptchaToken')

    if 'PostmanRuntime' not in request.headers.get('User-Agent', ''):
        recaptcha_response = requests.post('https://www.google.com/recaptcha/api/siteverify', data={
            'secret': '6LcAkewkAAAAAPSABLLl-G3tdvzPJCmou67uZtKc',
            'response': recaptcha_token
        })
        recaptcha_result = recaptcha_response.json()
        if not recaptcha_result.get('success'):
            return jsonify({"msg": "Invalid reCAPTCHA, please try again."}), 400

    if not username or not password:
        return jsonify({"msg": "Please provide both username and password"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT password_hash FROM users WHERE username = %s", (username,))
    user = cur.fetchone()

    if user and bcrypt.checkpw(password.encode('utf-8'), user[0].encode('utf-8')):
        access_token = create_access_token(identity=username)
        return jsonify(access_token=access_token), 200
    else:
        return jsonify({"msg": "Invalid username or password"}), 401

# Protected route for dashboard
@app.route('/dashboard', methods=['GET'])
@jwt_required()
def dashboard():
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user), 200

# Function to run all scraping tasks
def run_all_scraping():
    print("Running all scraping tasks at:", datetime.now())

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
            scrape_func()
            print(f"{scrape_func.__name__} completed successfully.")
        except Exception as e:
            print(f"Error in {scrape_func.__name__}: {str(e)}")

# Initialize APScheduler
scheduler = BackgroundScheduler()

# Schedule the job to run every 24 hours
scheduler.add_job(func=run_all_scraping, trigger="interval", hours=24)
scheduler.start()

#Quick Scan
@app.route('/api/run-scan', methods=['POST'])
@jwt_required()
def run_scan():
    try:
        # Extract data from the request
        data = request.json
        selected_engines = data.get('engines')
        time_frame = data.get('timeFrame')
        file_type = data.get('fileType')
        terms = data.get('terms')

        # Start the scraping process in a separate thread
        thread = Thread(target=scrape_tenders_from_websites, args=(selected_engines, time_frame, file_type, terms))
        thread.start()

        return jsonify({"msg": "Scraping started."}), 202  # Acknowledge the request
    except Exception as e:
        logging.error(f"Error starting scrape: {e}")
        return jsonify({"msg": "Error starting scrape."}), 500
    
# Route to get all scraping tasks
@app.route('/api/scraping-tasks', methods=['GET'])
@jwt_required()
def get_scraping_tasks():
    current_user = get_jwt_identity()

    logging.info(f"Fetching tasks for user_id: {current_user}")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT task_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type 
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
                "end_time": task[4]
            }

            if task_dict["start_time"] is not None:
                task_dict["start_time"] = task_dict["start_time"].isoformat()
            if task_dict["end_time"] is not None:
                task_dict["end_time"] = task_dict["end_time"].isoformat()

            task_list.append(task_dict)

        return jsonify({"tasks": task_list}), 200
    except Exception as e:
        logging.error(f"Error fetching tasks: {str(e)}")
        return jsonify({"msg": "Error fetching tasks."}), 500

# Task ID Along with User ID
def generate_job_id(user_id, task_id):
    return f"user_{user_id}_task_{task_id}"

def schedule_task_scrape(user_id, task_id, job_function, trigger, **trigger_args):
    job_id = generate_job_id(user_id, task_id)

    existing_job = scheduler.get_job(job_id)
    if existing_job:
        print(f"Removing existing job {job_id} before rescheduling.")
        scheduler.remove_job(job_id)

    try:
        scheduler.add_job(job_function, trigger, id=job_id, **trigger_args)
        print(f'Scheduled job: {job_id}')
    except Exception as e:
        print(f"Error scheduling job {job_id}: {e}")


# Unified tender fetching route
@app.route('/api/tenders', methods=['GET', 'POST'])
@jwt_required()
def get_tenders():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Handle POST request
        if request.method == 'POST':
            data = request.get_json()
            tender_types = data.get('tenderTypes', [])
            logging.info(f"Tender Types Querying: {tender_types}")

            if not tender_types:
                tender_types = ['UNDP']

            query = "SELECT title, description, closing_date, status, source_url, format, tender_type FROM tenders"
            query_params = []

            if tender_types:
                query += " WHERE tender_type = ANY(%s);"
                query_params = (f'{{{" , ".join(tender_types)}}}',)
                logging.info(f"Querying with tender types: {query_params}")

            logging.info(f"Executing query: {query} with params: {query_params}")
            cur.execute(query, query_params)
            tenders = cur.fetchall()
            logging.info(f"Tenders fetched from DB (POST): {tenders}")

        # Handle GET request
        elif request.method == 'GET':
            logging.info("Fetching tenders with possible date filtering.")
            start_date = request.args.get('startDate')
            end_date = request.args.get('endDate')

            query = "SELECT title, description, closing_date, status, source_url, format, tender_type FROM tenders"
            query_params = []

            if start_date and end_date:
                query += " WHERE closing_date BETWEEN %s AND %s"
                query_params = (start_date, end_date)
                logging.info(f"Filtering tenders by Date Range: {start_date} to {end_date}")

            logging.info(f"Executing query: {query} with params: {query_params}")
            cur.execute(query, query_params)
            tenders = cur.fetchall()

            # Log total records found
            total_records = len(tenders)
            logging.info(f"Total records found in tenders table: {total_records}")

        # Transform fetched tenders into a list of dictionaries
        tenders_list = [{
            "title": tender[0],
            "description": tender[1] if tender[1] is not None else "No description",
            "closing_date": tender[2],
            "status": tender[3].capitalize(),
            "source_url": tender[4],
            "format": tender[5],
            "tender_type": tender[6]
        } for tender in tenders]

        open_tenders = [tender for tender in tenders_list if tender["status"].lower() == "open"]
        closed_tenders = [tender for tender in tenders_list if tender["status"].lower() == "closed"]

        logging.info(f"Open tenders count: {len(open_tenders)}")
        logging.info(f"Closed tenders count: {len(closed_tenders)}")

        return jsonify({
            "open_tenders": open_tenders,
            "closed_tenders": closed_tenders,
            "total_tenders": total_records,
            "month_names": ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        }), 200

    except Exception as e:
        logging.error("Error fetching tenders: %s", str(e))
        return jsonify({"error": "An error occurred while fetching tenders."}), 500
    finally:
        cur.close()
        conn.close()
        
# Function to log task events
def log_task_event(task_id, user_id, log_message):
    conn = get_db_connection()
    cur = conn.cursor()
    created_at = datetime.now().isoformat()  # Use ISO format/UTC for consistency
    cur.execute("INSERT INTO task_logs (task_id, user_id, log_entry, created_at) VALUES (%s, %s, %s, %s)",
            (task_id, user_id, log_message, created_at))
    conn.commit()


    # Route to clear logs
@app.route('/api/clear-logs/<int:task_id>', methods=['DELETE'])
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
@app.route('/api/add-task', methods=['POST'])
@jwt_required()
def add_task():
    current_user = get_jwt_identity()
    data = request.get_json()

    name = data.get('name')
    frequency = data.get('frequency', 'Daily')
    priority = data.get('priority', 'Medium')
    tender_type = data.get('tenderType', 'All')  # Handle tender type

    # If start_time and end_time are provided, parse them; otherwise, set defaults according to frequency
    if data.get('startTime') and data.get('endTime'):
        start_time = parser.parse(data.get('startTime'))
        end_time = parser.parse(data.get('endTime'))
    else:
        current_time = datetime.now()
        if frequency == 'Daily':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)  # End time is the next day
        elif frequency == 'Weekly':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(weeks=1)  # End time is one week later
        else:
            return jsonify({"msg": "Frequency must be either 'Daily' or 'Weekly' or specify start and end times."}), 400

    if not name or start_time is None or end_time is None:
        return jsonify({"msg": "Task name, start time, and end time are required."}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scheduled_tasks (user_id, name, frequency, start_time, end_time, priority, is_enabled, tender_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING task_id;
    """, (current_user, name, frequency, start_time, end_time, priority, False, tender_type))  # Include tender_type here

    task_id = cur.fetchone()[0]  # Automatically generated task_id
    conn.commit()

    log_task_event(task_id, current_user, f'Task "{name}" added successfully.')

    return jsonify({"msg": "Task added successfully.", "task_id": task_id}), 201

# Route to fetch logs
@app.route('/api/task-logs/<int:task_id>', methods=['GET'])
@jwt_required()
def get_task_logs(task_id):
    current_user = get_jwt_identity()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT log_entry, created_at FROM task_logs WHERE task_id = %s AND user_id = %s", (task_id, current_user))
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
@app.route('/api/all-task-logs', methods=['GET'])
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
@app.route('/api/cancel-task/<int:task_id>', methods=['DELETE'])
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

    job_id = generate_job_id(current_user, task_id)

    # Check if the job exists before trying to remove it
    job = scheduler.get_job(job_id)
    if job:
        try:
            scheduler.remove_job(job_id)
            log_task_event(task_id, current_user, f'Task "{task_id}" canceled successfully.')
        except Exception as e:
            return jsonify({"msg": f"Failed to remove job {job_id}: {str(e)}"}), 500
    else:
        print(f"Job {job_id} not found in scheduler.")

    cur.execute("DELETE FROM scheduled_tasks WHERE task_id = %s", (task_id,))
    conn.commit()

    return jsonify({"msg": "Task canceled successfully."}), 200

# Route to toggle task status (Enable/Disable)
@app.route('/api/toggle-task-status/<int:task_id>', methods=['PATCH'])
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
@app.route('/api/edit-task/<int:task_id>', methods=['PUT'])
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
        if frequency == 'Daily':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
        elif frequency == 'Weekly':
            start_time = current_time.replace(hour=10, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(weeks=1)
        else:
            return jsonify({"msg": "Frequency must be either 'Daily' or 'Weekly' or specify start and end times."}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, name, frequency, start_time, end_time, priority, tender_type FROM scheduled_tasks WHERE task_id = %s", (task_id,))
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

atexit.register(lambda: scheduler.shutdown())

# Register blueprints
app.register_blueprint(keyword_bp)
app.register_blueprint(search_terms_bp)
app.register_blueprint(user_preferences_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(tenders_bp)

if __name__ == '__main__':
    socketio.run(app, debug=True)
