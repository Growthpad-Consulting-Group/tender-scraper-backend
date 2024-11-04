from flask import Flask, request, jsonify
import bcrypt
from config import get_db_connection
from flask_jwt_extended import create_access_token, JWTManager, jwt_required, get_jwt_identity
from flask_cors import CORS
from keyword_routes import keyword_bp
from search_terms import search_terms_bp
from user_preferences_routes import user_preferences_bp
from upload_routes import upload_bp
from scraper import scrape_tenders
from ca_tenders import scrape_ca_tenders
from undp_tenders import scrape_undp_tenders
from reliefweb_tenders import fetch_reliefweb_tenders
from scrape_jobinrwanda_tenders import scrape_jobinrwanda_tenders
from scrape_treasury_ke_tenders import scrape_treasury_ke_tenders
from website_scraper import scrape_tenders_from_websites

from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import requests

app = Flask(__name__)
CORS(app)

# JWT setup
app.config['JWT_SECRET_KEY'] = 'your_secret_key'  # Change this to a strong secret key
jwt = JWTManager(app)

# User login route
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    recaptcha_token = data.get('recaptchaToken')

    # Verify reCAPTCHA if not testing with Postman
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

    # Fetch user from the database
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

    # Call all scraping functions
    scraping_functions = [
        scrape_ca_tenders,
        fetch_reliefweb_tenders,
        scrape_jobinrwanda_tenders,
        scrape_treasury_ke_tenders,
        scrape_undp_tenders,
        scrape_tenders_from_websites,
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

atexit.register(lambda: scheduler.shutdown())

# Register blueprints
app.register_blueprint(keyword_bp)
app.register_blueprint(search_terms_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(user_preferences_bp)

if __name__ == '__main__':
    app.run(debug=True)
