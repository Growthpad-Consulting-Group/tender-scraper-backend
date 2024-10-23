from flask import Flask, request, jsonify
import bcrypt
from config import get_db_connection
from flask_jwt_extended import create_access_token, JWTManager, jwt_required, get_jwt_identity
from flask_cors import CORS
from keyword_routes import keyword_bp
from user_preferences_routes import user_preferences_bp  # Import the new blueprint
from upload_routes import upload_bp  # Import the new blueprint
from scraper import scrape_tenders  # Import the scraping function

import requests

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})  # Allow your frontend origin

# JWT setup
app.config['JWT_SECRET_KEY'] = 'your_secret_key'  # Change this to a strong secret key
jwt = JWTManager(app)

# User login route
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    recaptcha_token = data.get('recaptchaToken')  # Get reCAPTCHA token

    # Log received data
    print(f"Received username: {username}")
    print(f"Received password: {password}")
    print(f"Received token: {recaptcha_token}")
    print(f"User-Agent: {request.headers.get('User-Agent')}")  # Log User-Agent

    # Check for Postman User-Agent
    if 'PostmanRuntime' in request.headers.get('User-Agent', ''):
        print("Skipping reCAPTCHA verification for Postman testing")
    else:
        # Verify reCAPTCHA
        recaptcha_response = requests.post('https://www.google.com/recaptcha/api/siteverify', data={
            'secret': '6LcAkewkAAAAAPSABLLl-G3tdvzPJCmou67uZtKc',  # Your secret key here
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

    # Log user data
    print(f"Fetched user: {user}")

    if user:
        # Compare password hash with the input
        if bcrypt.checkpw(password.encode('utf-8'), user[0].encode('utf-8')):
            access_token = create_access_token(identity=username)
            return jsonify(access_token=access_token), 200
        else:
            print("Invalid password")  # Log for debugging
            return jsonify({"msg": "Invalid password"}), 401
    else:
        print("User not found")  # Log for debugging
        return jsonify({"msg": "User not found"}), 404

    cur.close()
    conn.close()

# Protected route for dashboard
@app.route('/dashboard', methods=['GET'])
@jwt_required()
def dashboard():
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user), 200

# New endpoint for scraping tenders
@app.route('/api/scrape', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated
def scrape():
    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch keywords from the database
    cur.execute("SELECT keyword FROM keywords")
    keywords = [row[0] for row in cur.fetchall()]

    # Define the search engines to be used
    search_engines = ["Google", "Bing", "Yahoo", "DuckDuckGo", "Ask"]

    # Call the scraping function with search engines and keywords
    tenders = scrape_tenders(search_engines, keywords)

    return jsonify({"msg": "Scraping completed", "tenders_found": len(tenders)}), 200


# Register the keyword blueprint before running the app
app.register_blueprint(keyword_bp)

# Register the upload blueprint
app.register_blueprint(upload_bp)

# Register the user preferences blueprint
app.register_blueprint(user_preferences_bp)

if __name__ == '__main__':
    app.run(debug=True)
