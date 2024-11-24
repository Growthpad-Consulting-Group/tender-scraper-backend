from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity
import bcrypt
from app.config.config import get_db_connection
import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    recaptcha_token = data.get('recaptchaToken')

    # Log the request host for debugging
    print("Request Host:", request.host)

    # Retrieve the reCAPTCHA secret key from environment variables
    recaptcha_secret_key = os.getenv("RECAPTCHA_SECRET_KEY")

    # Skip reCAPTCHA for localhost or 127.0.0.1
    if not (request.host.startswith('localhost') or request.host.startswith('127.0.0.1')) and \
            'PostmanRuntime' not in request.headers.get('User-Agent', ''):
        recaptcha_response = requests.post('https://www.google.com/recaptcha/api/siteverify', data={
            'secret': recaptcha_secret_key,  # Use the secret key from the environment
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
        refresh_token = create_refresh_token(identity=username)  # Create a refresh token
        return jsonify(access_token=access_token, refresh_token=refresh_token), 200
    else:
        return jsonify({"msg": "Invalid username or password"}), 401

@auth_bp.route('/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    current_user = get_jwt_identity()
    new_access_token = create_access_token(identity=current_user)
    return jsonify(access_token=new_access_token), 200