from flask import Blueprint, request, jsonify, url_for, redirect
from flask_jwt_extended import create_access_token, create_refresh_token, jwt_required, get_jwt_identity
from flask_cors import CORS
import bcrypt
from webapp.config.config import get_db_connection
import requests
import os
from dotenv import load_dotenv
import secrets
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from webapp.cache.redis_cache import get_cache, set_cache
import logging

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__)
CORS(auth_bp, origins=[os.getenv("FRONTEND_URL", "http://localhost:3000")])

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASS = os.getenv("EMAIL_PASS")
EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", 465))
EMAIL_SECURE = os.getenv("EMAIL_SECURE", "true").lower() == "true"
EMAIL_REPLYTO = os.getenv("EMAIL_REPLYTO")
RECAPTCHA_SECRET_KEY = os.getenv("RECAPTCHA_SECRET_KEY")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:5000")

def send_magic_link_email(to_email, token):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT name FROM users WHERE email = %s", (to_email,))
    user = cur.fetchone()
    cur.close()
    conn.close()

    name = user[0] if user and user[0] else "User"
    magic_link = f"{APP_BASE_URL}/verify?token={token}&email={to_email}"
    msg = MIMEMultipart("alternative")
    msg['Subject'] = "Your GCG Tender Management Dashboard Magic Link"
    msg['From'] = f"Growthpad Tender Management Dashboard <{EMAIL_USER}>"
    msg['To'] = to_email
    msg['Reply-To'] = EMAIL_REPLYTO

    text = f"""
    Hello {name},

    Need to log in? Click this link to access the GCG Tender Management Dashboard: {magic_link}
    This link will expire in 15 minutes. If you didn’t request this, contact strategic@growthpad.co.ke.

    Growthpad Consulting Group
    7th Floor, Mitsumi Business Park,
    Westlands – Nairobi, Kenya
    P.O. Box 1093-00606
    Phone: +254 701 850 850

    Best regards,
    The GCG Team
    """
    
    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; color: #261914;">
        <img src="https://growthpad.co.ke/wp-content/uploads/2024/10/GCG-final-logo-proposals_v6-6.png" alt="Growthpad Logo" style="display: block; margin: 0 auto 20px; max-width: 150px;">
        <h2 style="color: #f05d23; text-align: center;">Your GCG Tender Management Dashboard Magic Link</h2>
        <p style="font-size: 16px; line-height: 1.5; color: #261914;">
            Hello {name},
        </p>
        <p style="font-size: 16px; line-height: 1.5; color: #261914;">
            Need to log in? Use the link below to access the GCG Tender Management Dashboard. This is a one-time magic link for your convenience.
        </p>
        <div style="text-align: center; margin: 30px 0;">
            <a href="{magic_link}" style="background-color: #f05d23; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; display: inline-block; font-weight: bold;">Log In Now</a>
        </div>
        <p style="font-size: 14px; color: #261914; text-align: center;">
            This link will expire in <strong>15 minutes</strong>. If you didn’t request this, please contact <a href="mailto:strategic@growthpad.co.ke" style="color: #f05d23;">strategic@growthpad.co.ke</a>.
        </p>
        <hr style="border: none; border-top: 1px solid #ddd; margin: 30px 0;">
        <p style="font-size: 16px; line-height: 1.5; color: #261914;">
            <strong>Growthpad Consulting Group</strong><br/>
            7th Floor, Mitsumi Business Park,<br/>
            Westlands – Nairobi, Kenya<br/>
            P.O. Box 1093-00606<br/>
            <strong>Phone:</strong> +254 701 850 850
        </p>
        <p style="font-size: 14px; text-align: center; color: #777; margin-top: 20px;">
            Best regards,<br/>
            The GCG Team
        </p>
    </div>
    """

    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    try:
        if EMAIL_SECURE:
            with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
                server.login(EMAIL_USER, EMAIL_PASS)
                server.send_message(msg)
        else:
            with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASS)
                server.send_message(msg)
        return True
    except Exception as e:
        print(f"Email sending error: {str(e)}")
        return False

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.json
    email = data.get('email') 
    password = data.get('password')
    recaptcha_token = data.get('recaptchaToken')

    print("Request Host:", request.host)

    if not (request.host.startswith('localhost') or request.host.startswith('127.0.0.1')) and \
            'PostmanRuntime' not in request.headers.get('User-Agent', ''):
        recaptcha_response = requests.post('https://www.google.com/recaptcha/api/siteverify', data={
            'secret': RECAPTCHA_SECRET_KEY,
            'response': recaptcha_token
        })
        recaptcha_result = recaptcha_response.json()
        if not recaptcha_result.get('success'):
            return jsonify({"msg": "Invalid reCAPTCHA, please try again."}), 400

    if not email or not password:
        return jsonify({"msg": "Please provide both email and password"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT password_hash FROM users WHERE email = %s", (email,))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if user and bcrypt.checkpw(password.encode('utf-8'), user[0].encode('utf-8')):
        access_token = create_access_token(identity=email)
        refresh_token = create_refresh_token(identity=email)
        return jsonify(access_token=access_token, refresh_token=refresh_token), 200
    else:
        return jsonify({"msg": "Invalid email or password"}), 401

@auth_bp.route('/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    current_user = get_jwt_identity()
    new_access_token = create_access_token(identity=current_user)
    return jsonify(access_token=new_access_token), 200

@auth_bp.route('/magic-link', methods=['POST'])
def magic_link():
    data = request.json
    email = data.get('email').strip() if data.get('email') else None
    print(f"Received email: '{email}'")

    if not email:
        return jsonify({"msg": "Email is required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT email FROM users WHERE LOWER(email) = LOWER(%s)", (email,))
    user = cur.fetchone()
    print(f"Query result: {user}")
    
    if not user:
        cur.close()
        conn.close()
        return jsonify({"msg": "Email not found"}), 404

    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(minutes=15)
    
    cur.execute("""
        INSERT INTO magic_tokens (email, token, expires_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (email) DO UPDATE
        SET token = %s, expires_at = %s
    """, (email, token, expires_at, token, expires_at))
    
    conn.commit()
    cur.close()
    conn.close()

    if send_magic_link_email(email, token):
        return jsonify({"msg": "Magic link sent successfully"}), 200
    else:
        return jsonify({"msg": "Failed to send email"}), 500

@auth_bp.route('/verify', methods=['GET'])
def verify_magic_link_get():
    token = request.args.get('token')
    email = request.args.get('email')

    if not token or not email:
        return jsonify({"msg": "Token and email are required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT expires_at FROM magic_tokens 
        WHERE email = %s AND token = %s
    """, (email, token))
    
    result = cur.fetchone()
    
    if result and result[0] > datetime.utcnow():
        cur.execute("DELETE FROM magic_tokens WHERE email = %s", (email,))
        conn.commit()
        
        access_token = create_access_token(identity=email)
        refresh_token = create_refresh_token(identity=email)
        
        cur.close()
        conn.close()
        
        frontend_url = f"{os.getenv('FRONTEND_URL', 'http://localhost:3000')}/auth/callback?access_token={access_token}&refresh_token={refresh_token}"
        return redirect(frontend_url)
    
    cur.close()
    conn.close()
    return jsonify({"msg": "Invalid or expired token"}), 401

@auth_bp.route('/verify-magic-link', methods=['POST'])
def verify_magic_link():
    data = request.json
    token = data.get('token')
    email = data.get('email')

    if not token or not email:
        return jsonify({"msg": "Token and email are required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT expires_at FROM magic_tokens 
        WHERE email = %s AND token = %s
    """, (email, token))
    
    result = cur.fetchone()
    
    if result and result[0] > datetime.utcnow():
        cur.execute("DELETE FROM magic_tokens WHERE email = %s", (email,))
        conn.commit()
        
        access_token = create_access_token(identity=email)
        refresh_token = create_refresh_token(identity=email)
        
        cur.close()
        conn.close()
        
        return jsonify({
            "access_token": access_token,
            "refresh_token": refresh_token
        }), 200
    
    cur.close()
    conn.close()
    return jsonify({"msg": "Invalid or expired token"}), 401

@auth_bp.route('/cleanup-tokens', methods=['POST'])
def cleanup_expired_tokens():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM magic_tokens WHERE expires_at < %s", (datetime.utcnow(),))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"msg": "Expired tokens cleaned up"}), 200

@auth_bp.route('/user-profile', methods=['GET'])
@jwt_required()
def user_profile():
    """Return the authenticated user's profile info."""
    email = get_jwt_identity()  # Get email from JWT
    
    # Try to get the name from the cache
    cached_name = get_cache(f"user_name_{email}")
    if cached_name:
        logger.info(f"Returning cached name for user: {email}")
        return jsonify({"name": cached_name}), 200
    
    # Cache miss: fetch from the database
    logger.info(f"Fetching name from database for user: {email}")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT name FROM users WHERE email = %s", (email,))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if user and user[0]:
        logger.info(f"Name fetched from database for user: {email}, caching it")
        set_cache(f"user_name_{email}", user[0], expiry=3600)
        return jsonify({"name": user[0]}), 200
    else:
        logger.warning(f"User not found in database for email: {email}")
        return jsonify({"msg": "User not found"}), 404