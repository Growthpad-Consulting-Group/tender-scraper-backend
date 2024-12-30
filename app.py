# Import necessary libraries
from flask import request, jsonify
from dotenv import load_dotenv
from flask_socketio import SocketIO
from flask_jwt_extended import JWTManager
import logging
import atexit
import os
from datetime import timedelta  # Import timedelta for setting expiration times

# Import custom modules and services
from app import create_app
from app.services.scheduler import start_scheduler, shutdown_scheduler

# Load environment variables from .env file
load_dotenv()

# Create Flask app and SocketIO instance
app, socketio = create_app()  # This initializes the app and socketio

# Configure logging for the application
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# JWT setup: Retrieve secret key from environment variables
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')

# Set reasonable expiration for tokens
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=60)  # Access tokens expire in 60 minutes
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = timedelta(days=14)     # Refresh tokens expire in 14 days

# Initialize JWT
jwt = JWTManager(app)

# Start background scheduler for periodic tasks and register graceful shutdown
start_scheduler()
atexit.register(shutdown_scheduler)

# Import and register application blueprints to handle different routes
from app.routes.keywords.keyword_routes import keyword_bp
from app.routes.terms.search_terms import search_terms_bp
from app.routes.upload.upload_routes import upload_bp
from app.routes.terms.directory_keywords import directory_keywords_bp
from app.routes.terms.base_keywords import base_keywords_bp
from app.routes.countries.countries import countries_bp
from app.routes.closing_keywords.closing_keywords import closing_keywords_bp
from app.routes.scraping_log.scraping_log import scraping_log_bp

app.register_blueprint(keyword_bp)
app.register_blueprint(search_terms_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(directory_keywords_bp)
app.register_blueprint(base_keywords_bp)
app.register_blueprint(countries_bp)
app.register_blueprint(closing_keywords_bp)
app.register_blueprint(scraping_log_bp)

# Run the Flask application with SocketIO support
if __name__ == '__main__':
    socketio.run(app, debug=True)  # Start the server with debug enabled