# webapp/app.py
import eventlet
eventlet.monkey_patch()  # Ensure eventlet patches the standard library

from flask import request, jsonify
from dotenv import load_dotenv
from flask_jwt_extended import JWTManager
from flask_socketio import SocketIO
import logging
import atexit
import os
from datetime import timedelta

from webapp import create_app, socketio
from webapp.services.scheduler import scheduler as apscheduler
from webapp.task_service.scheduler import setup_scheduler

load_dotenv()

# Create Flask app
app = create_app()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# JWT setup
app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=60)
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = timedelta(days=14)
jwt = JWTManager(app)

# SocketIO setup with eventlet
socketio.init_app(app, cors_allowed_origins="*", async_mode="eventlet", logger=True, engineio_logger=True)

# Test SocketIO connection
@socketio.on('connect', namespace='/scraping')
def handle_connect():
    logger.info("Client connected to /scraping namespace")
    socketio.emit('test', {'msg': 'Connected to server'}, namespace='/scraping')

# Handle client disconnect gracefully
@socketio.on('disconnect', namespace='/scraping')
def handle_disconnect():
    logger.info("Client disconnected gracefully from /scraping namespace")

# Handle SocketIO errors
@socketio.on_error('/scraping')
def handle_scraping_error(e):
    logger.error(f"SocketIO error in /scraping namespace: {str(e)}")

@socketio.on_error_default
def handle_default_error(e):
    logger.error(f"SocketIO default error: {str(e)}")

# Register blueprints
from webapp.routes.auth.auth import auth_bp
from webapp.routes.dashboard import dashboard_bp
from webapp.routes.tenders import tenders_bp
from webapp.routes.keywords.keyword_routes import keyword_bp
from webapp.routes.terms.search_terms import search_terms_bp
from webapp.routes.upload.upload_routes import upload_bp
from webapp.routes.terms.relevant_keywords import relevant_keywords_bp
from webapp.routes.terms.base_keywords import base_keywords_bp
from webapp.routes.countries.countries import countries_bp
from webapp.routes.closing_keywords.closing_keywords import closing_keywords_bp
from webapp.routes.scraping_log.scraping_log import scraping_log_bp
from webapp.services.quick_scan import quick_scan_bp
from webapp.services.keep_alive import keep_alive_bp
from webapp.services.notifications_service import notifications_service_bp
from webapp.task_service import task_service_bp  # Updated import

app.register_blueprint(auth_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(tenders_bp)
app.register_blueprint(keyword_bp)
app.register_blueprint(search_terms_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(relevant_keywords_bp)
app.register_blueprint(base_keywords_bp)
app.register_blueprint(countries_bp)
app.register_blueprint(closing_keywords_bp)
app.register_blueprint(scraping_log_bp)
app.register_blueprint(quick_scan_bp)
app.register_blueprint(keep_alive_bp)
app.register_blueprint(notifications_service_bp)
app.register_blueprint(task_service_bp)

# Set up and start the scheduler
setup_scheduler(apscheduler)
apscheduler.start()

def shutdown_scheduler():
    """Shutdown the scheduler gracefully."""
    apscheduler.shutdown()
    logger.info("Scheduler shut down gracefully.")

atexit.register(shutdown_scheduler)

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5001, use_reloader=False)