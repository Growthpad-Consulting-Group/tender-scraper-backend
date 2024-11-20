from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_socketio import SocketIO
import os
from .routes.auth.auth import auth_bp
from .routes.dashboard import dashboard_bp  # Correct import for dashboard
from .routes.tenders import tenders_bp
from .services.task_service import task_manager_bp
from .services.quick_scan import quick_scan_bp

def create_app():
    app = Flask(__name__)
    CORS(app)  # Allow credentials for your requests

    # Load configurations (you can create your config.py file)
    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')

# Initialize JWT manager
    jwt = JWTManager(app)

    # Initialize SocketIO
    socketio = SocketIO(app, cors_allowed_origins='*')

    # Register blueprints for routes
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(tenders_bp)
    app.register_blueprint(task_manager_bp)
    app.register_blueprint(quick_scan_bp)

    return app, socketio
