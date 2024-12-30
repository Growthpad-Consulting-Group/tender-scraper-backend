from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from flask_socketio import SocketIO
import os
from .routes.auth.auth import auth_bp
from .routes.dashboard import dashboard_bp
from .routes.tenders import tenders_bp
from .services.task_service import task_manager_bp
from .services.quick_scan import quick_scan_bp
from .services.query_scan import query_scan_bp
from .extensions import socketio, jwt  # Import the extensions

# Initialize the app object here globally
app = None

def create_app():
    global app  # Make sure to use the global variable defined above
    if app is None:  # Only create the app if it's not already created
        app = Flask(__name__)
        CORS(app)  # Allow credentials for your requests

        # Load configurations
        app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')

        # Initialize extensions
        socketio.init_app(app)  # Correctly initialize the SocketIO instance
        jwt.init_app(app)

        # Define custom error handlers after instantiating JWTManager
        @jwt.unauthorized_loader
        def unauthorized_response(callback):
            return jsonify({"msg": "Missing or invalid JWT"}), 401

        @jwt.invalid_token_loader
        def invalid_token_response(callback):
            return jsonify({"msg": "Signature verification failed"}), 422

        # Initialize SocketIO in the app's context
        socketio = SocketIO(app, cors_allowed_origins='*')

        # Register blueprints for routes
        app.register_blueprint(auth_bp)
        app.register_blueprint(dashboard_bp)
        app.register_blueprint(tenders_bp)
        app.register_blueprint(task_manager_bp)
        app.register_blueprint(quick_scan_bp)
        app.register_blueprint(query_scan_bp)

    return app
