from flask import Flask, jsonify, g
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from webapp.extensions import socketio, jwt
import os
from webapp.config import close_db_connection
import logging
from webapp import socket_handlers  # Import socket_handlers to register SocketIO handlers

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_app():
    app = Flask(__name__)
    CORS(app)

    app.config['JWT_SECRET_KEY'] = os.getenv('JWT_SECRET_KEY')
    jwt.init_app(app)  # Initialize JWT only

    @jwt.unauthorized_loader
    def unauthorized_response(callback):
        return jsonify({"msg": "Missing or invalid JWT"}), 401

    @jwt.invalid_token_loader
    def invalid_token_response(callback):
        return jsonify({"msg": "Signature verification failed"}), 422

    # Register teardown handler to close database connections
    @app.teardown_appcontext
    def teardown_db(exception):
        """Close the database connection at the end of the request."""
        logging.info("Teardown appcontext called, closing database connection.")
        conn = g.pop('db', None)
        if conn is not None:
            close_db_connection(conn)
        else:
            logging.debug("No database connection to close in this context.")

    # Initialize SocketIO with the app
    socketio.init_app(app)

    return app