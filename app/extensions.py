# app/extensions.py
from flask_socketio import SocketIO
from flask_jwt_extended import JWTManager

socketio = SocketIO(cors_allowed_origins='*')  # Initialize SocketIO here
jwt = JWTManager()  # Initialize JWTManager here