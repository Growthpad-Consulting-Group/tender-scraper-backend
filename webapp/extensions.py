# webapp/extensions.py
from flask_socketio import SocketIO
from flask_jwt_extended import JWTManager

socketio = SocketIO(cors_allowed_origins='*')  # Single instance, initialized later
jwt = JWTManager()  # JWTManager instance