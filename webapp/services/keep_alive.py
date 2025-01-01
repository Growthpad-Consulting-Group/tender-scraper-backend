from flask import Blueprint, jsonify  # Import necessary Flask components

# Create a Blueprint for keep alive ping
keep_alive_bp = Blueprint('keep_alive', __name__)

@keep_alive_bp.route('/api/keep-alive', methods=['GET'])
def keep_alive():
    try:
        # If successful
        return "Ping successful! More alive than a Monday morning coffee!", 200
    except Exception as e:
        # If there's an issue reaching the service
        return jsonify({"message": "Oh no! It seems this server is having a midlife crisis. " +
                                   "Please check back when it's figured things out!"}), 503