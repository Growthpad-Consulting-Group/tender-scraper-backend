from flask import Blueprint, request, jsonify, current_app
from webapp.config import get_db_connection
from flask_jwt_extended import jwt_required
from webapp.cache.redis_cache import set_cache, get_cache, delete_cache
import logging
from datetime import datetime, date  # Import both datetime and date
from dateutil.relativedelta import relativedelta  # Make sure to import this


# Create a Blueprint for Scraping log management
scraping_log_bp = Blueprint('scraping_log_bp', __name__)

# Get All Logs
@scraping_log_bp.route('/api/logs', methods=['GET'])
@jwt_required()
def get_logs():
    """Retrieve all logs from scraping_log with caching."""
    cache_key = 'scraping_logs'

    # Check if logs are in cache
    cached_logs = get_cache(cache_key)
    if cached_logs:
        return jsonify(cached_logs), 200  # Return cached logs if they exist

    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor()
        cursor.execute("SELECT * FROM scraping_log ORDER BY created_at DESC;")
        logs = cursor.fetchall()

        # Convert logs to a list of dictionaries
        logs_list = []
        for log in logs:
            logs_dict = {
                'id': log[0],
                'website_name': log[1],
                'visiting_url': log[2],
                'tenders_found': log[3],
                'tender_title': log[4],
                'closing_keyword': log[6],
                'filtered_keyword': log[7],
                'relevant': log[8],
                'status': log[9],
            }

            # Handle potential date fields
            for date_field in ['closing_date', 'created_at']:
                date_value = log[5] if date_field == 'closing_date' else log[10]
                if isinstance(date_value, (date, datetime)):  # Check against correct types
                    logs_dict[date_field] = date_value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    logs_dict[date_field] = date_value  # Keep the original value if it's already a string

            logs_list.append(logs_dict)

        # Store the fetched logs in the cache
        set_cache(cache_key, logs_list)

        return jsonify(logs_list), 200

    except Exception as e:
        logging.error(f"Error retrieving logs: {str(e)}")
        return jsonify({"error": "Failed to retrieve logs"}), 500
    finally:
        cursor.close()
        db_connection.close()

@scraping_log_bp.route('/api/logs/clear', methods=['DELETE'])
@jwt_required()
def clear_logs():
    """Clear all logs from scraping_log."""
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor()
        cursor.execute("DELETE FROM scraping_log;")
        db_connection.commit()

        # Optionally, clear cache when logs are cleared
        delete_cache('scraping_logs')

        return jsonify({"message": "All logs cleared successfully."}), 200

    except Exception as e:
        logging.error(f"Error clearing logs: {str(e)}")
        return jsonify({"error": "Failed to clear logs"}), 500
    finally:
        cursor.close()
        db_connection.close()

@scraping_log_bp.route('/api/logs/clear_by_date', methods=['DELETE'])
@jwt_required()
def clear_logs_by_date():
    """Clear logs from scraping_log based on a date threshold."""
    db_connection = None
    cursor = None
    try:
        # Get the current date and compute the desired threshold
        timeframe = request.json.get("timeframe")  # 'last3Months', 'last6Months', 'pastYear'
        threshold_date = datetime.now()

        if timeframe == 'last3Months':
            threshold_date -= relativedelta(months=3)
        elif timeframe == 'last6Months':
            threshold_date -= relativedelta(months=6)
        elif timeframe == 'pastYear':
            threshold_date -= relativedelta(years=1)
        else:
            return jsonify({"error": "Invalid timeframe specified."}), 400

        db_connection = get_db_connection()
        cursor = db_connection.cursor()

        # Check how many logs will be deleted
        cursor.execute("SELECT COUNT(*) FROM scraping_log WHERE created_at < %s;", (threshold_date,))
        count_before = cursor.fetchone()[0]
        logging.info(f'Number of logs to delete: {count_before}')

        # Perform the deletion
        cursor.execute("DELETE FROM scraping_log WHERE created_at < %s;", (threshold_date,))
        db_connection.commit()

        # Check how many logs remain after deletion
        cursor.execute("SELECT COUNT(*) FROM scraping_log WHERE created_at < %s;", (threshold_date,))
        count_after = cursor.fetchone()[0]
        logging.info(f'Number of logs remaining after delete: {count_after}')

        # Clear the cache when logs are cleared
        delete_cache('scraping_logs')

        return jsonify({"message": "Logs cleared successfully."}), 200

    except Exception as e:
        logging.error(f"Error clearing logs by date: {str(e)}")
        return jsonify({"error": "Failed to clear logs by date"}), 500
    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()

if __name__ == '__main__':
    app.run(debug=True)