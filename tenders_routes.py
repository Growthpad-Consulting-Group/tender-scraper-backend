from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required
from config import get_db_connection
import logging
import datetime

tenders_bp = Blueprint('tenders_bp', __name__)

# API route to get tenders expiring soon (next 7 days)
@tenders_bp.route('/api/tenders/expiring_soon', methods=['GET'])
@jwt_required()
def get_expiring_soon_tenders():
    try:
        # Calculate the current date and the date 7 days from now
        today = datetime.datetime.now()
        next_week = today + datetime.timedelta(days=7)

        # Connect to the database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # SQL query to get tenders expiring soon
                query = """
                    SELECT title, closing_date, source_url
                    FROM tenders
                    WHERE closing_date BETWEEN %s AND %s
                    AND status = 'open'
                    ORDER BY closing_date ASC
                """
                params = (today, next_week)
                cur.execute(query, params)
                tenders = cur.fetchall()

        # Return the expiring tenders as JSON
        if not tenders:
            return jsonify({"msg": "No tenders expiring soon"}), 200

        # Convert closing_date to ISO format (if it's not already)
        tenders_data = [
            {
                "title": tender[0],
                "closing_date": tender[1].isoformat() if isinstance(tender[1], datetime.datetime) else tender[1],
                "source_url": tender[2]
            }
            for tender in tenders
        ]

        return jsonify({"tenders": tenders_data}), 200

    except Exception as e:
        logging.error(f"Error retrieving tenders: {e}")
        return jsonify({"msg": "Error retrieving tenders", "error": str(e)}), 500

# Existing API to get general tender statistics by month
@tenders_bp.route('/api/tenders', methods=['GET'])
@jwt_required()
def get_tenders():
    try:
        # Get the start and end dates from the query parameters
        start_date_str = request.args.get('startDate', type=str)
        end_date_str = request.args.get('endDate', type=str)

        # Parse the dates if they exist, otherwise set them to None
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d') if start_date_str else None
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d') if end_date_str else None

        # Connect to the database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Prepare the base query
                query = """
                    SELECT EXTRACT(MONTH FROM closing_date) AS month, 
                           COUNT(*) FILTER (WHERE LOWER(status) = 'open') AS open_count,
                           COUNT(*) FILTER (WHERE LOWER(status) = 'closed') AS closed_count
                    FROM tenders
                """

                # If dates are provided, filter by the date range
                if start_date and end_date:
                    query += " WHERE closing_date BETWEEN %s AND %s"
                    params = (start_date, end_date)
                else:
                    # If no date filter is provided, fetch all data
                    params = ()

                query += """
                    GROUP BY EXTRACT(MONTH FROM closing_date)
                    ORDER BY month
                """

                # Execute the query
                cur.execute(query, params)
                tenders = cur.fetchall()

                if not tenders:
                    # If no tenders are found, return empty data for all months
                    tenders = [(month, 0, 0) for month in range(1, 13)]  # Fill months with 0 data

                # Month names for the x-axis of the chart
                month_names = [
                    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
                ]

                # Prepare the result data for open and closed tenders
                monthly_data = {
                    "open": [0] * 12,
                    "closed": [0] * 12
                }

                for tender in tenders:
                    month = int(tender[0]) - 1  # Month is 1-indexed, so we subtract 1
                    monthly_data["open"][month] = tender[1]
                    monthly_data["closed"][month] = tender[2]

        # Return the JSON response with the tender data
        return jsonify({
            "open_tenders": monthly_data["open"],
            "closed_tenders": monthly_data["closed"],
            "month_names": month_names
        }), 200

    except Exception as e:
        logging.error(f"Error retrieving tenders: {e}")
        return jsonify({"msg": "Error retrieving tenders", "error": str(e)}), 500
