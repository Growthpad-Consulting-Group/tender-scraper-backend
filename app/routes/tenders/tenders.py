from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, JWTManager, jwt_required, get_jwt_identity
import bcrypt
from app.config import get_db_connection
import requests
import logging
from app.cache.redis_cache import set_cache, get_cache, delete_cache
import datetime

tenders_bp = Blueprint('tenders', __name__)

# Unified tender fetching route
@tenders_bp.route('/api/tenders', methods=['GET', 'POST'])
@jwt_required()
def get_tenders():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Handle POST request
        if request.method == 'POST':
            data = request.get_json()
            tender_types = data.get('tenderTypes', [])
            logging.info(f"Tender Types Querying: {tender_types}")

            # Existing logic...

        elif request.method == 'GET':
            # Check if we're fetching tender counts for "Uploaded Websites"
            if request.args.get('type') == 'uploaded':
                logging.info("Fetching tender counts for 'Uploaded Websites'")

                # Check the cache first
                cached_result = get_cache('tender_counts_uploaded')
                if cached_result:
                    return jsonify(cached_result), 200  # Return cached response

                # Proceed to query the database
                cur.execute("""
                    SELECT COUNT(*) FROM tenders WHERE tender_type = 'Uploaded Websites' AND status = 'open'
                """)
                open_count = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(*) FROM tenders WHERE tender_type = 'Uploaded Websites' AND status = 'closed'
                """)
                closed_count = cur.fetchone()[0]

                result = {
                    "open_tenders": open_count,
                    "closed_tenders": closed_count
                }

                # Cache the result
                set_cache('tender_counts_uploaded', result)

                return jsonify(result), 200

            # Proceed to fetch all tenders with possible date filtering
            logging.info("Fetching all tenders with possible date filtering.")
            start_date = request.args.get('startDate')
            end_date = request.args.get('endDate')

            query = "SELECT title, description, closing_date, status, source_url, format, tender_type, scraped_at FROM tenders"
            query_params = []

            if start_date and end_date:
                query += " WHERE closing_date BETWEEN %s AND %s"
                query_params = (start_date, end_date)
                logging.info(f"Filtering tenders by Date Range: {start_date} to {end_date}")

            logging.info(f"Executing query: {query} with params: {query_params}")
            cur.execute(query, query_params)
            tenders = cur.fetchall()

            # Log total records found
            total_records = len(tenders)
            logging.info(f"Total records found in tenders table: {total_records}")

        # Transform fetched tenders into a list of dictionaries
        tenders_list = [{
            "title": tender[0],
            "description": tender[1] if tender[1] is not None else "No description",
            "closing_date": tender[2],
            "status": tender[3].capitalize(),
            "source_url": tender[4],
            "format": tender[5],
            "tender_type": tender[6],
            "scraped_at": tender[7]
        } for tender in tenders]

        open_tenders = [tender for tender in tenders_list if tender["status"].lower() == "open"]
        closed_tenders = [tender for tender in tenders_list if tender["status"].lower() == "closed"]

        logging.info(f"Open tenders count: {len(open_tenders)}")
        logging.info(f"Closed tenders count: {len(closed_tenders)}")

        return jsonify({
            "open_tenders": open_tenders,
            "closed_tenders": closed_tenders,
            "total_tenders": total_records,
            "month_names": ["January", "February", "March", "April", "May", "June", "July", "August", "September",
                            "October", "November", "December"]
        }), 200

    except Exception as e:
        logging.error("Error fetching tenders: %s", str(e))

        return jsonify({"error": "An error occurred while fetching tenders."}), 500
    finally:
        cur.close()
        conn.close()

# New endpoint for expiring soon tenders
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
