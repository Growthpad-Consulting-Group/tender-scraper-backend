# from flask import Blueprint, request, jsonify  # Import necessary Flask components
# from flask_jwt_extended import jwt_required  # Import JWT handling utilities
# from threading import Thread  # For running background tasks
# from webapp.scrapers.query_scraper import scrape_tenders_from_query  # Import the scraping function
# from webapp.services.log import ScrapingLog  # Import your logging class
# from webapp.scrapers.scraper_status import scraping_status


# # Create a Blueprint for query scan related routes
# query_scan_bp = Blueprint('query_scan', __name__)

# @query_scan_bp.route('/api/get-progress-log', methods=['GET'])
# @jwt_required()
# def get_progress_log():
#     try:
#         # Fetch logs using the existing method
#         logs = ScrapingLog.get_logs()  # Use the correct method to fetch logs
#         # Include scraping status in the response
#         return jsonify({"logs": logs, "scrapingComplete": scraping_status['complete']}), 200
#     except Exception as e:
#         ScrapingLog.add_log(f"Error fetching progress log: {e}")  # Replaced logging.error with ScrapingLog.add_log
#         return jsonify({"msg": "Error fetching progress log."}), 500

# # Query Scan API endpoint
# @query_scan_bp.route('/api/run-query-scan', methods=['POST'])
# @jwt_required()
# def query_scan():
#     global scraping_status  # Use global variable
#     scraping_status['complete'] = False  # Reset status at start

#     try:
#         data = request.json
#         selected_engines = data.get('engines')
#         time_frame = data.get('timeFrame')
#         file_type = data.get('fileType')
#         terms = data.get('terms')
#         region = data.get('region')  # Extract region from payload

#         # Add logging here to see what the backend receives
#         ScrapingLog.add_log(f"Received payload: Engines: {selected_engines}, TimeFrame: {time_frame}, FileType: {file_type}, Terms: {terms}, Region: {region}")

#         if not all([selected_engines, time_frame, file_type, terms]):
#             return jsonify({"msg": "Missing parameters."}), 400

#         # Handle the case where region is empty or missing
#         if region is None:  # If region is not provided, set a default or handle accordingly
#             region = ""

#         # Continue with the scraping thread, passing region as an argument
#         thread = Thread(target=scrape_tenders_from_query, args=(selected_engines, time_frame, file_type, terms, region))
#         thread.start()

#         return jsonify({"msg": "Scraping started."}), 202
#     except Exception as e:
#         ScrapingLog.add_log(f"Error starting scrape: {e}")
#         return jsonify({"msg": "Error starting scrape."}), 500
