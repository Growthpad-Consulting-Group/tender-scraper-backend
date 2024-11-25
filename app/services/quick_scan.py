from flask import Blueprint, request, jsonify  # Import necessary Flask components
from flask_jwt_extended import jwt_required  # Import JWT handling utilities
import logging  # For logging operation statuses and errors
from threading import Thread  # For running background tasks
from app.scrapers.website_scraper import scrape_tenders_from_websites  # Import the scraping function

# Create a Blueprint for quick scan related routes
quick_scan_bp = Blueprint('quick_scan', __name__)

# Quick Scan API endpoint
@quick_scan_bp.route('/api/run-scan', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated to access this endpoint
def run_scan():
    """
    Starts the tender scraping process in a background thread.

    This endpoint runs the scraping function based on the parameters received in the request body.

    Returns:
        jsonify: A JSON response indicating the status of the scraping operation.
    """
    try:
        # Extract data from the incoming JSON request
        data = request.json  # Get JSON data from the request
        selected_engines = data.get('engines')  # Extract selected search engines
        time_frame = data.get('timeFrame')  # Extract the desired time frame
        file_type = data.get('fileType')  # Extract the desired file type
        terms = data.get('terms')  # Extract search terms
        region = data.get('region', 'any')  # Extract the selected region; default to 'any' if not provided

        # Validate that necessary parameters are included
        if not all([selected_engines, time_frame, file_type, terms]):
            return jsonify({"msg": "Missing parameters."}), 400  # Respond with an error if params are missing

        # It's okay for region to be None or 'any' based on your logic
        # If you want to specifically handle 'any' case, you can also do so before passing it to the scraper

        # Start the scraping process in a separate thread
        thread = Thread(target=scrape_tenders_from_websites, args=(selected_engines, time_frame, file_type, terms, region))
        thread.start()  # Start the thread

        return jsonify({"msg": "Scraping started."}), 202  # Respond with a message acknowledging request receipt
    except Exception as e:
        # Log any errors encountered during the operation
        logging.error(f"Error starting scrape: {e}")
        return jsonify({"msg": "Error starting scrape."}), 500  # Return an error response if the operation fails