from flask import Blueprint, request, jsonify  # Import necessary Flask components
from flask_jwt_extended import jwt_required  # Import JWT handling utilities
from threading import Thread  # For running background tasks
from app.scrapers.website_scraper import scrape_tenders_from_websites  # Import the scraping function
from app.services.log import ScrapingLog  # Import your logging class
from app.scrapers.scraper_status import scraping_status


# Create a Blueprint for quick scan related routes
quick_scan_bp = Blueprint('quick_scan', __name__)

# Global variable to represent scraping status

@quick_scan_bp.route('/api/get-progress-log', methods=['GET'])
@jwt_required()
def get_progress_log():
    try:
        logs = ScrapingLog.get_logs()  # Fetch logs
        # Include scraping status in the response
        return jsonify({
            "logs": logs,
            "scrapingComplete": scraping_status['complete'],
            "total_found": scraping_status['total_found'],
            "relevant_count": scraping_status['relevant_count'],
            "irrelevant_count": scraping_status['irrelevant_count'],
            "open_count": scraping_status['open_count'],
            "closed_count": scraping_status['closed_count'],
            "tenders": scraping_status['tenders'],  # Return the detailed tenders
        }), 200
    except Exception as e:
        ScrapingLog.add_log(f"Error fetching progress log: {e}")
        return jsonify({"msg": "Error fetching progress log."}), 500


# Quick Scan API endpoint
@quick_scan_bp.route('/api/run-scan', methods=['POST'])
@jwt_required()
def run_scan():
    global scraping_status  # Use global variable
    scraping_status['complete'] = False  # Reset status at start

    try:
        # Extract data from the incoming JSON request
        data = request.json
        selected_engines = data.get('engines')
        time_frame = data.get('timeFrame')
        file_type = data.get('fileType')
        terms = data.get('terms')
        website = data.get('website')  # Get the selected website if present

        # Log received terms and website for debugging
        ScrapingLog.add_log(f"Received terms: {terms}, Selected website: {website}")

        # Validate that necessary parameters are included
        if not all([selected_engines, time_frame, file_type, terms]):
            return jsonify({"msg": "Missing parameters."}), 400

        # Start the scraping process in a separate thread
        thread = Thread(target=scrape_tenders_from_websites, args=(selected_engines, time_frame, file_type, terms, website))
        thread.start()

        return jsonify({"msg": "Scraping started."}), 202
    except Exception as e:
        ScrapingLog.add_log(f"Error starting scrape: {e}")
        return jsonify({"msg": "Error starting scrape."}), 500
