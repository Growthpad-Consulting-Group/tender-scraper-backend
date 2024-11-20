from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, JWTManager, jwt_required, get_jwt_identity
import logging
from threading import Thread
from app.scrapers.website_scraper import scrape_tenders_from_websites




quick_scan_bp = Blueprint('quick_scan', __name__)

# Quick Scan
@quick_scan_bp.route('/api/run-scan', methods=['POST'])
@jwt_required()
def run_scan():
    try:
        # Extract data from the request
        data = request.json
        selected_engines = data.get('engines')
        time_frame = data.get('timeFrame')
        file_type = data.get('fileType')
        terms = data.get('terms')

        # Start the scraping process in a separate thread
        thread = Thread(target=scrape_tenders_from_websites, args=(selected_engines, time_frame, file_type, terms))
        thread.start()

        return jsonify({"msg": "Scraping started."}), 202  # Acknowledge the request
    except Exception as e:
        logging.error(f"Error starting scrape: {e}")
        return jsonify({"msg": "Error starting scrape."}), 500
