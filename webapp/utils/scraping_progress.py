# In app/utils/scraping_progress.py

from flask import request, jsonify
from flask_socketio import SocketIO
from flask_jwt_extended import jwt_required
from webapp.scrapers.scraper import scrape_tenders
from webapp.scrapers.ungm_tenders import scrape_ungm_tenders
from webapp.scrapers.undp_tenders import scrape_undp_tenders
from webapp.scrapers.ppip_tenders import scrape_ppip_tenders
from webapp.scrapers.reliefweb_tenders import fetch_reliefweb_tenders
from webapp.scrapers.scrape_jobinrwanda_tenders import scrape_jobinrwanda_tenders
from webapp.scrapers.scrape_treasury_ke_tenders import scrape_treasury_ke_tenders
from webapp.scrapers.website_scraper import scrape_tenders_from_websites

def run_scraping_with_progress(socketio, tender_types):
    scraping_functions = {
        'CA Tenders': scrape_ungm_tenders,
        'ReliefWeb Jobs': fetch_reliefweb_tenders,
        'Job in Rwanda': scrape_jobinrwanda_tenders,
        'Kenya Treasury': scrape_treasury_ke_tenders,
        'UNDP': scrape_undp_tenders,
        'PPIP': scrape_ppip_tenders,
        'Website Tenders': scrape_tenders_from_websites,
        'General Tenders': scrape_tenders,
    }

    total_tasks = len(tender_types)
    for i, tender_type in enumerate(tender_types):
        function = scraping_functions.get(tender_type)
        if function:
            try:
                function()  # Call the actual scraping function
                print(f"{function.__name__} completed successfully.")
            except Exception as e:
                print(f"Error in {function.__name__}: {str(e)}")

            # Calculate and emit progress
            progress = int((i + 1) / total_tasks * 100)
            socketio.emit('progress', {'progress': progress})  # Send progress to client

    socketio.emit('scan-complete')  # Notify the client that the scan is complete

def register_scraping_routes(app, socketio):
    @app.route('/run-scraping', methods=['POST'])
    @jwt_required()
    def run_scraping():
        # Extract the tender types from the JSON body of the request
        tender_types = request.json.get('tender_types')

        if not tender_types or not isinstance(tender_types, list):
            return jsonify({"error": "Please provide a valid list of tender types."}), 400

        # Call the run_scraping_with_progress function with the current socketio instance
        run_scraping_with_progress(socketio, tender_types)
        return jsonify({"message": "Scraping started!"}), 202  # 202 indicates that the request has been accepted for processing