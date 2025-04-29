# webapp/routes/tenders/tenders.py
import os
import re
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from webapp.config.config import get_db_connection
import logging
from webapp.cache.redis_cache import set_cache, get_cache, delete_cache
from datetime import date, datetime
import uuid
from webapp.scrapers.run_query_scraper import scrape_tenders_from_query
from webapp.scrapers.constants import SEARCH_ENGINES
from webapp.extensions import socketio
from webapp.task_service.utils import set_task_state, get_task_state, delete_task_state
from dotenv import load_dotenv


tenders_bp = Blueprint('tenders', __name__)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
DEFAULT_RECIPIENT_EMAIL = os.getenv("DEFAULT_RECIPIENT_EMAIL")

@tenders_bp.route('/api/tenders/search-terms', methods=['GET'])
@jwt_required()
def get_search_terms():
    current_user = get_jwt_identity()
    cache_key = f"tenders_search_terms:user:{current_user}"
    logger.info(f"💡 Reached search-terms endpoint for user: {current_user}")
    
    cached_terms = get_cache(cache_key)
    if cached_terms is not None:
        logger.info(f"Cache hit: Returning cached tender search terms for user: {current_user}")
        return jsonify({"search_terms": cached_terms}), 200

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, term, created_at FROM search_terms WHERE user_id = %s ORDER BY created_at DESC", (current_user,))
                search_terms = [
                    {
                        "id": row[0],
                        "term": row[1],
                        "created_at": row[2].isoformat()
                    }
                    for row in cur.fetchall()
                ]
                set_cache(cache_key, search_terms, expiry=1800)  # Cache for 30 minutes
        return jsonify({"search_terms": search_terms}), 200
    except Exception as e:
        logger.error(f"Error fetching search terms for user {current_user}: {str(e)}")
        return jsonify({"error": "Failed to fetch search terms"}), 500
    
@tenders_bp.route('/api/tenders/run-query', methods=['POST'])
@jwt_required()
def run_tender_query():
    try:
        data = request.get_json()
        query = data.get('query', '')
        engines = data.get('engines', SEARCH_ENGINES)
        custom_emails = data.get('custom_emails', '')
        if not query or not engines:
            return jsonify({'msg': 'Query and engines are required'}), 400

        # Validate custom_emails if provided
        if custom_emails:
            email_list = [email.strip() for email in custom_emails.split(',') if email.strip()]
            email_regex = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
            invalid_emails = [email for email in email_list if not re.match(email_regex, email)]
            if invalid_emails:
                return jsonify({"msg": f"Invalid email addresses: {', '.join(invalid_emails)}"}), 400
            custom_emails = ','.join(email_list)

        current_user = get_jwt_identity()
        task_id = str(uuid.uuid4())
        set_task_state(task_id, {
            "status": "running",
            "cancel": False,
            "tenders": [],
            "visited_urls": [],
            "total_urls": 0,
            "summary": {
                "openTenders": 0,
                "closedTenders": 0,
                "totalTenders": 0
            }
        })
        logger.info(f"Starting tender query scrape for: {query} with engines: {engines} and task_id: {task_id}")
        db_connection = get_db_connection()

        def run_scraper():
            from webapp.services.email_notifications import notify_open_tenders
            try:
                tenders = scrape_tenders_from_query(db_connection, query, engines, task_id)
                logger.debug(f"Received {len(tenders)} tenders from scrape_tenders_from_query: {[t.get('title') for t in tenders]}")
                total_tenders = len(tenders)
                open_tenders_count = len([t for t in tenders if t.get('status') == 'open'])
                closed_tenders_count = len([t for t in tenders if t.get('status') == 'expired'])

                # Serialize tenders for task state and WebSocket
                serialized_tenders = [
                    {
                        **t,
                        'closing_date': t['closing_date'].isoformat() if isinstance(t.get('closing_date'), date) else t.get('closing_date'),
                        'scraped_at': t['scraped_at'].isoformat() if isinstance(t.get('scraped_at'), datetime) else t.get('scraped_at')
                    }
                    for t in tenders
                ]

                # Update task state
                task_state = get_task_state(task_id) or {}
                task_state.update({
                    'status': 'complete',
                    'tenders': serialized_tenders,
                    'summary': {
                        'openTenders': open_tenders_count,
                        'closedTenders': closed_tenders_count,
                        'totalTenders': total_tenders
                    }
                })
                set_task_state(task_id, task_state)
                # logger.debug(f"Task state updated: {task_state}")

                # Log final counts
                logger.info(f"Scraping completed for query: {query}, found {total_tenders} tenders, Open: {open_tenders_count}, Expired: {closed_tenders_count}")

                # Send email notifications
                recipient_email = custom_emails or DEFAULT_RECIPIENT_EMAIL
                if open_tenders_count > 0 and recipient_email:
                    logger.info(f"Sending email notifications for {open_tenders_count} open tenders to: {recipient_email}")
                    try:
                        notify_open_tenders(tenders, task_id, recipient_email=recipient_email)
                    except Exception as e:
                        logger.error(f"Failed to send email notifications for task {task_id}: {str(e)}")

                # Emit WebSocket update
                socketio.emit('scrape_update', {
                    'taskId': task_id,
                    'status': 'complete',
                    'tenders': serialized_tenders,
                    'message': f"Scraping completed for query: {query}",
                    'summary': {
                        'openTenders': open_tenders_count,
                        'closedTenders': closed_tenders_count,
                        'totalTenders': total_tenders
                    }
                }, namespace='/scraping')
            except Exception as e:
                logger.error(f"Error in scraping task {task_id}: {str(e)}")
                socketio.emit('scrape_update', {
                    'taskId': task_id,
                    'status': 'error',
                    'tenders': [],
                    'message': f"Error scraping: {str(e)}",
                    'summary': {
                        'openTenders': 0,
                        'closedTenders': 0,
                        'totalTenders': 0
                    }
                }, namespace='/scraping')
            finally:
                db_connection.close()
                logger.info(f"Cleaning up task_id {task_id} from Redis")
                delete_task_state(task_id)

        socketio.start_background_task(run_scraper)
        logger.info(f"Returning task_id {task_id} to client")
        return jsonify({'msg': 'Scraping started, results will be sent via WebSocket', 'task_id': task_id}), 202

    except Exception as e:
        logger.error(f"Error starting tender query scrape: {str(e)}")
        if 'task_id' in locals():
            delete_task_state(task_id)
        return jsonify({'error': 'Failed to start scraping'}), 500

@tenders_bp.route('/api/tenders/cancel-scrape', methods=['POST'])
@jwt_required()
def cancel_scrape():
    task_id = request.json.get("task_id")
    logger.info(f"Received cancel request for task_id: {task_id}")
    task_state = get_task_state(task_id)
    if task_state:
        task_state["cancel"] = True
        set_task_state(task_id, task_state)
        logger.info(f"Set cancel flag for task_id {task_id}")
        return jsonify({"msg": "Scraping canceled"}), 200
    logger.warning(f"Task not found for task_id: {task_id}")
    return jsonify({"msg": "Task not found"}), 404

@tenders_bp.route('/api/tenders', methods=['GET', 'POST'])
@jwt_required()
def get_tenders():
    """Fetch tenders with filters (GET) or create tenders (POST)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if request.method == 'POST':
                    data = request.get_json()
                    tender_types = data.get('tenderTypes', [])
                    logger.info(f"Tender Types Querying: {tender_types}")
                    return jsonify({"msg": "POST not implemented yet"}), 501

                elif request.method == 'GET':
                    # Check for specific tender type counts
                    if request.args.get('type') == 'uploaded':
                        logger.info("Fetching tender counts for 'Uploaded Websites'")
                        cached_result = get_cache('tender_counts_uploaded')
                        if cached_result is not None:
                            return jsonify(cached_result), 200

                        cur.execute("""
                            SELECT
                                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_count,
                                SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed_count
                            FROM tenders
                            WHERE tender_type = 'Uploaded Websites'
                        """)
                        open_count, closed_count = cur.fetchone()
                        result = {"open_tenders": open_count or 0, "closed_tenders": closed_count or 0}
                        set_cache('tender_counts_uploaded', result, expiry=3600)  # Cache for 1 hour
                        return jsonify(result), 200

                    # General tender search with filters
                    query = request.args.get('query', '')
                    location = request.args.get('location', '')
                    min_budget = request.args.get('min_budget', type=float)
                    max_budget = request.args.get('max_budget', type=float)
                    start_date = request.args.get('startDate')
                    end_date = request.args.get('endDate')

                    # Create a cache key based on query parameters
                    current_user = get_jwt_identity()
                    cache_key = f"tenders:user:{current_user}:query:{query}:location:{location}:min_budget:{min_budget or ''}:max_budget:{max_budget or ''}:start_date:{start_date or ''}:end_date:{end_date or ''}"
                    cached_tenders = get_cache(cache_key)
                    if cached_tenders is not None:
                        logger.info(f"Cache hit: Returning cached tenders for user: {current_user}")
                        return jsonify({"tenders": cached_tenders}), 200

                    sql = """
                        SELECT id, title, description, closing_date, status, source_url, format, tender_type, scraped_at, location
                        FROM tenders
                        WHERE status = 'open'
                        AND (title ILIKE %s OR description ILIKE %s)
                    """
                    params = [f'%{query}%', f'%{query}%']

                    if location:
                        sql += " AND location ILIKE %s"
                        params.append(f'%{location}%')
                    if min_budget:
                        sql += " AND budget >= %s"
                        params.append(min_budget)
                    if max_budget:
                        sql += " AND budget <= %s"
                        params.append(max_budget)
                    if start_date and end_date:
                        sql += " AND closing_date BETWEEN %s AND %s"
                        params.extend([start_date, end_date])
                        logger.info(f"Filtering tenders by Date Range: {start_date} to {end_date}")

                    cur.execute(sql, params)
                    tenders = cur.fetchall()

                    tenders_list = [
                        {
                            "id": t[0],
                            "title": t[1],
                            "description": t[2] or "No description",
                            "closing_date": t[3].isoformat(),
                            "status": t[4].capitalize(),
                            "source_url": t[5],
                            "format": t[6],
                            "tender_type": t[7],
                            "scraped_at": t[8].isoformat() if t[8] else None,
                            "location": t[9]
                        }
                        for t in tenders
                    ]

                    set_cache(cache_key, tenders_list, expiry=3600)  # Cache for 1 hour
                    return jsonify({"tenders": tenders_list}), 200

    except Exception as e:
        logger.error(f"Error handling tenders request: {str(e)}")
        return jsonify({"error": "Failed to process tenders request"}), 500

@tenders_bp.route('/api/tenders/<int:tender_id>', methods=['GET'])
@jwt_required()
def get_tender_by_id(tender_id):
    """Fetch a single tender by its ID."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, title, description, closing_date, status, source_url, format, tender_type, scraped_at, location
                    FROM tenders
                    WHERE id = %s
                """, (tender_id,))
                tender = cur.fetchone()

                if not tender:
                    return jsonify({"msg": "Tender not found"}), 404

                tender_data = {
                    "id": tender[0],
                    "title": tender[1],
                    "description": tender[2] or "No description",
                    "closing_date": tender[3].isoformat(),
                    "status": tender[4].capitalize(),
                    "source_url": tender[5],
                    "format": tender[6],
                    "tender_type": tender[7],
                    "scraped_at": tender[8].isoformat() if tender[8] else None,
                    "location": tender[9]
                }
                return jsonify({"tender": tender_data}), 200

    except Exception as e:
        logger.error(f"Error fetching tender {tender_id}: {str(e)}")
        return jsonify({"error": "Failed to fetch tender"}), 500
    
@tenders_bp.route('/api/tenders/tender-types', methods=['GET'])
@jwt_required()
def get_tender_types():
    """Fetch all available tender types."""
    current_user = get_jwt_identity()
    cache_key = f"tender_types:user:{current_user}"
    logger.info(f"Fetching tender types for user: {current_user}")

    # Check cache first
    cached_tender_types = get_cache(cache_key)
    if cached_tender_types is not None:
        logger.info(f"Cache hit: Returning cached tender types for user: {current_user}")
        return jsonify({"tenderTypes": cached_tender_types}), 200

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Query distinct tender types from the tenders table
                cur.execute("SELECT DISTINCT tender_type FROM tenders WHERE tender_type IS NOT NULL ORDER BY tender_type")
                tender_types = [row[0] for row in cur.fetchall()]
                
                # Cache the result for 1 hour
                set_cache(cache_key, tender_types, expiry=3600)
                logger.info(f"Fetched {len(tender_types)} tender types for user: {current_user}")
                return jsonify({"tenderTypes": tender_types}), 200

    except Exception as e:
        logger.error(f"Error fetching tender types for user {current_user}: {str(e)}")
        return jsonify({"error": "Failed to fetch tender types"}), 500    

@tenders_bp.route('/api/tenders/counts', methods=['GET'])
@jwt_required()
def get_tender_counts():
    """Get counts of open and closed tenders by tender type."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cached_counts = get_cache('tender_counts_all')
                if cached_counts is not None:
                    return jsonify(cached_counts), 200

                cur.execute("""
                    SELECT
                        tender_type,
                        SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_count,
                        SUM(CASE WHEN status = 'closed' THEN 1 ELSE 0 END) AS closed_count
                    FROM tenders
                    GROUP BY tender_type
                """)
                counts = cur.fetchall()

                counts_dict = {
                    row[0]: {"open_tenders": row[1] or 0, "closed_tenders": row[2] or 0}
                    for row in counts
                }
                set_cache('tender_counts_all', counts_dict, expiry=3600)  # Cache for 1 hour
                return jsonify(counts_dict), 200

    except Exception as e:
        logger.error(f"Error fetching tender counts: {str(e)}")
        return jsonify({"error": "Failed to fetch tender counts"}), 500
    
    