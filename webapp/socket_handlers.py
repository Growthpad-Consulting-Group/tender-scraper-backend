# webapp/socket_handlers.py
from webapp.task_service.utils import set_task_state  # Updated import
from webapp.extensions import socketio  # Updated import for socketio
import logging

@socketio.on('connect', namespace='/scraping')
def handle_connect():
    logging.info("Client connected to /scraping namespace")

@socketio.on('disconnect', namespace='/scraping')
def handle_disconnect():
    logging.info("Client disconnected gracefully from /scraping namespace")

@socketio.on('join_task', namespace='/scraping')
def handle_join_task(data):
    task_id = data.get('taskId')
    logging.info(f"Received join_task event for task_id: {task_id}")
    
    task_state = get_task_state(task_id)
    if task_state:
        status = task_state.get('status', 'idle')
        tenders = task_state.get('tenders', [])
        visited_urls = task_state.get('visited_urls', [])
        total_urls = task_state.get('total_urls', 0)
        summary = task_state.get('summary', {})
        start_time = task_state.get('startTime', None)  # Ensure startTime is always fetched
        logging.info(f"Task {task_id} found in Redis: status={status}, startTime={start_time}")
        socketio.emit('scrape_update', {
            'taskId': task_id,
            'status': status,
            'tenders': tenders,
            'visitedUrls': visited_urls,
            'totalUrls': total_urls,
            'summary': summary,
            'startTime': start_time
        }, namespace='/scraping')
    else:
        logging.info(f"Task {task_id} not found in Redis, emitting idle status")
        socketio.emit('scrape_update', {
            'taskId': task_id,
            'status': 'idle',
            'startTime': None
        }, namespace='/scraping')