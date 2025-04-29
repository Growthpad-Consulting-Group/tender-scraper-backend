import logging
from datetime import datetime, timedelta
from webapp.config import get_db_connection, close_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delete_expired_tenders():
    """
    Deletes tenders from the tenders table where the closing_date is more than one month in the past.
    """
    # Define "one month" as 30 days
    ONE_MONTH = timedelta(days=30)
    current_date = datetime.now()

    # Get database connection
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Query all tenders with their closing_date
        cur.execute("SELECT id, title, closing_date FROM tenders")
        tenders = cur.fetchall()

        deleted_count = 0
        for tender in tenders:
            tender_id, title, closing_date = tender

            # Skip if closing_date is None
            if not closing_date:
                logger.warning(f"Tender ID {tender_id} ({title}) has no closing_date, skipping.")
                continue

            # Ensure closing_date is a datetime object
            if isinstance(closing_date, str):
                try:
                    closing_date = datetime.fromisoformat(closing_date.replace('Z', '+00:00'))
                except ValueError as e:
                    logger.error(f"Failed to parse closing_date for tender ID {tender_id} ({title}): {str(e)}")
                    continue

            # Calculate the expiration threshold (closing_date + 1 month)
            expiration_threshold = closing_date + ONE_MONTH

            # Check if the tender has expired by more than one month
            if current_date > expiration_threshold:
                try:
                    # Delete the tender
                    cur.execute("DELETE FROM tenders WHERE id = %s", (tender_id,))
                    conn.commit()
                    deleted_count += 1
                    logger.info(f"Deleted expired tender ID {tender_id} ({title}), closed on {closing_date}")
                except Exception as e:
                    logger.error(f"Failed to delete tender ID {tender_id} ({title}): {str(e)}")
                    conn.rollback()

        logger.info(f"Finished checking tenders. Deleted {deleted_count} expired tenders.")

    except Exception as e:
        logger.error(f"Error while deleting expired tenders: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if conn is not None:
            close_db_connection(conn)

if __name__ == "__main__":
    delete_expired_tenders()