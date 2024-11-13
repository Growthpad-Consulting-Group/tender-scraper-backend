import pg8000
from contextlib import closing
from config import get_db_connection
from datetime import datetime  # Import datetime to avoid the error
import logging

def insert_tender_to_db(tender_info, db_connection):
    """Inserts or updates tender information into the database."""
    with closing(db_connection) as conn:
        cur = conn.cursor()

        # Ensure status is set correctly
        current_date = datetime.now().date()
        tender_info['status'] = 'open' if tender_info['closing_date'] > current_date else 'closed'

        # SQL query to insert or update tender information
        insert_sql = '''  
            INSERT INTO tenders (title, description, closing_date, source_url, status, scraped_at, format, tender_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(source_url) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                closing_date = EXCLUDED.closing_date,
                status = EXCLUDED.status,
                format = EXCLUDED.format,
                scraped_at = EXCLUDED.scraped_at,
                tender_type = EXCLUDED.tender_type
        '''

        # Prepare parameters for insertion
        params = (
            tender_info['title'],
            tender_info.get('description', ''),  # Ensure you provide a default description if not found
            tender_info['closing_date'],
            tender_info['source_url'],
            tender_info['status'],
            tender_info['scraped_at'],
            tender_info['format'],
            tender_info['tender_type']
        )

        try:
            # Execute the insertion/updating query and commit
            cur.execute(insert_sql, params)
            conn.commit()
            logging.info(f"Successfully inserted/updated tender: {tender_info['title']}")
            return True  # Indicate success
        except Exception as e:
            conn.rollback()
            logging.error("Error inserting/updating tender: %s", str(e))
            return False  # Indicate failure
        finally:
            cur.close()


def get_keywords_and_terms(db_connection):
    """Retrieves keywords and their associated search terms from the database."""
    with closing(db_connection) as conn:
        cur = conn.cursor()

        # SQL to get all keywords
        cur.execute("SELECT id, keyword FROM keywords")
        keywords = cur.fetchall()

        # SQL to get all search terms
        cur.execute("SELECT term FROM search_terms")
        search_terms = [row[0] for row in cur.fetchall()]

        # Create a list to hold keyword data
        keyword_data = []

        for keyword_id, keyword_text in keywords:
            keyword_data.append({
                'id': keyword_id,
                'keyword': keyword_text,
                'terms': search_terms  # Include all search terms
            })

        return keyword_data

def create_tables():
    """Creates the necessary tables in the database."""
    connection = get_db_connection()
    with closing(connection) as conn:
        cur = conn.cursor()

        # SQL to create the tenders table if it doesn't exist
        cur.execute(''' 
            CREATE TABLE IF NOT EXISTS tenders (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT,  -- This line is correct, ensures description is included
                closing_date DATE NOT NULL,
                source_url TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                format TEXT NOT NULL,
                scraped_at DATE NOT NULL,
                tender_type TEXT NOT NULL
            )
        ''')

        conn.commit()

if __name__ == "__main__":
    create_tables()
