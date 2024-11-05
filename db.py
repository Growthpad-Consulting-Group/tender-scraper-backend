import pg8000
from contextlib import closing
from config import get_db_connection

def insert_tender_to_db(tender_info, db_connection):
    """Inserts or updates tender information into the database."""
    with closing(db_connection) as conn:
        cur = conn.cursor()

        # SQL query to insert or update tender information
        cur.execute('''  
            INSERT INTO tenders (title, closing_date, source_url, status, format, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(source_url) DO UPDATE SET
                title = EXCLUDED.title,
                closing_date = EXCLUDED.closing_date,
                status = EXCLUDED.status,
                format = EXCLUDED.format,
                scraped_at = EXCLUDED.scraped_at
        ''', (tender_info['title'], tender_info['closing_date'], tender_info['source_url'],
              tender_info['status'], tender_info['format'], tender_info['scraped_at']))

        conn.commit()

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
                closing_date DATE NOT NULL,
                source_url TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                format TEXT NOT NULL,
                scraped_at DATE NOT NULL
            )
        ''')

        conn.commit()

if __name__ == "__main__":
    create_tables()
