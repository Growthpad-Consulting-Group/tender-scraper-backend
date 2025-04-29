import pg8000
from contextlib import closing
from webapp.config import get_db_connection
from datetime import datetime
import logging


def insert_tender_to_db(tender_info, db_connection):
    """Inserts or updates tender information into the database."""
    cur = None  # Ensure cur is defined for the finally block
    try:
        logging.info("Starting insert for tender: %s", tender_info['title'])
        cur = db_connection.cursor()
        logging.debug(f"Cursor created: {cur}")

        # Set status based on current date
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
            tender_info.get('description', ''),  # Provide a default description if not found
            tender_info['closing_date'],
            tender_info['source_url'],
            tender_info['status'],
            tender_info['scraped_at'],
            tender_info['format'],
            tender_info['tender_type']
        )

        # Execute the insertion/updating query and commit
        logging.info("Executing insert query for tender '%s'", tender_info['title'])
        cur.execute(insert_sql, params)
        db_connection.commit()
        logging.info("Successfully inserted/updated tender: %s", tender_info['title'])
        return True  # Indicate success

    except Exception as e:
        db_connection.rollback()
        logging.error("Error inserting/updating tender '%s': %s", tender_info['title'], str(e))
        return False  # Indicate failure
    finally:
        if cur and not cur.closed:  # Close the cursor only if open
            cur.close()
            logging.info("Cursor closed successfully.")  # Log cursor closure


def get_keywords_and_terms(db_connection):
    """Retrieves keywords and their associated search terms from the database."""
    with closing(db_connection) as conn:
        cur = conn.cursor()

        try:
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
        except Exception as e:
            logging.error("Error retrieving keywords and terms: %s", str(e))
            return []


def get_relevant_keywords(db_connection):
    """Fetches keywords from the relevant_keywords table."""
    try:
        cur = db_connection.cursor()
        logging.info("Fetching all keywords from relevant_keywords")
        cur.execute("SELECT keyword FROM relevant_keywords")
        keywords = [row[0] for row in cur.fetchall()]
        logging.info(f"Fetched keywords: {keywords}")
        cur.close()
        return keywords
    except Exception as e:
        logging.error(f"Error retrieving keywords: {str(e)}")
        return []


def rename_relevant_keyword(db_connection, old_keyword, new_keyword):
    """Renames a keyword in the relevant_keywords table."""
    try:
        cur = db_connection.cursor()

        # SQL query to rename the keyword
        update_sql = '''
            UPDATE relevant_keywords
            SET keyword = %s
            WHERE keyword = %s
        '''

        # Execute the update query
        cur.execute(update_sql, (new_keyword, old_keyword))
        db_connection.commit()

        if cur.rowcount == 0:
            # No rows updated means the old keyword doesn't exist
            raise Exception(f"Keyword '{old_keyword}' does not exist.")

        logging.info(f"Successfully renamed relevant keyword from '{old_keyword}' to '{new_keyword}'")
        return True  # Indicate success

    except Exception as e:
        db_connection.rollback()
        logging.error(f"Error renaming relevant keyword from '{old_keyword}' to '{new_keyword}': {str(e)}")
        raise
    finally:
        if cur and not cur.closed:  # Close the cursor only if open
            cur.close()


def add_relevant_keyword_to_db(db_connection, keyword):
    """Adds a new keyword to the relevant_keywords table."""
    cur = None  # Ensure cur is defined for the finally block
    try:
        cur = db_connection.cursor()

        # SQL query to insert a new keyword
        insert_sql = '''
            INSERT INTO relevant_keywords (keyword)
            VALUES (%s)
            RETURNING id
        '''

        logging.info(f"Attempting to add keyword: {keyword}")

        # Execute the insertion query
        cur.execute(insert_sql, (keyword,))
        db_connection.commit()

        new_id = cur.fetchone()[0]  # Get the new keyword ID
        logging.info(f"Successfully added new relevant keyword with ID: {new_id}")
        return new_id  # Return the new keyword ID

    except Exception as e:
        db_connection.rollback()
        logging.error(f"Error adding relevant keyword '{keyword}': {str(e)}")
        raise  # Reraise the exception for handling in the calling code
    finally:
        if cur:  # Always check if cur is defined before trying to close
            cur.close()  # Close the cursor unconditionally
            logging.info("Cursor closed successfully.")


def remove_relevant_keyword_from_db(cur, keyword):
    """Removes a keyword from the relevant_keywords table."""
    try:
        # SQL query to remove the keyword
        delete_sql = '''
            DELETE FROM relevant_keywords
            WHERE keyword = %s
        '''

        # Execute the delete query
        cur.execute(delete_sql, (keyword,))
        cur.connection.commit()  # Commit using the cursor's connection

        if cur.rowcount == 0:
            raise Exception(f"Keyword '{keyword}' does not exist.")

        logging.info(f"Successfully removed relevant keyword: {keyword}")
        return True  # Indicate success

    except Exception as e:
        cur.connection.rollback()  # Rollback using the cursor's connection
        logging.error(f"Error removing relevant keyword '{keyword}': {str(e)}")
        raise  # Reraise the exception for handling in the calling code
    finally:
        # Simply attempt to close the cursor, without checking `cur.closed`
        try:
            cur.close()  # Attempt to close the cursor explicitly
            logging.info(f"Cursor for keyword '{keyword}' closed successfully.")
        except Exception as cursor_error:
            logging.warning(f"Error closing cursor: {str(cursor_error)}")


def create_tables():
    """Creates the necessary tables in the database."""
    connection = get_db_connection()
    try:
        with closing(connection) as conn:
            cur = conn.cursor()

            # SQL to create the tenders table if it doesn't exist
            cur.execute('''
                CREATE TABLE IF NOT EXISTS tenders (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    closing_date DATE NOT NULL,
                    source_url TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL,
                    format TEXT NOT NULL,
                    scraped_at DATE NOT NULL,
                    tender_type TEXT NOT NULL
                )
            ''')

            # SQL to create the relevant_keywords table if it doesn't exist
            cur.execute('''
                CREATE TABLE IF NOT EXISTS relevant_keywords (
                    id SERIAL PRIMARY KEY,
                    keyword TEXT NOT NULL
                )
            ''')

            conn.commit()
            logging.info("Tenders and relevant_keywords tables created or already exist.")

    except Exception as e:
        logging.error("Error creating tables: %s", str(e))


if __name__ == "__main__":
    create_tables()