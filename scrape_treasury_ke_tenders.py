import requests
from bs4 import BeautifulSoup
from datetime import datetime
from config import get_db_connection
from utils import insert_tender_to_db

def scrape_treasury_ke_tenders():
    """Scrapes tenders from the Kenya Treasury website and inserts them into the database."""
    url = "https://www.treasury.go.ke/tenders/"

    try:
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed to retrieve Kenya Treasury page, status code: {response.status_code}")
            return

        print("Successfully retrieved Kenya Treasury page.")
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table
        table = soup.find('table', {'id': 'tablepress-3'})
        rows = table.find_all('tr')[1:]  # Skip the header row

        print(f"Found {len(rows)} rows in the tender table.")

        # Create the database connection
        db_connection = get_db_connection()
        current_year = datetime.now().year  # Get the current year

        try:
            for row in rows:
                columns = row.find_all('td')
                if len(columns) < 5:  # Ensure there are enough columns
                    continue

                reference_number = columns[0].text.strip()
                title = columns[1].text.strip()
                document_url = columns[2].find('a')['href'] if columns[2].find('a') else None
                deadline_str = columns[4].text.strip()

                print(f"Deadline string found for tender '{title}': {deadline_str}")

                try:
                    deadline_date = datetime.strptime(deadline_str, "%Y-%m-%d %H:%M:%S").date()
                except ValueError as e:
                    print(f"Error parsing deadline date for tender '{title}': {e}")
                    continue

                # Only process tenders with a deadline in the current year
                if deadline_date.year != current_year:
                    continue

                status = "Open" if deadline_date > datetime.now().date() else "Closed"

                tender_data = {
                    'title': title,
                    'description': reference_number,
                    'closing_date': deadline_date,
                    'source_url': document_url,
                    'status': status,
                    'format': "HTML",
                    'scraped_at': datetime.now().date()
                }

                try:
                    insert_tender_to_db(tender_data, db_connection)  # Insert the tender
                    print(f"Tender inserted into database: {title}")
                    print(f"Title: {title}\n"
                          f"Reference Number: {reference_number}\n"
                          f"Closing Date: {deadline_date}\n"
                          f"Status: {status}\n"
                          f"Format: HTML\n")
                    print("=" * 40)  # Separator for readability
                except Exception as e:
                    print(f"Error inserting tender '{title}' into database: {e}")

        finally:
            db_connection.close()  # Ensure the connection is closed

        print("Scraping completed.")

    except Exception as e:
        print(f"An error occurred while scraping: {e}")

if __name__ == "__main__":
    scrape_treasury_ke_tenders()
