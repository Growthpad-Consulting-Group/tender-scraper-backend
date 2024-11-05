import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from config import get_db_connection
from utils import insert_tender_to_db

def scrape_undp_tenders():
    """Scrapes tenders from the UNDP procurement notices page and inserts them into the database."""
    url = "https://procurement-notices.undp.org/"

    try:
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed to retrieve UNDP page, status code: {response.status_code}")
            return

        print("Successfully retrieved UNDP page.")
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all tender links
        tenders = soup.find_all('a', class_='vacanciesTableLink')
        print(f"Found {len(tenders)} tenders.")

        db_connection = get_db_connection()  # Get the database connection

        for tender in tenders:
            title_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Title' in x.strip())
            title = title_label.find_next_sibling('span').text.strip() if title_label else "N/A"

            # Only process tenders with "Kenya" in the title
            if 'Kenya' not in title:
                continue

            ref_no_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Ref No' in x.strip())
            reference_number = ref_no_label.find_next_sibling('span').text.strip() if ref_no_label else "N/A"

            deadline_label = tender.find('div', class_='vacanciesTable__cell__label', string=lambda x: x and 'Deadline' in x.strip())
            deadline_str = deadline_label.find_next_sibling('span').find('nobr').text.strip() if deadline_label and deadline_label.find_next_sibling('span').find('nobr') else "N/A"

            print(f"Deadline string found for tender '{title}': {deadline_str}")

            try:
                match = re.search(r'(\d{1,2}-\w{3}-\d{2})', deadline_str)
                if match:
                    cleaned_date = match.group(1)
                    print(f"Cleaned date part: '{cleaned_date}'")
                    deadline_date = datetime.strptime(cleaned_date, "%d-%b-%y").date()
                else:
                    raise ValueError("Date not found in the deadline string.")
            except ValueError as e:
                print(f"Error parsing deadline date for tender '{title}': {e}")
                continue

            status = "Open" if deadline_date > datetime.now().date() else "Closed"
            negotiation_id = tender['href'].split('=')[-1]
            source_url = f"https://procurement-notices.undp.org/view_negotiation.cfm?nego_id={negotiation_id}"

            tender_data = {
                'title': title,
                'description': reference_number,
                'closing_date': deadline_date,
                'source_url': source_url,
                'status': status,
                'format': "HTML",
                'scraped_at': datetime.now().date()
            }

            try:
                insert_tender_to_db(tender_data, db_connection)  # Insert into the database
                print(f"Tender inserted into database: {title}")
                print(f"Title: {title}\n"
                      f"Organization: {reference_number}\n"
                      f"Closing Date: {deadline_date}\n"
                      f"Status: {status}\n"
                      f"Format: HTML\n")
                print("=" * 40)  # Separator for readability
            except Exception as e:
                print(f"Error inserting tender '{title}' into database: {e}")

        print("Scraping completed.")

    except Exception as e:
        print(f"An error occurred while scraping: {e}")
    finally:
        db_connection.close()  # Ensure the connection is closed

if __name__ == "__main__":
    scrape_undp_tenders()
