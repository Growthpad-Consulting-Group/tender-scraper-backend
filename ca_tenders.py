import requests
from bs4 import BeautifulSoup
from datetime import datetime
from config import get_db_connection  # Import the function to get a database connection
from utils import insert_tender_to_db  # Import the utility function for database insertion

def get_format(url):
    """Determine the document format based on the URL."""
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'  # Default to HTML if no specific format is found

def scrape_ca_tenders():
    """Scrapes tenders from the CA Kenya website and inserts them into the database."""
    url = "https://www.ca.go.ke/open-tenders"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to retrieve CA tenders page, status code: {response.status_code}")
        return

    print("Successfully retrieved CA tenders page.")
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table containing tenders
    table = soup.find('table', id='datatable')
    rows = table.find('tbody').find_all('tr')
    print(f"Found {len(rows)} tenders.")

    # Get the database connection
    db_connection = get_db_connection()

    try:
        for row in rows:
            columns = row.find_all('td')

            if len(columns) < 6:
                print("Row does not have enough columns. Skipping.")
                continue

            # Extracting tender details
            title = columns[2].text.strip()  # Tender Description
            description = columns[1].text.strip()  # Tender No
            end_date_str = columns[4].text.strip()  # End Date
            download_links = columns[5].find_all('a')

            # Extract the "Download Tender Document" link (second link)
            source_url = None
            if len(download_links) > 1:
                source_url = "https://www.ca.go.ke" + download_links[1]['href']  # Construct full URL

            # Log the details found
            print(f"Found tender: '{title}'")

            # Extract and parse the closing date
            try:
                closing_date = datetime.strptime(end_date_str.split(' ')[0], "%d-%b-%Y").date()
                print(f"Closing date for tender '{title}': {closing_date}")

                # Determine the status based on the closing date
                status = "open" if closing_date > datetime.now().date() else "closed"

                # Determine the document format
                format_type = get_format(source_url) if source_url else "N/A"

                tender_data = {
                    'title': title,
                    'description': description,
                    'closing_date': closing_date,
                    'source_url': source_url if source_url else "N/A",
                    'status': status,
                    'format': format_type,  # Update to use determined format
                    'scraped_at': datetime.now().date(),
                    'tender_type': "CA Tenders"  # Mapping the tender type
                }

                # Insert the tender into the database with db_connection
                insert_tender_to_db(tender_data, db_connection)
                print(f"Tender inserted into database: {title}")
                print(f"Title: {title}\n"
                      f"Description: {description}\n"
                      f"Closing Date: {closing_date}\n"
                      f"Status: {status}\n"
                      f"Source URL: {source_url}\n"
                      f"Format: {format_type}\n"  # Display the determined format
                      f"Tender Type: CA Tenders\n")  # Display the tender type
                print("=" * 40)  # Separator for readability

            except ValueError as e:
                print(f"Error parsing closing date for tender '{title}': {e}")

    except Exception as e:
        print(f"An error occurred during scraping: {e}")

    finally:
        db_connection.close()  # Ensure the connection is closed

    print("Scraping completed.")

if __name__ == "__main__":
    scrape_ca_tenders()