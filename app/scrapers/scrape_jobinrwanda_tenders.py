import requests
from bs4 import BeautifulSoup
from datetime import datetime
from app.config import get_db_connection
from app.routes.tenders.tender_utils import insert_tender_to_db

def get_format(url):
    """Determine the document format based on the URL."""
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'  # Default to HTML if no specific format is found

def scrape_jobinrwanda_tenders():
    """Scrapes tenders from Job in Rwanda website and inserts them into the database."""
    url = "https://www.jobinrwanda.com/jobs/tender"

    try:
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Failed to retrieve tenders page, status code: {response.status_code}")
            return

        print("Successfully retrieved tenders page.")
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all tender cards
        tender_cards = soup.find_all('article', class_='node--type-job')
        print(f"Found {len(tender_cards)} tenders.")

        # Create a database connection
        db_connection = get_db_connection()

        try:
            for card in tender_cards:
                # Extract the title and source URL
                title_tag = card.find('h5', class_='card-title')
                if title_tag:
                    anchor_tag = title_tag.find_parent('a')
                    if anchor_tag and 'href' in anchor_tag.attrs:
                        title = anchor_tag.find('span').get_text(strip=True)
                        source_url = f"https://www.jobinrwanda.com{anchor_tag['href']}"
                    else:
                        print("Anchor tag not found or href missing.")
                        continue
                else:
                    print("Title tag not found.")
                    continue

                # Extract the description and other details
                description_tag = card.find('p', class_='card-text')
                description = description_tag.get_text(strip=True) if description_tag else "N/A"

                # Extract the deadline
                deadline_tag = description_tag.find('time', class_='datetime') if description_tag else None
                closing_date_str = deadline_tag['datetime'] if deadline_tag else None

                if closing_date_str:
                    closing_date = datetime.fromisoformat(closing_date_str.replace('Z', '+00:00')).date()
                    status = "open" if closing_date > datetime.now().date() else "closed"

                    # Determine the format based on the source URL
                    format_type = get_format(source_url)

                    tender_data = {
                        'title': title,
                        'description': description,
                        'closing_date': closing_date,
                        'source_url': source_url,
                        'status': status,
                        'format': format_type,  # Set format based on URL
                        'scraped_at': datetime.now().date(),
                        'tender_type': "Job in Rwanda"  # Specifying the tender type
                    }

                    # Insert the tender into the database
                    insert_tender_to_db(tender_data, db_connection)
                    print(f"Tender inserted into database: {title}")
                    print(f"Title: {title}\n"
                          f"Description: {description}\n"
                          f"Closing Date: {closing_date}\n"
                          f"Status: {status}\n"
                          f"Source URL: {source_url}\n"
                          f"Format: {format_type}\n"  # Display the determined format
                          f"Tender Type: Job in Rwanda\n")
                    print("=" * 40)
                else:
                    print(f"Closing date not found for tender '{title}'.")

        finally:
            db_connection.close()  # Ensure the connection is closed

        print("Scraping completed.")

    except Exception as e:
        print(f"An error occurred while scraping: {e}")

if __name__ == "__main__":
    scrape_jobinrwanda_tenders()