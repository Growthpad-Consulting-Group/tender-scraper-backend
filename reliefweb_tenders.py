import requests
from datetime import datetime
from config import get_db_connection
from utils import insert_tender_to_db

def get_format(url):
    """Determine the document format based on the URL."""
    if url.lower().endswith('.pdf'):
        return 'PDF'
    elif url.lower().endswith('.docx'):
        return 'DOCX'
    return 'HTML'  # Default to HTML if no specific format is found

def fetch_reliefweb_tenders():
    """Fetches tenders from the ReliefWeb API and inserts them into the database."""
    api_url = "https://api.reliefweb.int/v1/jobs?appname=rwint-user-0&profile=list&preset=latest&slim=1&query%5Bvalue%5D=country.id%3A131&query%5Boperator%5D=AND"

    try:
        response = requests.get(api_url)

        if response.status_code != 200:
            print(f"Failed to fetch tenders, status code: {response.status_code}")
            return []

        data = response.json()
        tenders = []

        # Get the database connection
        db_connection = get_db_connection()

        try:
            for job in data['data']:
                title = job['fields']['title']
                closing_date = job['fields']['date'].get('closing') if 'date' in job['fields'] else None

                if closing_date:
                    closing_date_obj = datetime.strptime(closing_date, "%Y-%m-%dT%H:%M:%S%z").date()
                    status = "open" if closing_date_obj > datetime.now().date() else "closed"
                    organization = job['fields']['source'][0]['name'] if job['fields'].get('source') else 'Unknown'

                    # Prepare the tender info
                    source_url = f"https://reliefweb.int/job/{job['id']}"
                    format_type = get_format(source_url)  # Determine the document format

                    tender_info = {
                        'title': title,
                        'closing_date': closing_date_obj,
                        'source_url': source_url,
                        'status': status,
                        'format': format_type,  # Set format based on URL
                        'description': organization,  # Store organization in the description column
                        'scraped_at': datetime.now().date(),
                        'tender_type': "ReliefWeb Jobs"  # Adding the tender type
                    }

                    tenders.append(tender_info)

                    # Insert into the database with db_connection
                    insert_tender_to_db(tender_info, db_connection)

                    # Print formatted log output
                    print(f"Title: {title}")
                    print(f"Organization: {organization}")
                    print(f"Closing Date: {closing_date_obj}")
                    print(f"Status: {status}")
                    print(f"Format: {format_type}")  # Display the determined format
                    print(f"Tender Type: ReliefWeb Jobs")
                    print("=" * 40)  # Separator for readability

                else:
                    print(f"Skipping job '{title}' due to missing closing date.")

        finally:
            db_connection.close()  # Ensure the connection is closed

        return tenders

    except Exception as e:
        print(f"An error occurred while fetching tenders: {e}")
        return []

if __name__ == "__main__":
    fetch_reliefweb_tenders()