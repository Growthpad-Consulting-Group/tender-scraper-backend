import requests
from datetime import datetime
from config import get_db_connection
from utils import insert_tender_to_db

def fetch_reliefweb_tenders():
    """Fetches tenders from the ReliefWeb API and inserts them into the database."""
    api_url = "https://api.reliefweb.int/v1/jobs?appname=rwint-user-0&profile=list&preset=latest&slim=1&query%5Bvalue%5D=country.id%3A131&query%5Boperator%5D=AND"

    try:
        response = requests.get(api_url)

        if response.status_code == 200:
            data = response.json()
            tenders = []

            # Get the database connection
            db_connection = get_db_connection()

            try:
                for job in data['data']:
                    title = job['fields']['title']
                    closing_date = job['fields']['date']['closing'] if 'date' in job['fields'] else None

                    if closing_date:
                        closing_date_obj = datetime.strptime(closing_date, "%Y-%m-%dT%H:%M:%S%z").date()
                        status = "Open" if closing_date_obj > datetime.now().date() else "Closed"
                        organization = job['fields']['source'][0]['name'] if job['fields'].get('source') else 'Unknown'

                        # Prepare the tender info
                        tender_info = {
                            'title': title,
                            'closing_date': closing_date_obj,
                            'source_url': f"https://reliefweb.int/job/{job['id']}",
                            'status': status,
                            'format': "HTML",  # Setting format to HTML
                            'description': organization,  # Store organization in the description column
                            'scraped_at': datetime.now().date()
                        }

                        tenders.append(tender_info)

                        # Insert into the database with db_connection
                        insert_tender_to_db(tender_info, db_connection)

                        # Print formatted log output
                        print(f"Title: {title}")
                        print(f"Organization: {organization}")
                        print(f"Closing Date: {closing_date_obj}")
                        print(f"Status: {status}")
                        print(f"Format: HTML")
                        print("=" * 40)  # Separator for readability

                    else:
                        print(f"Skipping job '{title}' due to missing closing date.")

            finally:
                db_connection.close()  # Ensure the connection is closed

            return tenders
        else:
            print(f"Failed to fetch tenders, status code: {response.status_code}")
            return []

    except Exception as e:
        print(f"An error occurred while fetching tenders: {e}")
        return []

if __name__ == "__main__":
    fetch_reliefweb_tenders()
