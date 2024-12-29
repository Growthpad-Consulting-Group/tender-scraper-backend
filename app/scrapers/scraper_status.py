# app/scrapers/scraper_status.py
scraping_status = {
    'complete': False,
    'total_found': 0,
    'relevant_count': 0,
    'irrelevant_count': 0,
    'open_count': 0,
    'closed_count': 0,
    'tenders': []  # New field to store tenders' detailed info
}