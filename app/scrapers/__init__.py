from .website_scraper import scrape_tenders_from_websites
from .query_scraper import scrape_tenders_from_query
scrape_tenders_from_websites()
scrape_tenders_from_query()

__all__ = [
    'scrape_tenders_from_websites'
    'scrape_tenders_from_query'
]
