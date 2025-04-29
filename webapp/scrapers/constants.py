# webapp/scrapers/constants.py

# List of search engines for query scraping
SEARCH_ENGINES = ["Bing", "Startpage", "Ecosia", "Yahoo", "DuckDuckGo"]

# User agents for HTTP requests
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
]

# Domains to exclude from scraping
EXCLUDED_DOMAINS = [
    "facebook.com", "linkedin.com", "twitter.com", "instagram.com", "pinterest.com",
    "youtube.com", "wikipedia.org", "reddit.com", "tiktok.com", "snapchat.com"
]

# Flag to disable Selenium for testing (set to True to use requests only)
DISABLE_SELENIUM = False