import time
import random
import base64
from urllib.parse import urljoin, parse_qs, unquote, urlparse, quote
from bs4 import BeautifulSoup
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webapp.routes.tenders.tender_utils import (
    is_valid_url, get_format, construct_search_url, extract_description_from_response,
    extract_closing_dates, parse_closing_date, is_relevant_tender, insert_tender_to_db,
    extract_pdf_text, extract_docx_text, fetch_relevant_keywords
)
from webapp.services.log import ScrapingLog
from datetime import datetime, date
from webapp.extensions import socketio
from webapp.task_service.utils import set_task_state, get_task_state, delete_task_state
from webapp.scrapers.constants import SEARCH_ENGINES, USER_AGENTS, EXCLUDED_DOMAINS, DISABLE_SELENIUM

def is_excluded_domains(url, excluded_domains):
    return any(domain in url for domain in excluded_domains)

def setup_selenium(use_proxy=False):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    chrome_options.add_argument("--headless")
    if use_proxy:
        chrome_options.add_argument('--proxy-server=http://your-proxy:port')
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def is_tender_related_url(url_to_check):
    """Check if a URL is likely to be related to a tender"""
    relevant_keywords = ['tender', 'rfp', 'procurement', 'bid', 'proposal', 'contract', 'eoi']
    url_lower = url_to_check.lower()
    return any(keyword in url_lower for keyword in relevant_keywords)

def scrape_tender_details(url, title, headers, db_connection):
    def scrape_page_content(page_url, page_format, max_depth=1):
        if max_depth < 0:
            ScrapingLog.add_log(f"Max depth reached for {page_url}")
            return None, None

        try:
            response = requests.get(page_url, headers=headers, timeout=5)
            response.raise_for_status()

            if page_format == 'PDF':
                content = extract_pdf_text(response.content)
                ScrapingLog.add_log(f"Extracted PDF content from {page_url}, length: {len(content)}")
                return content, []
            elif page_format == 'DOC':
                content = extract_docx_text(response.content)
                ScrapingLog.add_log(f"Extracted DOC content from {page_url}, length: {len(content)}")
                return content, []
            else:
                content = response.text
                ScrapingLog.add_log(f"Extracted HTML content from {page_url}, length: {len(content)}")

                keywords = fetch_relevant_keywords(db_connection)
                if not any(keyword.lower() in content.lower() for keyword in keywords):
                    ScrapingLog.add_log(f"Page content doesn't appear to be tender-related: {page_url}")
                    return content, []

                soup = BeautifulSoup(content, 'html.parser')
                links = soup.find_all('a', href=True)

                sub_links = []
                for link in links[:15]:
                    href = link.get('href')
                    if not href:
                        ScrapingLog.add_log(f"Skipping link with no href: {link}")
                        continue

                    if href.startswith('#') or 'login' in href.lower() or 'contact' in href.lower() or 'about' in href.lower():
                        continue

                    sub_url = urljoin(page_url, href)

                    if sub_url == page_url or urlparse(sub_url).netloc != urlparse(page_url).netloc:
                        continue

                    if is_valid_url(sub_url) and not is_excluded_domains(sub_url, EXCLUDED_DOMAINS):
                        sub_format = get_format(sub_url)
                        if sub_format in ['PDF', 'DOC']:
                            sub_links.append((sub_url, sub_format))
                        elif is_tender_related_url(sub_url):
                            sub_links.append((sub_url, sub_format))

                sub_links = sub_links[:3]
                ScrapingLog.add_log(f"Found {len(sub_links)} relevant sublinks on {page_url}")
                return content, sub_links

        except Exception as e:
            ScrapingLog.add_log(f"Error scraping page {page_url}: {str(e)}")
            return None, []

    try:
        format_type = get_format(url)
        ScrapingLog.add_log(f"========================================================\nScraping details from: {url}\nTender found: {title}")
        content, sub_links = scrape_page_content(url, format_type)

        if not content:
            ScrapingLog.add_log(f"Failed to retrieve content from {url}\n========================================================\n")
            return None, "error"

        closing_dates = extract_closing_dates(content, db_connection)
        if closing_dates:
            closing_date_str, keyword = closing_dates[0]
            closing_date = parse_closing_date(closing_date_str)
            if not closing_date:
                ScrapingLog.add_log(f"Failed to parse closing date: {closing_date_str}\n========================================================\n")
                return None, "parse_FAILED"
            ScrapingLog.add_log(f"Found closing date: {closing_date_str}")
        else:
            ScrapingLog.add_log(f"No closing dates found on main page {url}, checking subpages")
            for sub_url, sub_format in sub_links:
                ScrapingLog.add_log(f"Checking subpage: {sub_url}")
                sub_content, _ = scrape_page_content(sub_url, sub_format, max_depth=0)
                if not sub_content:
                    continue
                sub_closing_dates = extract_closing_dates(sub_content, db_connection)
                if sub_closing_dates:
                    closing_date_str, keyword = sub_closing_dates[0]
                    closing_date = parse_closing_date(closing_date_str)
                    if closing_date:
                        ScrapingLog.add_log(f"Found closing date on subpage {sub_url}: {closing_date_str}")
                        url = sub_url
                        format_type = sub_format
                        content = sub_content
                        break
            else:
                ScrapingLog.add_log(f"No closing dates found for {url} or its subpages\n========================================================\n")
                return None, "no_date"

        current_date = date.today()
        status = "expired" if closing_date < current_date else "open"
        if status == "expired":
            ScrapingLog.add_log(f"Tender expired: Closing date {closing_date} is before current date {current_date}\n========================================================\n")

        relevant, matched_keywords = is_relevant_tender(content, db_connection)
        keywords = fetch_relevant_keywords(db_connection)
        ScrapingLog.add_log(f"Relevant: {'Yes' if relevant else 'No'}, based on {'matched keywords: ' + str(matched_keywords) if relevant else 'keywords: ' + str(keywords)}")
        if not relevant:
            ScrapingLog.add_log(f"========================================================\n")
            return None, "not_relevant"

        response = requests.get(url, headers=headers, timeout=10)
        description = extract_description_from_response(response, format_type)
        tender_info = {
            "title": title,
            "description": description,
            "closing_date": closing_date,
            "source_url": url,
            "status": status,
            "scraped_at": datetime.now(),
            "format": format_type,
            "tender_type": "Search Query Tenders",
            "location": "Kenya"
        }

        if insert_tender_to_db(tender_info, db_connection):
            ScrapingLog.add_log(f"Tender stored in database: {title}\n========================================================\n")
            return tender_info, status
        else:
            ScrapingLog.add_log(f"Failed to store tender in database: {title}\n========================================================\n")
            return None, "db_FAILED"

    except Exception as e:
        ScrapingLog.add_log(f"Error scraping tender details from {url}: {str(e)}\n========================================================\n")
        return None, "error"

def serialize_tender(tender):
    return {
        "title": tender["title"],
        "description": tender["description"],
        "closing_date": tender["closing_date"].isoformat() if isinstance(tender["closing_date"], date) else tender["closing_date"],
        "scraped_at": tender["scraped_at"].isoformat() if isinstance(tender["scraped_at"], datetime) else tender["scraped_at"],
        "source_url": tender["source_url"],
        "status": tender["status"],
        "format": tender["format"],
        "tender_type": tender["tender_type"],
        "location": tender["location"]
    }

def decode_bing_url(encoded_url):
    try:
        padding_needed = len(encoded_url) % 4
        if padding_needed:
            encoded_url += "=" * (4 - padding_needed)

        decoded_bytes = base64.b64decode(encoded_url, validate=True)

        try:
            decoded_url = decoded_bytes.decode('utf-8')
        except UnicodeDecodeError:
            decoded_url = decoded_bytes.decode('utf-8', errors='ignore')

        return decoded_url
    except Exception as e:
        ScrapingLog.add_log(f"Error decoding Bing URL {encoded_url}: {str(e)}")
        return None

def scrape_tenders_from_query(db_connection, query, engines, task_id):
    # Initialize task state
    task_state = get_task_state(task_id) or {}
    start_time = task_state.get('startTime', datetime.now().isoformat())
    set_task_state(task_id, {
        'status': 'running',
        'startTime': start_time,
        'tenders': [],
        'visited_urls': [],
        'total_urls': 0,
        'summary': {},
        'cancel': False
    })

    tenders = []
    total_tenders_count = 0
    expired_tenders_count = 0
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    driver = None

    try:
        ScrapingLog.add_log(f"Starting scraping task {task_id} for query: {query}")
        # Emit initial running status
        socketio.emit('scrape_update', {
            'taskId': task_id,
            'status': 'running',
            'startTime': start_time,
            'tenders': [],
            'visitedUrls': [],
            'totalUrls': 0,
            'summary': {
                'openTenders': 0,
                'closedTenders': 0,
                'totalTenders': 0
            },
            'message': f"Started scraping for query: {query}",
        }, namespace='/scraping')

        for engine in engines:
            # Check for cancellation
            task_state = get_task_state(task_id)
            if task_state and task_state.get("cancel", False):
                ScrapingLog.add_log(f"Scraping task {task_id} canceled before scraping {engine}")
                set_task_state(task_id, {
                    'status': 'canceled',
                    'startTime': start_time,
                    'tenders': [serialize_tender(t) for t in tenders],
                    'visited_urls': task_state.get('visited_urls', []),
                    'total_urls': task_state.get('total_urls', 0),
                    'summary': {
                        'openTenders': len(tenders),
                        'closedTenders': expired_tenders_count,
                        'totalTenders': total_tenders_count
                    }
                })
                socketio.emit('scrape_update', {
                    'taskId': task_id,
                    'status': 'canceled',
                    'startTime': start_time,
                    'tenders': [serialize_tender(t) for t in tenders],
                    'visitedUrls': task_state.get('visited_urls', []),
                    'totalUrls': task_state.get('total_urls', 0),
                    'summary': {
                        'openTenders': len(tenders),
                        'closedTenders': expired_tenders_count,
                        'totalTenders': total_tenders_count
                    },
                    'message': "Scraping canceled by user",
                }, namespace='/scraping')
                return tenders

            search_url = construct_search_url(engine, query)
            if not search_url:
                ScrapingLog.add_log(f"Unsupported search engine: {engine}")
                continue

            ScrapingLog.add_log(f"Scraping {engine} for query: {query} | URL: {search_url}")
            max_retries = 3
            retry_delay = 1
            use_proxy = False
            links = []

            prefer_requests = engine in ["Yahoo"]

            if DISABLE_SELENIUM or prefer_requests:
                if engine == "Yahoo":
                    try:
                        response = requests.get(search_url, headers=headers, timeout=10)
                        html = response.text
                        soup = BeautifulSoup(html, 'html.parser')
                        link_elements = soup.select('div.dd.algo.algo-sr.relsrch h3.title a')
                        links = [
                            {
                                'href': link.get('href'),
                                'title': link.get_text(strip=True) or 'Untitled'
                            }
                            for link in link_elements
                            if link.get('href')
                        ]
                    except Exception as req_e:
                        ScrapingLog.add_log(f"Error scraping {engine} with requests: {str(req_e)}")
                        continue
            else:
                for attempt in range(max_retries):
                    try:
                        driver = setup_selenium(use_proxy=use_proxy)
                        driver.get(search_url)
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        if engine == "Ecosia":
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_all_elements_located(
                                    (By.CSS_SELECTOR, "div.mainline__result-wrapper article div.result__title a.result__link")
                                )
                            )
                            link_elements = driver.find_elements(By.CSS_SELECTOR, 
                                "div.mainline__result-wrapper article div.result__title a.result__link")
                            links = [{'href': elem.get_attribute('href'), 'title': elem.text.strip() or 'Untitled'} 
                                     for elem in link_elements]
                        elif engine == "Bing":
                            link_elements = driver.find_elements(By.CSS_SELECTOR, 'li.b_algo h2 a')
                            links = [{'href': elem.get_attribute('href'), 'title': elem.text.strip() or 'Untitled'} 
                                     for elem in link_elements]
                        elif engine == "Startpage":
                            link_elements = driver.find_elements(By.CSS_SELECTOR, 'div.result a.result-title')
                            links = [{'href': elem.get_attribute('href'), 'title': elem.text.strip() or 'Untitled'} 
                                     for elem in link_elements]
                        elif engine == "DuckDuckGo":
                            link_elements = driver.find_elements(By.CSS_SELECTOR, 'article a[data-testid="result-title-a"]')
                            links = [{'href': elem.get_attribute('href'), 'title': elem.text.strip() or 'Untitled'} 
                                     for elem in link_elements]
                            if not links:
                                link_elements = driver.find_elements(By.CSS_SELECTOR, 'article h2 a')
                                links = [{'href': elem.get_attribute('href'), 'title': elem.text.strip() or 'Untitled'} 
                                         for elem in link_elements]
                            if not links:
                                link_elements = driver.find_elements(By.CSS_SELECTOR, 'article a')
                                links = [{'href': elem.get_attribute('href'), 'title': elem.text.strip() or 'Untitled'} 
                                         for elem in link_elements]
                        break
                    except (TimeoutException, WebDriverException) as e:
                        ScrapingLog.add_log(f"Error scraping {engine} with Selenium: {str(e)}")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            if attempt == 1:
                                use_proxy = True
                        else:
                            ScrapingLog.add_log(f"Failed to scrape {engine} after {max_retries} attempts")
                            continue
                    finally:
                        if driver:
                            driver.quit()
                            driver = None

            ScrapingLog.add_log(f"Found {len(links)} links on {engine} search page")
            # Emit total URLs found
            socketio.emit('total_urls', {
                'totalUrls': len(links),
                'taskId': task_id
            }, namespace='/scraping')
            task_state = get_task_state(task_id)
            if task_state:
                task_state['total_urls'] = len(links)
                set_task_state(task_id, task_state)

            for link in links:
                # Check for cancellation
                task_state = get_task_state(task_id)
                if task_state and task_state.get("cancel", False):
                    ScrapingLog.add_log(f"Scraping task {task_id} canceled during link processing")
                    set_task_state(task_id, {
                        'status': 'canceled',
                        'startTime': start_time,
                        'tenders': [serialize_tender(t) for t in tenders],
                        'visited_urls': task_state.get('visited_urls', []),
                        'total_urls': task_state.get('total_urls', 0),
                        'summary': {
                            'openTenders': len(tenders),
                            'closedTenders': expired_tenders_count,
                            'totalTenders': total_tenders_count
                        }
                    })
                    socketio.emit('scrape_update', {
                        'taskId': task_id,
                        'status': 'canceled',
                        'startTime': start_time,
                        'tenders': [serialize_tender(t) for t in tenders],
                        'visitedUrls': task_state.get('visited_urls', []),
                        'totalUrls': task_state.get('total_urls', 0),
                        'summary': {
                            'openTenders': len(tenders),
                            'closedTenders': expired_tenders_count,
                            'totalTenders': total_tenders_count
                        },
                        'message': "Scraping canceled by user",
                    }, namespace='/scraping')
                    return tenders

                href = link['href']
                link_title = link['title']
                if not href:
                    ScrapingLog.add_log(f"Skipping link with no href: {link}")
                    continue

                actual_url = urljoin(search_url, href) if href.startswith('/') else href
                ScrapingLog.add_log(f"Initial URL: {actual_url}")

                if 'google.com/url' in actual_url:
                    query_params = parse_qs(urlparse(actual_url).query)
                    actual_url = query_params.get('q', [actual_url])[0]
                    ScrapingLog.add_log(f"Decoded Google URL: {actual_url}")
                elif 'bing.com/ck/a' in actual_url:
                    query_params = parse_qs(urlparse(actual_url).query)
                    encoded_url = query_params.get('u', [None])[0]
                    if encoded_url and encoded_url.startswith('a1aHR0c'):
                        actual_url = decode_bing_url(encoded_url)
                        if not actual_url:
                            ScrapingLog.add_log(f"Failed to decode Bing URL: {encoded_url}")
                            continue
                    else:
                        ScrapingLog.add_log(f"No encoded URL found in Bing redirect: {actual_url}")
                        continue
                    ScrapingLog.add_log(f"Decoded Bing URL: {actual_url}")
                elif 'yahoo.com' in actual_url and '/RU=' in actual_url:
                    try:
                        ru_start = actual_url.index('/RU=') + 4
                        ru_end = actual_url.index('/RK=', ru_start) if '/RK=' in actual_url[ru_start:] else len(actual_url)
                        encoded_url = actual_url[ru_start:ru_end]
                        if encoded_url:
                            decoded_url = unquote(encoded_url)
                            actual_url = decoded_url
                            ScrapingLog.add_log(f"Decoded Yahoo URL: {actual_url}")
                        else:
                            ScrapingLog.add_log(f"No RU parameter value found in Yahoo redirect: {actual_url}")
                            continue
                    except Exception as e:
                        ScrapingLog.add_log(f"Error decoding Yahoo URL {actual_url}: {str(e)}")
                        continue
                elif 'duckduckgo.com/l/?uddg=' in actual_url:
                    actual_url = unquote(actual_url.split('uddg=')[1].split('&')[0])
                    ScrapingLog.add_log(f"Decoded DuckDuckGo URL: {actual_url}")

                if not is_valid_url(actual_url):
                    ScrapingLog.add_log(f"Skipping invalid URL: {actual_url}")
                    continue
                if is_excluded_domains(actual_url, EXCLUDED_DOMAINS):
                    ScrapingLog.add_log(f"Skipping URL from excluded domain: {actual_url}")
                    continue

                ScrapingLog.add_log(f"Visiting URL: {actual_url} with title: {link_title}")
                # Emit URL visit
                socketio.emit('visit_url', {
                    'url': actual_url,
                    'taskId': task_id
                }, namespace='/scraping')
                task_state = get_task_state(task_id)
                if task_state:
                    task_state['visited_urls'].append(actual_url)
                    set_task_state(task_id, task_state)

                tender_details, status = scrape_tender_details(actual_url, link_title, headers, db_connection)

                if status in ["open", "expired", "not_relevant"]:
                    total_tenders_count += 1
                if status == "expired":
                    expired_tenders_count += 1
                if tender_details:
                    tenders.append(tender_details)
                    task_state = get_task_state(task_id)
                    if task_state:
                        task_state['tenders'] = [serialize_tender(t) for t in tenders]
                        set_task_state(task_id, task_state)

        ScrapingLog.add_log(f"Scraping completed. Total tenders found: {total_tenders_count}, Open: {len(tenders)}, Expired: {expired_tenders_count}")
        # Finalize task state and emit completion
        serialized_tenders = [serialize_tender(t) for t in tenders]
        visited_urls = get_task_state(task_id).get('visited_urls', []) if get_task_state(task_id) else []
        total_urls = get_task_state(task_id).get('total_urls', 0) if get_task_state(task_id) else 0
        set_task_state(task_id, {
            'status': 'complete',
            'startTime': start_time,
            'tenders': serialized_tenders,
            'visited_urls': visited_urls,
            'total_urls': total_urls,
            'message': f"Scraping completed for query: {query}",
            'summary': {
                'openTenders': len(tenders),
                'closedTenders': expired_tenders_count,
                'totalTenders': total_tenders_count
            }
        })
        socketio.emit('scrape_update', {
            'taskId': task_id,
            'status': 'complete',
            'startTime': start_time,
            'tenders': serialized_tenders,
            'visitedUrls': visited_urls,
            'totalUrls': total_urls,
            'summary': {
                'openTenders': len(tenders),
                'closedTenders': expired_tenders_count,
                'totalTenders': total_tenders_count
            },
            'message': f"Scraping completed for query: {query}",
        }, namespace='/scraping')
        # Clean up task state
        delete_task_state(task_id)
        return tenders

    except Exception as e:
        ScrapingLog.add_log(f"Error in scrape_tenders_from_query: {str(e)}")
        # Handle error state and emit
        visited_urls = get_task_state(task_id).get('visited_urls', []) if get_task_state(task_id) else []
        total_urls = get_task_state(task_id).get('total_urls', 0) if get_task_state(task_id) else 0
        set_task_state(task_id, {
            'status': 'error',
            'startTime': start_time,
            'tenders': [serialize_tender(t) for t in tenders],
            'visited_urls': visited_urls,
            'total_urls': total_urls,
            'message': f"Error scraping for query: {query}: {str(e)}",
            'summary': {
                'openTenders': len(tenders),
                'closedTenders': expired_tenders_count,
                'totalTenders': total_tenders_count
            }
        })
        socketio.emit('scrape_update', {
            'taskId': task_id,
            'status': 'error',
            'startTime': start_time,
            'tenders': [serialize_tender(t) for t in tenders],
            'visitedUrls': visited_urls,
            'totalUrls': total_urls,
            'summary': {
                'openTenders': len(tenders),
                'closedTenders': expired_tenders_count,
                'totalTenders': total_tenders_count
            },
            'message': f"Error scraping for query: {query}: {str(e)}",
        }, namespace='/scraping')
        # Clean up task state
        delete_task_state(task_id)
        return tenders
    finally:
        if driver:
            driver.quit()