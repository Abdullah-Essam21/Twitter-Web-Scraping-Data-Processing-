import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlencode, quote_plus
import time
import random
import os
from construct_url import construct_nitter_url
import shutil
# Install dependencies: pip install selenium
# Requires geckodriver in PATH
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_driver():
    """Sets up a headless Firefox WebDriver."""
    try:
        options = Options()
        # Updated for newer Selenium versions
        options.add_argument("--headless")
        options.set_preference("general.useragent.override", 
                              "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0")
        service = Service()
        driver = webdriver.Firefox(service=service, options=options)
        return driver
    except Exception as e:
        logger.error(f"Error setting up WebDriver: {e}")
        return None

def nitter_html_generator(start_url, output_dir, max_pages=None, delay_min=2, delay_max=5, resume_file='last_nitter_url.txt', url_log_file='visited_urls.txt'):
    """Generator that yields HTML strings and logs URLs for Nitter search pages."""
    driver = setup_driver()
    if not driver:
        return

    url = start_url
    if os.path.exists(resume_file):
        with open(resume_file, 'r') as f:
            resume_url = f.read().strip()
            if resume_url:
                url = resume_url
                logger.info(f"Resuming from: {url}")

    page = 0
    try:
        while True:
            try:
                driver.get(url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.timeline-item, div.show-more"))
                )
                html = driver.page_source
                logger.info(f"Fetching {url} - Length: {len(html)}")

                # Save HTML
                html_file = f"page_{page + 1}.html"
                html_dir_path = os.path.join(output_dir, html_file)
                with open(html_dir_path, "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(f"Saved HTML to {html_dir_path}")

                # Log URL
                with open(url_log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{url}\n")

                # Check for tweets
                tweet_elements = driver.find_elements(By.CSS_SELECTOR, "div.timeline-item")
                if not tweet_elements:
                    logger.warning(f"No tweets found on page {page + 1}.")
                    break
                yield html

                # Find 'Load more'
                try:
                    load_more_button = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div.show-more:not(.timeline-item) > a"))
                    )
                    next_href = load_more_button.get_attribute("href")
                    logger.info(f"Found 'Load more' link: {next_href}")
                    url = next_href  # Use full URL directly
                    with open(resume_file, 'w') as f:
                        f.write(url)
                except Exception as e:
                    logger.info(f"No 'Load more' link found or reached end.")
                    break

                page += 1
                if max_pages and page >= max_pages:
                    logger.info(f"Reached max pages limit: {max_pages}")
                    break

                time.sleep(random.uniform(delay_min, delay_max))

            except Exception as e:
                logger.error(f"Error on page {page + 1}: {e}")
                break

    finally:
        logger.info("Closing the browser...")
        driver.quit()
        if os.path.exists(resume_file):
            os.remove(resume_file)

def scrape_twitter(keyword, since, until, max_pages=10, base_url="https://nitter.net", output_dir="extracted_html"):
    """Primary execution function for Twitter scraping."""
    # 1. Setup Output Directory
    if os.path.exists(output_dir):
        logger.info(f"Clearing output directory: {output_dir}")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # 2. Clear visited_urls.txt for a fresh run
    if os.path.exists('visited_urls.txt'):
        os.remove('visited_urls.txt')

    # 3. Construct Start URL
    start_url = construct_nitter_url(keyword, since, until, base_url)
    logger.info(f"Starting scrape for keyword: '{keyword}' from {since} to {until}")
    logger.info(f"URL: {start_url}")

    # 4. Run Generator
    parsed_count = 0
    for html in nitter_html_generator(start_url, output_dir, max_pages=max_pages):
        parsed_count += 1
    
    logger.info(f"Scraping complete. Total pages fetched: {parsed_count}")
    return parsed_count

# Example usage
if __name__ == "__main__":
    scrape_twitter(
        keyword="United States",
        since="2025-06-01",
        until="2025-07-19",
        max_pages=2
    )