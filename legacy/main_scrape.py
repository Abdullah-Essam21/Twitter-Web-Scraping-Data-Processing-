import asyncio
import httpx
import os
import random
import logging
import json
import re
import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sanitize_folder_name(name):
    """Removes special characters to make a valid folder name."""
    return re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')

class NitterScraper:
    def __init__(self, base_url="https://nitter.net"):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://nitter.net/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }

    def _construct_url(self, keyword, config):
        """
        Dynamically builds the Nitter URL based on provided filters.
        Ensures zero-values and empty strings are omitted to avoid bot detection.
        """
        base_search = f"{self.base_url}/search"
        
        # 1. Start with absolute essentials
        params = {
            "f": "tweets",
            "q": keyword
        }
        
        # 2. Add specific filters only if they have non-zero/non-empty values
        # This prevents Nitter from triggering bot-checks on empty/zero params
        standard_filters = ["since", "until", "min_faves", "min_retweets", "min_replies"]
        for key in standard_filters:
            val = config.get(key)
            if val and str(val) != "0":
                params[key] = val

        # 3. Add Toggle Filters (Include 'f-' and Exclude 'e-')
        toggle_keys = [
            "nativeretweets", "media", "videos", "news", 
            "native_video", "replies", "links", "images", 
            "quote", "spaces"
        ]

        # Map Include (f-) and Exclude (e-)
        for key in toggle_keys:
            # Check include_ prefix
            if config.get(f"include_{key}"):
                params[f"f-{key}"] = "on"
            # Check exclude_ prefix
            if config.get(f"exclude_{key}"):
                params[f"e-{key}"] = "on"

        final_url = f"{base_search}?{urlencode(params, quote_via=quote_plus)}"
        return final_url

    async def fetch_page(self, client, url):
        """Handles retries and network logic for a single URL."""
        for attempt in range(3):
            try:
                response = await client.get(url, timeout=30.0)
                response.raise_for_status()
                
                content_len = len(response.text)
                if content_len > 1000:
                    return response.text
                
                logger.warning(f"Response too short ({content_len} chars) for {url}: {response.text[:200]}...")
                await asyncio.sleep(random.uniform(5, 10)) # Wait longer on suspiciously short responses
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                await asyncio.sleep(5)
        return None

    async def scrape_keyword(self, config):
        """
        The core logic for a single keyword 'job'.
        """
        keyword = config.get('keyword')
        if not keyword:
            logger.error("No keyword provided in config. Skipping job.")
            return "Failed: No Keyword"

        max_pages = config.get('max_pages', 3)
        
        # 1. Create a Unique Session Identity
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        folder_slug = sanitize_folder_name(keyword)
        output_dir = os.path.join("data", f"{folder_slug}_{timestamp}")
        os.makedirs(output_dir, exist_ok=True)

        # 2. Save Metadata (config.json) for the parser
        # We store the original human-readable keyword here
        metadata_path = os.path.join(output_dir, "config.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)

        # 3. Build the initial filtered URL
        current_url = self._construct_url(keyword, config)
        page_count = 0

        # Clear visited_urls.txt for fresh start if needed (or just append)
        # For HTTPX multi-threaded it might be better to have an async logger or separate logs
        url_log_file = "visited_urls.txt"

        async with httpx.AsyncClient(headers=self.headers, http2=True, follow_redirects=True) as client:
            while current_url and page_count < max_pages:
                logger.info(f"[{keyword}] Fetching Page {page_count + 1}: {current_url}")
                
                # Log to visited_urls.txt
                with open(url_log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{current_url}\n")

                html = await self.fetch_page(client, current_url)
                
                if not html:
                    logger.warning(f"[{keyword}] Stopped: No valid HTML returned.")
                    break
                
                # Save the raw HTML
                page_count += 1
                file_path = os.path.join(output_dir, f"page_{page_count}.html")
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(html)

                # 4. Parse for Next Page (Cursor)
                soup = BeautifulSoup(html, 'html.parser')
                
                if not soup.select("div.timeline-item"):
                    logger.info(f"[{keyword}] No more tweets found matching these filters.")
                    break

                load_more = soup.select_one("div.show-more:not(.timeline-item) a")
                
                if load_more and load_more.has_attr('href'):
                    path = load_more['href']
                    if path.startswith('?'):
                        path = f"/search{path}"
                    
                    current_url = f"{self.base_url}/{path.lstrip('/')}"
                    await asyncio.sleep(random.uniform(2, 5))
                else:
                    logger.info(f"[{keyword}] Reached the end of search results.")
                    current_url = None
                    
        return f"Completed [{keyword}]: {page_count} pages saved to {output_dir}"

from profile_scrape import NitterProfileScraper

async def run_parallel_scrape(scrape_configs):
    """
    Orchestrates multiple scraping jobs (Search or Profile).
    """
    search_scraper = NitterScraper()
    profile_scraper = NitterProfileScraper()
    
    tasks = []
    for cfg in scrape_configs:
        job_type = cfg.get("job_type", "Search")
        if job_type == "Profile":
            tasks.append(profile_scraper.scrape_profile(cfg))
        else:
            tasks.append(search_scraper.scrape_keyword(cfg))
            
    # Run concurrently
    results = await asyncio.gather(*tasks)
    for res in results:
        logger.info(res)
    return results

if __name__ == "__main__":
    # Example usage
    search_jobs = [
        {"keyword": "Bitcoin", "max_pages": 1},
        {"keyword": "Donald Trump","min_faves": 100, "max_pages": 3},
    ]
    asyncio.run(run_parallel_scrape(search_jobs))