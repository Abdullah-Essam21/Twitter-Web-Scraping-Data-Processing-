import asyncio
import httpx
import os
import random
import logging
import json
import datetime
import re
from bs4 import BeautifulSoup

# Setup logging (shares the global logging config)
logger = logging.getLogger(__name__)

def sanitize_folder_name(name):
    """Removes special characters to make a valid folder name."""
    return re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_')

class NitterProfileScraper:
    def __init__(self, base_url="https://nitter.net"):
        self.base_url = base_url.rstrip('/')
        # Higher-fidelity headers for profile scraping
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "TE": "trailers"
        }

    async def fetch_page(self, client, url):
        """Fetches page with specific retry logic for 0-byte or error responses."""
        for attempt in range(3):
            try:
                response = await client.get(url, timeout=25.0)
                if response.status_code == 404:
                    return "404"
                
                response.raise_for_status()
                html = response.text
                
                # Nitter occasionally returns empty pages on high load
                if not html or len(html) < 2000:
                    logger.warning(f"Attempt {attempt+1}: Empty or tiny response. Retrying...")
                    await asyncio.sleep(random.uniform(5, 10))
                    continue
                
                return html
            except Exception as e:
                logger.error(f"Attempt {attempt+1} failed for {url}: {e}")
                await asyncio.sleep(random.uniform(5, 10))
        return None

    def extract_metadata(self, soup, handle):
        """Extracts stats based on the Nitter profile structure."""
        return {
            "handle": f"@{handle}",
            "scraping_timestamp": datetime.datetime.now().isoformat(),
            "full_name": getattr(soup.select_one(".profile-card-fullname"), 'text', "").strip(),
            "bio": getattr(soup.select_one(".profile-bio"), 'text', "").strip(),
            "joined_date": getattr(soup.select_one(".profile-joindate"), 'text', "").strip(),
            "tweets_count": getattr(soup.select_one(".posts .profile-stat-num"), 'text', "0").strip(),
            "following_count": getattr(soup.select_one(".following .profile-stat-num"), 'text', "0").strip(),
            "followers_count": getattr(soup.select_one(".followers .profile-stat-num"), 'text', "0").strip(),
            "likes_count": getattr(soup.select_one(".likes .profile-stat-num"), 'text', "0").strip(),
        }

    async def scrape_profile(self, config):
        """
        Refactored to match the orchestration pattern in the pipeline.
        """
        handle = config.get('keyword', '').strip().lstrip('@')
        if not handle:
            logger.error("No user handle provided. Skipping profile job.")
            return "Failed: No Handle"

        max_pages = config.get('max_pages', 3)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Setup Session Folder
        folder_slug = sanitize_folder_name(handle)
        output_dir = os.path.join("data", f"profile_{folder_slug}_{timestamp}")
        os.makedirs(output_dir, exist_ok=True)

        # 2. Save config.json for the Parser
        # We store the handle as 'keyword' so the parser injects it into the CSV
        config_path = os.path.join(output_dir, "config.json")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=2)

        current_url = f"{self.base_url}/{handle}"
        page_count = 0
        metadata_saved = False

        # 3. Execution Loop
        async with httpx.AsyncClient(headers=self.headers, http2=True, follow_redirects=True) as client:
            while current_url and page_count < max_pages:
                logger.debug(f"[{handle}] Requesting: {current_url}")
                html = await self.fetch_page(client, current_url)

                if html == "404" or (html and 'class="error-panel"' in html):
                    logger.error(f"User '{handle}' not found or instace error.")
                    return f"Failed: User {handle} not found"
                
                if not html: break

                soup = BeautifulSoup(html, 'html.parser')

                # Extract and save metadata on the first valid page
                if not metadata_saved:
                    meta = self.extract_metadata(soup, handle)
                    meta_filename = f"{handle}_profile_{timestamp}.json"
                    with open(os.path.join(output_dir, meta_filename), "w", encoding="utf-8") as f:
                        json.dump(meta, f, indent=4)
                    metadata_saved = True
                    logger.info(f"[{handle}] Profile metadata saved to {meta_filename}")

                page_count += 1
                page_file = f"page_{page_count}.html"
                with open(os.path.join(output_dir, page_file), "w", encoding="utf-8") as f:
                    f.write(html)
                logger.info(f"[{handle}] Saved {page_file}")

                # Find 'Load more' for the timeline
                load_more = soup.select_one("div.show-more:not(.timeline-item) a")
                if load_more and load_more.has_attr('href'):
                    # The href in profiles is relative to the user root, e.g. /handle?cursor=...
                    path = load_more['href']
                    if path.startswith('/'):
                        current_url = f"{self.base_url}{path}"
                    else:
                        current_url = f"{self.base_url}/{handle}{path}"
                    
                    await asyncio.sleep(random.uniform(4, 7))
                else:
                    current_url = None

        return f"Completed Profile [{handle}]: {page_count} pages saved."
