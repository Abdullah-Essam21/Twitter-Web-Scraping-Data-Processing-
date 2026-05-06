import asyncio
import random
import logging
from src.core.parsing.parser import TweetParser, ProfileParser

logger = logging.getLogger(__name__)

class BaseScraper:
    def __init__(self, client, storage):
        self.client = client
        self.storage = storage

    async def _scrape_loop(self, start_url, session_dir, max_pages, keyword_label):
        current_url = start_url
        page_count = 0
        
        while current_url and page_count < max_pages:
            logger.info(f"[{keyword_label}] Fetching Page {page_count + 1}...")
            html = await self.client.fetch_page(current_url)
            
            if not html or html == "404":
                logger.warning(f"[{keyword_label}] Stopped: Invalid response.")
                break
            
            # Save HTML snapshot
            page_count += 1
            self.storage.save_html(session_dir, page_count, html)
            
            # Check for next page
            path = TweetParser.get_next_page_url(html)
            if path:
                # Handle search vs profile URL construction
                if path.startswith('/'):
                    current_url = f"{self.client.base_url}{path}"
                else:
                    # Logic for relative paths if any
                    current_url = f"{self.client.base_url}/{keyword_label.lstrip('@')}{path}"
                
                await asyncio.sleep(random.uniform(3, 6))
            else:
                current_url = None
                
        return page_count

class SearchScraper(BaseScraper):
    async def scrape(self, config):
        keyword = config.get('keyword')
        max_pages = config.get('max_pages', 3)
        session_dir = self.storage.create_session_dir(keyword)
        
        # Save config for reproducibility
        self.storage.save_json(config, "config.json", session_dir)
        
        start_url = self.client.construct_search_url(keyword, config)
        count = await self._scrape_loop(start_url, session_dir, max_pages, keyword)
        return f"Search [{keyword}] completed: {count} pages."

class ProfileScraper(BaseScraper):
    async def scrape(self, config):
        handle = config.get('keyword', '').strip().lstrip('@')
        max_pages = config.get('max_pages', 3)
        session_dir = self.storage.create_session_dir(handle, prefix="profile_")
        
        self.storage.save_json(config, "config.json", session_dir)
        
        start_url = self.client.construct_profile_url(handle)
        
        # We fetch first page specifically to get metadata
        html = await self.client.fetch_page(start_url)
        if html and html != "404":
            meta = ProfileParser.extract_metadata(html, handle)
            self.storage.save_json(meta, f"{handle}_profile.json", session_dir)
            logger.info(f"[{handle}] Profile metadata saved.")
            
            # Continue normally for timeline
            count = await self._scrape_loop(start_url, session_dir, max_pages, f"@{handle}")
            return f"Profile [@{handle}] completed: {count} pages."
        return f"Profile [@{handle}] failed."
