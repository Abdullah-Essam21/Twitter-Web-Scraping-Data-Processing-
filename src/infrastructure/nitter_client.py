import httpx
import random
import asyncio
import logging
from urllib.parse import urlencode, quote_plus

logger = logging.getLogger(__name__)

class NitterClient:
    def __init__(self, base_url="https://nitter.net"):
        self.base_url = base_url.rstrip('/')
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    def construct_search_url(self, keyword, config):
        base_search = f"{self.base_url}/search"
        params = {"f": "tweets", "q": keyword}
        
        standard_filters = ["since", "until", "min_faves", "min_retweets", "min_replies"]
        for key in standard_filters:
            val = config.get(key)
            if val and str(val) != "0":
                params[key] = val

        toggle_keys = ["nativeretweets", "media", "videos", "news", "native_video", "replies", "links", "images", "quote", "spaces"]
        for key in toggle_keys:
            if config.get(f"include_{key}"): params[f"f-{key}"] = "on"
            if config.get(f"exclude_{key}"): params[f"e-{key}"] = "on"

        return f"{base_search}?{urlencode(params, quote_via=quote_plus)}"

    def construct_profile_url(self, handle):
        return f"{self.base_url}/{handle.lstrip('@')}"

    async def fetch_page(self, url, retries=3):
        async with httpx.AsyncClient(headers=self.headers, http2=True, follow_redirects=True) as client:
            for attempt in range(retries):
                try:
                    response = await client.get(url, timeout=30.0)
                    if response.status_code == 404:
                        return "404"
                    response.raise_for_status()
                    
                    if len(response.text) > 1000:
                        return response.text
                    
                    logger.warning(f"Response too short for {url}, retrying...")
                    await asyncio.sleep(random.uniform(2, 5))
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e}")
                    await asyncio.sleep(random.uniform(2, 5))
            return None
