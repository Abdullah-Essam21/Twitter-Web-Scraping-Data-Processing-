import asyncio
import os
import pandas as pd
import logging
from src.infrastructure.nitter_client import NitterClient
from src.infrastructure.storage import StorageManager
from src.core.scrapers.scrapers import SearchScraper, ProfileScraper
from src.core.parsing.parser import TweetParser

logger = logging.getLogger(__name__)

class PipelineService:
    def __init__(self, base_url="https://nitter.net"):
        self.storage = StorageManager()
        self.client = NitterClient(base_url)
        self.search_scraper = SearchScraper(self.client, self.storage)
        self.profile_scraper = ProfileScraper(self.client, self.storage)

    async def run_scrape_jobs(self, configs):
        tasks = []
        for cfg in configs:
            if cfg.get("job_type") == "Profile":
                tasks.append(self.profile_scraper.scrape(cfg))
            else:
                tasks.append(self.search_scraper.scrape(cfg))
        
        return await asyncio.gather(*tasks)

    def process_all_raw(self):
        """Processes all HTML in raw and produces CSV."""
        all_tweets = []
        raw_root = self.storage.raw_dir
        
        for subdir in os.listdir(raw_root):
            dir_path = os.path.join(raw_root, subdir)
            if not os.path.isdir(dir_path): continue
            
            # Read config if exists
            config = {}
            config_path = os.path.join(dir_path, "config.json")
            if os.path.exists(config_path):
                import json
                with open(config_path, "r") as f: config = json.load(f)

            for filename in os.listdir(dir_path):
                if filename.endswith(".html"):
                    path = os.path.join(dir_path, filename)
                    with open(path, "r", encoding="utf-8") as f:
                        html = f.read()
                    
                    tweets = TweetParser.extract_tweets(html)
                    # Inject metadata from config
                    for t in tweets:
                        t['source_job'] = config.get('keyword', 'unknown')
                        t['job_type'] = config.get('job_type', 'Search')

                    all_tweets.extend(tweets)

        if not all_tweets:
            logger.warning("No tweets found during processing.")
            return None

        # Deduplicate
        unique_tweets = {t.get('tweet_id', id(t)): t for t in all_tweets}
        final_list = list(unique_tweets.values())
        
        # Save JSON
        json_path = self.storage.save_json(final_list, f"tweets_{format_timestamp()}.json")
        
        # Save CSV
        df = pd.json_normalize(final_list)
        csv_filename = f"tweets_{format_timestamp()}.csv"
        csv_path = os.path.join(self.storage.processed_dir, csv_filename)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        
        logger.info(f"Deduplicated {len(final_list)} tweets and saved to {csv_path}")
        return csv_path

def format_timestamp():
    import datetime
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
