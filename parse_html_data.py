import logging
import os
import json
import re
from bs4 import BeautifulSoup

# Setup logging
logger = logging.getLogger(__name__)

def extract_tweets_from_html(html_content, human_keyword):
    """
    Extract tweet data from Nitter HTML content and inject the search keyword.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    tweets = []
    
    # Find all timeline items
    timeline_items = soup.find_all('div', class_='timeline-item')
    
    for item in timeline_items:
        if 'show-more' in item.get('class', []):
            continue
            
        tweet_data = {"search_keyword": human_keyword}
        
        try:
            # Basic info extraction
            tweet_link = item.find('a', class_='tweet-link')
            if tweet_link:
                href = tweet_link.get('href', '')
                tweet_data['tweet_url'] = href
                tweet_id_match = re.search(r'/status/(\d+)', href)
                if tweet_id_match:
                    tweet_data['tweet_id'] = tweet_id_match.group(1)
            
            # User info
            username_elem = item.find('a', class_='username')
            if username_elem:
                tweet_data['username'] = username_elem.get_text(strip=True)
                tweet_data['user_handle'] = username_elem.get('title', '').replace('@', '')
            
            fullname_elem = item.find('a', class_='fullname')
            if fullname_elem:
                tweet_data['fullname'] = fullname_elem.get_text(strip=True)

            # Content
            tweet_content = item.find('div', class_='tweet-content')
            if tweet_content:
                tweet_data['text_content'] = tweet_content.get_text(strip=True)
            
            # Engagement
            tweet_stats = item.find('div', class_='tweet-stats')
            if tweet_stats:
                stats = {}
                stat_spans = tweet_stats.find_all('span', class_='tweet-stat')
                for stat in stat_spans:
                    stat_text = stat.get_text(strip=True)
                    if 'icon-comment' in str(stat): stats['comments'] = stat_text
                    elif 'icon-retweet' in str(stat): stats['retweets'] = stat_text
                    elif 'icon-heart' in str(stat): stats['likes'] = stat_text
                tweet_data['engagement_stats'] = stats

            if 'username' in tweet_data and 'text_content' in tweet_data:
                tweets.append(tweet_data)
                
        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            continue
    
    return tweets

def parse_twitter_html(root_dir="data", output_json="extracted_tweets.json"):
    """
    Recursively processes all session folders in the root directory.
    Uses config.json in each folder to identify the human keyword.
    """
    if not os.path.exists(root_dir):
        logger.warning(f"Root directory not found: {root_dir}")
        return 0

    all_tweets = []
    
    # Walk the directory tree
    for root, dirs, files in os.walk(root_dir):
        # Look for sessions (folders with config.json)
        if "config.json" in files:
            config_path = os.path.join(root, "config.json")
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = json.load(f)
                    human_keyword = config.get("keyword", "Unknown")
            except Exception as e:
                logger.error(f"Error reading config in {root}: {e}")
                human_keyword = "Unknown"

            # Process all HTML files in this specific session folder
            html_files = [f for f in files if f.endswith(".html")]
            logger.info(f"Processing session [{root}] for keyword '{human_keyword}' - {len(html_files)} files")
            
            for html_file in html_files:
                file_path = os.path.join(root, html_file)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        html_content = f.read()
                    tweets = extract_tweets_from_html(html_content, human_keyword)
                    all_tweets.extend(tweets)
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")

    # Remove duplicates based on tweet_id (globally across sessions)
    unique_tweets = {}
    for tweet in all_tweets:
        if 'tweet_id' in tweet:
            unique_tweets[tweet['tweet_id']] = tweet
    
    final_tweets = list(unique_tweets.values())
    
    if final_tweets:
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(final_tweets, f, ensure_ascii=False, indent=2)
        logger.info(f"Successfully saved {len(final_tweets)} unique tweets to {output_json}")
    
    return len(final_tweets)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parse_twitter_html()