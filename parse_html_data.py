import logging
import os
import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Setup logging
logger = logging.getLogger(__name__)

def extract_tweets_from_html(html_content, human_keyword, base_url="https://nitter.net"):
    """
    Extracts high-fidelity tweet data using a robust, unified schema.
    Combines profile URL generation, verification types, and retweet tracking.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    tweets = []
    
    timeline_items = soup.find_all('div', class_='timeline-item')
    
    for item in timeline_items:
        # Skip 'Load more' buttons
        if 'show-more' in item.get('class', []):
            continue
            
        # --- UNIFIED SCHEMA ---
        handle = item.get('data-username', '')
        tweet_data = {
            'search_keyword': human_keyword,
            'tweet_id': None,
            'tweet_url': None,
            'user_handle': handle,
            'user_profile_url': urljoin(base_url, handle) if handle else None,
            'fullname': None,
            'is_verified': False,
            'verification_type': None, # Standard, Blue, Gold, etc.
            'timestamp_full': None,
            'date_text': None,
            'text_content': "",
            'hashtags': [],
            'mentions': [],
            'is_reply': False,
            'replying_to': [], 
            'is_retweet': False,
            'retweeted_by': None,
            'engagement_stats': {
                'replies': 0, 'retweets': 0, 'likes': 0, 'views': 0
            },
            'has_media': False,
            'media': [],
            'has_quote': False,
            'quoted_tweet': {
                'user_handle': None, 'text': None, 'url': None
            }
        }
        
        try:
            # 1. Primary Identifiers
            link_elem = item.find('a', class_='tweet-link')
            if link_elem:
                rel_path = link_elem.get('href', '')
                tweet_data['tweet_url'] = urljoin(base_url, rel_path)
                tid_match = re.search(r'/status/(\d+)', rel_path)
                if tid_match:
                    tweet_data['tweet_id'] = tid_match.group(1)

            # 2. User Info & Verification
            fn_elem = item.find('a', class_='fullname')
            if fn_elem:
                tweet_data['fullname'] = fn_elem.get_text(strip=True)
                
            v_icon = item.find('div', class_='verified-icon')
            if v_icon:
                tweet_data['is_verified'] = True
                v_classes = v_icon.get('class', [])
                # Capture specific verification (Gold, Blue, etc)
                tweet_data['verification_type'] = v_classes[-1] if len(v_classes) > 1 else 'standard'

            # 3. Date & Time (Fallback logic for Search vs Status pages)
            date_span = item.find('span', class_='tweet-date')
            if date_span and date_span.find('a'):
                tweet_data['timestamp_full'] = date_span.find('a').get('title', '')
                tweet_data['date_text'] = date_span.find('a').get_text(strip=True)
            else:
                pub_p = item.find('p', class_='tweet-published')
                if pub_p:
                    tweet_data['timestamp_full'] = pub_p.get_text(strip=True)
                    tweet_data['date_text'] = tweet_data['timestamp_full'].split(' · ')[0]

            # 4. Content, Hashtags, and Mentions
            content_div = item.find('div', class_='tweet-content')
            if content_div:
                tweet_data['text_content'] = content_div.get_text(strip=True)
                tweet_data['hashtags'] = [a.get_text() for a in content_div.find_all('a', href=re.compile(r'/search\?q=%23'))]
                # Filter for user profile links specifically
                tweet_data['mentions'] = [a.get_text() for a in content_div.find_all('a') 
                                         if a.get('href', '').startswith('/') and not a.get('href', '').startswith('/search')]

            # 5. Engagement Stats
            stats_div = item.find('div', class_='tweet-stats')
            if stats_div:
                for stat in stats_div.find_all('span', class_='tweet-stat'):
                    val_text = stat.get_text(strip=True).replace(',', '').replace('.', '')
                    val = int(val_text) if val_text.isdigit() else 0
                    icon = stat.find('span')
                    if icon:
                        cl = " ".join(icon.get('class', []))
                        if 'icon-comment' in cl: tweet_data['engagement_stats']['replies'] = val
                        elif 'icon-retweet' in cl: tweet_data['engagement_stats']['retweets'] = val
                        elif 'icon-heart' in cl: tweet_data['engagement_stats']['likes'] = val
                        elif 'icon-views' in cl: tweet_data['engagement_stats']['views'] = val

            # 6. Media (Images and Videos)
            attachments = item.find('div', class_='attachments')
            if attachments:
                tweet_data['has_media'] = True
                for img in attachments.find_all('img'):
                    tweet_data['media'].append({'type': 'image', 'url': urljoin(base_url, img.get('src', ''))})
                for video in attachments.find_all('video'):
                    src = video.find('source').get('src') if video.find('source') else video.get('src')
                    if src: tweet_data['media'].append({'type': 'video', 'url': urljoin(base_url, src)})

            # 7. Retweets and Replies
            rt_header = item.find('div', class_='retweet-header')
            if rt_header:
                tweet_data['is_retweet'] = True
                tweet_data['retweeted_by'] = rt_header.get_text(strip=True)

            rep_div = item.find('div', class_='replying-to')
            if rep_div:
                tweet_data['is_reply'] = True
                tweet_data['replying_to'] = [
                    {'handle': a.get_text(strip=True), 'url': urljoin(base_url, a.get('href', ''))} 
                    for a in rep_div.find_all('a')
                ]

            # 8. Quotes
            quote_div = item.find('div', class_='quote')
            if quote_div:
                tweet_data['has_quote'] = True
                q_user = quote_div.find('a', class_='username')
                q_link = quote_div.find('a', class_='quote-link')
                tweet_data['quoted_tweet'] = {
                    'user_handle': q_user.get_text(strip=True) if q_user else None,
                    'text': quote_div.find('div', class_='quote-text').get_text(strip=True) if quote_div.find('div', class_='quote-text') else None,
                    'url': urljoin(base_url, q_link.get('href', '')) if q_link else None
                }

            if tweet_data['tweet_id']:
                tweets.append(tweet_data)
                
        except Exception as e:
            logger.error(f"Error parsing tweet: {e}")
            continue
            
    return tweets

def parse_twitter_html(root_dir="data", output_jsonl="extracted_tweets.jsonl"):
    """
    Recursively processes all session folders in the root directory.
    Uses config.json in each folder to identify the human keyword.
    Writes output in JSONL format for memory efficiency.
    """
    if not os.path.exists(root_dir):
        logger.warning(f"Root directory not found: {root_dir}")
        return 0

    seen_ids = set()
    count = 0

    with open(output_jsonl, "w", encoding="utf-8") as f_out:
        # Walk the directory tree
        for root, dirs, files in os.walk(root_dir):
            # Look for sessions (folders with config.json)
            if "config.json" in files:
                config_path = os.path.join(root, "config.json")
                try:
                    with open(config_path, "r", encoding="utf-8") as f:
                        config = json.load(f)
                        human_keyword = config.get("keyword", "Unknown")
                        base_url = config.get("base_url", "https://nitter.net")
                except Exception as e:
                    logger.error(f"Error reading config in {root}: {e}")
                    human_keyword = "Unknown"
                    base_url = "https://nitter.net"

                # Process all HTML files in this specific session folder
                html_files = [f for f in files if f.endswith(".html")]
                logger.info(f"Processing session [{root}] for keyword '{human_keyword}' - {len(html_files)} files")
                
                for html_file in html_files:
                    path = os.path.join(root, html_file)
                    try:
                        with open(path, "r", encoding="utf-8") as f_in:
                            extracted = extract_tweets_from_html(f_in.read(), human_keyword, base_url)
                            for tweet in extracted:
                                if tweet['tweet_id'] not in seen_ids:
                                    f_out.write(json.dumps(tweet, ensure_ascii=False) + "\n")
                                    seen_ids.add(tweet['tweet_id'])
                                    count += 1
                    except Exception as e:
                        logger.error(f"Error reading {path}: {e}")

    logger.info(f"Successfully saved {count} unique tweets to {output_jsonl}")
    return count

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    parse_twitter_html()