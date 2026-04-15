import logging
import os
import json
import re
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def extract_tweets_from_html(html_content):
    """
    Extract tweet data from Nitter HTML content
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    tweets = []
    
    # Find all timeline items (tweets)
    timeline_items = soup.find_all('div', class_='timeline-item')
    
    for item in timeline_items:
        # Skip "Load more" buttons
        if 'show-more' in item.get('class', []):
            continue
            
        tweet_data = {}
        
        try:
            # Extract tweet link/ID
            tweet_link = item.find('a', class_='tweet-link')
            if tweet_link:
                tweet_data['tweet_url'] = tweet_link.get('href', '')
                # Extract tweet ID from URL
                tweet_id_match = re.search(r'/status/(\d+)', tweet_data['tweet_url'])
                if tweet_id_match:
                    tweet_data['tweet_id'] = tweet_id_match.group(1)
            
            # Check if it's a retweet
            retweet_header = item.find('div', class_='retweet-header')
            if retweet_header:
                retweet_text = retweet_header.get_text(strip=True)
                # Extract who retweeted
                tweet_data['retweeted_by'] = retweet_text.replace('retweeted', '').strip()
                tweet_data['is_retweet'] = True
            else:
                tweet_data['is_retweet'] = False
            
            # Extract user information
            username_elem = item.find('a', class_='username')
            if username_elem:
                tweet_data['username'] = username_elem.get_text(strip=True)
                tweet_data['user_handle'] = username_elem.get('title', '').replace('@', '')
            
            fullname_elem = item.find('a', class_='fullname')
            if fullname_elem:
                # Remove verification badge from fullname
                fullname_text = fullname_elem.get_text(strip=True)
                tweet_data['fullname'] = fullname_text
                
                # Check verification status
                verified_icon = fullname_elem.find('span', class_='verified-icon')
                if verified_icon:
                    tweet_data['is_verified'] = True
                    # Get verification type
                    classes = verified_icon.get('class', [])
                    if 'blue' in classes:
                        tweet_data['verification_type'] = 'blue'
                    elif 'government' in classes:
                        tweet_data['verification_type'] = 'government'
                    elif 'business' in classes:
                        tweet_data['verification_type'] = 'business'
                    else:
                        tweet_data['verification_type'] = 'other'
                else:
                    tweet_data['is_verified'] = False
            
            # Extract timestamp
            tweet_date = item.find('span', class_='tweet-date')
            if tweet_date:
                date_link = tweet_date.find('a')
                if date_link:
                    tweet_data['timestamp_text'] = date_link.get_text(strip=True)
                    tweet_data['timestamp_full'] = date_link.get('title', '')
            
            # Extract tweet content
            tweet_content = item.find('div', class_='tweet-content')
            if tweet_content:
                tweet_data['text_content'] = tweet_content.get_text(strip=True)
                
                # Extract hashtags
                hashtags = []
                for hashtag in tweet_content.find_all('a', href=re.compile(r'/search\?q=%23')):
                    hashtags.append(hashtag.get_text(strip=True))
                tweet_data['hashtags'] = hashtags
                
                # Extract mentions
                mentions = []
                for mention in tweet_content.find_all('a', href=re.compile(r'^/[^/]+$')):
                    if not mention.get('href', '').startswith('/search'):
                        mentions.append(mention.get_text(strip=True))
                tweet_data['mentions'] = mentions
            
            # Extract reply information
            replying_to = item.find('div', class_='replying-to')
            if replying_to:
                tweet_data['is_reply'] = True
                reply_mentions = []
                for reply_link in replying_to.find_all('a'):
                    reply_mentions.append(reply_link.get_text(strip=True))
                tweet_data['replying_to'] = reply_mentions
            else:
                tweet_data['is_reply'] = False
            
            # Extract engagement stats
            tweet_stats = item.find('div', class_='tweet-stats')
            if tweet_stats:
                stats = {}
                stat_spans = tweet_stats.find_all('span', class_='tweet-stat')
                
                for stat in stat_spans:
                    stat_text = stat.get_text(strip=True)
                    # Comments
                    if 'icon-comment' in str(stat):
                        stats['comments'] = stat_text if stat_text else '0'
                    # Retweets
                    elif 'icon-retweet' in str(stat):
                        stats['retweets'] = stat_text if stat_text else '0'
                    # Quotes
                    elif 'icon-quote' in str(stat):
                        stats['quotes'] = stat_text if stat_text else '0'
                    # Likes
                    elif 'icon-heart' in str(stat):
                        stats['likes'] = stat_text if stat_text else '0'
                    # Views/Plays
                    elif 'icon-play' in str(stat):
                        stats['plays'] = stat_text if stat_text else '0'
                
                tweet_data['engagement_stats'] = stats
            
            # Check for media attachments
            attachments = item.find('div', class_='attachments')
            if attachments:
                tweet_data['has_media'] = True
                media_types = []
                
                # Check for images
                if attachments.find('div', class_='attachment image'):
                    media_types.append('image')
                    image_count = len(attachments.find_all('div', class_='attachment image'))
                    tweet_data['image_count'] = image_count
                
                # Check for videos
                if attachments.find('div', class_='video-container'):
                    media_types.append('video')
                
                tweet_data['media_types'] = media_types
            else:
                tweet_data['has_media'] = False
            
            # Check for quoted tweets
            quote = item.find('div', class_='quote')
            if quote:
                tweet_data['has_quote'] = True
                quote_data = {}
                
                # Extract quoted user
                quote_fullname = quote.find('a', class_='fullname')
                if quote_fullname:
                    quote_data['quoted_user'] = quote_fullname.get_text(strip=True)
                
                quote_username = quote.find('a', class_='username')
                if quote_username:
                    quote_data['quoted_username'] = quote_username.get_text(strip=True)
                
                # Extract quoted text
                quote_text = quote.find('div', class_='quote-text')
                if quote_text:
                    quote_data['quoted_text'] = quote_text.get_text(strip=True)
                
                tweet_data['quoted_tweet'] = quote_data
            else:
                tweet_data['has_quote'] = False
            
            # Add to tweets list if we have basic data
            if 'username' in tweet_data and 'text_content' in tweet_data:
                tweets.append(tweet_data)
                
        except Exception as e:
            logger.error(f"Error processing tweet: {e}")
            continue
    
    return tweets

def save_tweets_to_json(tweets, filename='extracted_tweets.json'):
    """
    Save extracted tweets to a JSON file
    """
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(tweets, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(tweets)} tweets to {filename}")

def print_tweet_summary(tweets):
    """
    Print a summary of extracted tweets
    """
    logger.info(f"=== Tweet Extraction Summary ===")
    logger.info(f"Total tweets extracted: {len(tweets)}")
    
    if tweets:
        for i, tweet in enumerate(tweets[:3], 1):
            logger.info(f"Tweet {i} - User: {tweet.get('username', 'N/A')} - Text: {tweet.get('text_content', 'N/A')[:50]}...")

def process_html_file(filepath):
    """
    Process an HTML file and extract tweets
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        tweets = extract_tweets_from_html(html_content)
        print_tweet_summary(tweets)
        
        return tweets
        
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return []
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return []

def parse_twitter_html(input_dir="extracted_html", output_json="extracted_tweets.json"):
    """
    Process all HTML files in a directory, extract unique tweets, and save to JSON.
    """
    if not os.path.exists(input_dir):
        logger.error(f"Directory not found: {input_dir}")
        return 0

    all_html_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".html")]
    
    if not all_html_files:
        logger.warning(f"No HTML files found in {input_dir}")
        return 0

    all_tweets = []
    for filepath in all_html_files:
        logger.info(f"Processing {filepath}...")
        tweets = process_html_file(filepath)
        all_tweets.extend(tweets)
    
    # Remove duplicates based on tweet_id
    unique_tweets = {}
    for tweet in all_tweets:
        if 'tweet_id' in tweet:
            unique_tweets[tweet['tweet_id']] = tweet
    
    final_tweets = list(unique_tweets.values())
    
    if final_tweets:
        save_tweets_to_json(final_tweets, output_json)
        logger.info(f"Successfully saved {len(final_tweets)} unique tweets to {output_json}")
    
    return len(final_tweets)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parse_twitter_html()