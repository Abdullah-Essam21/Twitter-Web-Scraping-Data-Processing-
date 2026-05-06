from bs4 import BeautifulSoup
import re
import datetime
import logging

logger = logging.getLogger(__name__)

class TweetParser:
    @staticmethod
    def extract_tweets(html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        tweets = []
        timeline_items = soup.find_all('div', class_='timeline-item')
        
        for item in timeline_items:
            if 'show-more' in item.get('class', []): continue
            
            try:
                tweet_data = {}
                tweet_link = item.find('a', class_='tweet-link')
                if tweet_link:
                    tweet_data['tweet_url'] = tweet_link.get('href', '')
                    tweet_id_match = re.search(r'/status/(\d+)', tweet_data['tweet_url'])
                    if tweet_id_match: tweet_data['tweet_id'] = tweet_id_match.group(1)
                
                # Check if it's a retweet
                retweet_header = item.find('div', class_='retweet-header')
                tweet_data['is_retweet'] = bool(retweet_header)
                if retweet_header:
                    tweet_data['retweeted_by'] = retweet_header.get_text(strip=True).replace('retweeted', '').strip()
                
                username_elem = item.find('a', class_='username')
                if username_elem:
                    tweet_data['username'] = username_elem.get_text(strip=True)
                    tweet_data['user_handle'] = username_elem.get('title', '').replace('@', '')
                
                fullname_elem = item.find('a', class_='fullname')
                if fullname_elem:
                    tweet_data['fullname'] = fullname_elem.get_text(strip=True)

                tweet_content = item.find('div', class_='tweet-content')
                if tweet_content:
                    tweet_data['text_content'] = tweet_content.get_text(strip=True)
                
                # Engagement
                stats = {}
                tweet_stats = item.find('div', class_='tweet-stats')
                if tweet_stats:
                    for stat in tweet_stats.find_all('span', class_='tweet-stat'):
                        text = stat.get_text(strip=True) or '0'
                        if 'icon-comment' in str(stat): stats['comments'] = text
                        elif 'icon-retweet' in str(stat): stats['retweets'] = text
                        elif 'icon-heart' in str(stat): stats['likes'] = text

                tweet_data['engagement_stats'] = stats
                
                if 'username' in tweet_data and 'text_content' in tweet_data:
                    tweets.append(tweet_data)

            except Exception as e:
                logger.error(f"Error parsing tweet: {e}")
                continue
                
        return tweets

    @staticmethod
    def get_next_page_url(html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        load_more = soup.select_one("div.show-more:not(.timeline-item) a")
        if load_more and load_more.has_attr('href'):
            return load_more['href']
        return None

class ProfileParser:
    @staticmethod
    def extract_metadata(html_content, handle):
        soup = BeautifulSoup(html_content, 'html.parser')
        return {
            "handle": f"@{handle.lstrip('@')}",
            "scraping_timestamp": datetime.datetime.now().isoformat(),
            "full_name": getattr(soup.select_one(".profile-card-fullname"), 'text', "").strip(),
            "bio": getattr(soup.select_one(".profile-bio"), 'text', "").strip(),
            "joined_date": getattr(soup.select_one(".profile-joindate"), 'text', "").strip(),
            "tweets_count": getattr(soup.select_one(".posts .profile-stat-num"), 'text', "0").strip(),
            "followers_count": getattr(soup.select_one(".followers .profile-stat-num"), 'text', "0").strip(),
        }
