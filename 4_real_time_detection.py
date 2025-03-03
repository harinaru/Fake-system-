import tweepy
import feedparser

logging.info("Initializing real-time detection module...")

def fetch_twitter_trends(api_key, api_secret, bearer_token):
    client = tweepy.Client(bearer_token)
    query = "#fakeNews -is:retweet lang:en"
    tweets = client.search_recent_tweets(query=query, max_results=10)
    return [tweet.text for tweet in tweets.data] if tweets.data else []

def fetch_news_rss():
    rss_feeds = [
        "http://feeds.bbci.co.uk/news/rss.xml",
        "https://www.thehindu.com/news/feeder/default.rss"
    ]
    news_list = []
    for feed in rss_feeds:
        parsed_feed = feedparser.parse(feed)
        for entry in parsed_feed.entries[:5]:
            news_list.append(entry.title)
    return news_list

logging.info("Real-time detection module loaded.")