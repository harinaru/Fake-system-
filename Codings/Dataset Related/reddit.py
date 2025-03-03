import os
import logging
import pandas as pd
import praw
from datetime import datetime

# Setup logging (append to existing log file)
logging.basicConfig(
    filename="reddit_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)

logging.info("===== Reddit News Fetching Script Started =====")

# Initialize Reddit API
reddit = praw.Reddit(
    client_id="UIyqeoR-MDdIi12MEZovYA",
    client_secret="vjiYdARq6-Ot29eWkqRyDtyahC0xTw",
    user_agent="test_bot",
    username="Harinainyar",
    password="Harina6789h@",
)

# Define target subreddit
SUBREDDIT = "politics"
LIMIT = 500  # Number of posts to fetch
FILE_PATH = "reddit_news.csv"

try:
    # Fetch top posts from subreddit
    subreddit = reddit.subreddit(SUBREDDIT)
    posts = subreddit.top(limit=LIMIT)

    # Process posts into DataFrame
    data = []
    for post in posts:
        data.append({
            "title": post.title,
            "source": "Reddit",  # Uniformity with NewsAPI
            "timestamp": datetime.utcfromtimestamp(post.created_utc),
            "date": datetime.utcfromtimestamp(post.created_utc).date(),
            "url": post.url,
            "score": post.score  # Reddit ranking metric
        })

    df_reddit = pd.DataFrame(data)

    if df_reddit.empty:
        logging.warning("No data fetched from Reddit.")
        print("No new Reddit posts found.")
    else:
        # Ensure safe CSV writing
        if os.path.exists(FILE_PATH):
            try:
                logging.info("Reading existing reddit_news.csv")

                # Load existing data safely
                df_existing = pd.read_csv(FILE_PATH, encoding="utf-8", on_bad_lines="skip")

                # Ensure same structure before appending
                if set(df_reddit.columns) == set(df_existing.columns):
                    df_new = df_reddit[~df_reddit["title"].isin(df_existing["title"])]
                    if not df_new.empty:
                        df_new.to_csv(FILE_PATH, mode="a", index=False, header=False, encoding="utf-8")
                        logging.info(f"{len(df_new)} new Reddit articles appended to reddit_news.csv")
                        print(f"{len(df_new)} new Reddit articles saved.")
                    else:
                        logging.info("No new Reddit articles. All titles exist.")
                        print("No new articles to add.")
                else:
                    logging.error("CSV structure mismatch. Check manually.")
                    print("CSV structure mismatch. Check logs.")
            except Exception as e:
                logging.error(f"Error reading reddit_news.csv: {e}")
                print("Error reading reddit_news.csv. Check logs.")
        else:
            # First-time save
            df_reddit.to_csv(FILE_PATH, index=False, encoding="utf-8")
            logging.info(f"Reddit news saved to {FILE_PATH}")
            print(f"Reddit news saved to {FILE_PATH}")

except Exception as e:
    logging.error(f"Reddit API Error: {e}")
    print("Error fetching Reddit data. Check logs.")
