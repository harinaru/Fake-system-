import os
import pandas as pd
import logging
from newsapi import NewsApiClient

# Setup logging (append to existing log file)
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'  # Append mode (does not overwrite)
)

logging.info("===== Script Started =====")

# Initialize API Client
API_KEY = 'd1da71979955435b87f656e87f15dad7'
newsapi = NewsApiClient(api_key=API_KEY)

try:
    # Fetch worldwide political news
    political_news = newsapi.get_everything(q='politics', language='en', sort_by='publishedAt')

    # Handle API failure
    if political_news is None or 'articles' not in political_news:
        logging.error("Failed to fetch news articles. API response is invalid.")
        print("API response error. Check logs.")
        exit()

    # Extract articles
    articles = political_news.get('articles', [])

    if not articles:
        logging.warning("No articles fetched from the API.")
        print("No new data available.")
    else:
        # Process into DataFrame
        df_politics = pd.DataFrame(articles, columns=['title', 'source', 'publishedAt', 'url'])

        # Extract source name safely
        df_politics['source'] = df_politics['source'].apply(lambda x: x.get('name', 'Unknown') if isinstance(x, dict) else 'Unknown')

        # Convert publishedAt to Timestamp and Date
        df_politics['timestamp'] = pd.to_datetime(df_politics['publishedAt'], errors='coerce')
        df_politics['date'] = df_politics['timestamp'].dt.date

        # Rearrange columns
        df_politics = df_politics[['title', 'source', 'timestamp', 'date', 'url', 'publishedAt']]

        # File path
        file_path = 'newsapi.csv'

        # Ensure safe CSV writing
        if os.path.exists(file_path):
            try:
                logging.info("Reading existing newsapi.csv")

                # Load existing data safely (handle encoding + bad lines)
                df_existing = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')

                # Ensure all required columns exist before appending
                if set(df_politics.columns) == set(df_existing.columns):
                    df_new = df_politics[~df_politics['title'].isin(df_existing['title'])]
                    if not df_new.empty:
                        df_new.to_csv(file_path, mode='a', index=False, header=False, encoding='utf-8')
                        logging.info(f"{len(df_new)} new articles appended to newsapi.csv")
                    else:
                        logging.info("No new articles. All titles already exist.")
                else:
                    logging.error("CSV column structure mismatch. Check manually.")
                    print("CSV column structure mismatch. Check logs.")
            except Exception as e:
                logging.error(f"Error reading newsapi.csv: {e}")
                print("Error reading existing CSV file. Check logs.")
        else:
            # Write new CSV file without overwriting
            df_politics.to_csv(file_path, index=False, encoding='utf-8')
            logging.info("newsapi.csv updated with new data.")

        print("Process completed. Check log for details.")

except Exception as e:
    logging.error(f"API fetch error: {e}")
    print("Error fetching news data. Check logs.")

# Ensure logs are written immediately
logging.shutdown()
