# 1_data_preprocessing.py
import pandas as pd
import re
import numpy as np
import logging
import pickle
import os
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

try:
    logging.info("Loading datasets...")

    # Define correct paths
    true_path = os.path.join("Datasets", "ISOT", "True.csv")
    fake_path = os.path.join("Datasets", "ISOT", "Fake.csv")
    wel_fake_path = os.path.join("Datasets", "WELFAKE", "WELFake_Dataset.csv")

    # Load datasets
    true_df = pd.read_csv(true_path)
    fake_df = pd.read_csv(fake_path)
    wel_fake_df = pd.read_csv(wel_fake_path)

    logging.info("Loaded datasets...")

    # Ensure 'label' column exists in WELFake
    assert 'label' in wel_fake_df.columns, "WELFake dataset missing 'label' column"

    # Assign labels
    true_df['label'] = 1  # Real news
    fake_df['label'] = 0  # Fake news

    # Merge datasets
    df = pd.concat([true_df, fake_df, wel_fake_df], ignore_index=True)
    logging.info("dataset merged ")

    # Handle missing values
    df['text'] = df['text'].fillna("")  # Replace NaN values with empty string
    logging.info("Handled Missing VAlue ...")

    # Text cleaning function
    def clean_text(text):
        if isinstance(text, str):  # Ensure text is a string
            text = re.sub(r'[^a-zA-Z0-9 ]', '', text.lower())
            return text
        return ""  # Return empty string for non-string values
    
    logging.info("Text CLeann of  datasets Started ...")
    df['clean_text'] = df['text'].apply(clean_text)
    logging.info("Text CLeann of  datasets Completed ...")

    # TF-IDF Vectorization
    logging.info("TfidfVectorizer Started ...")
    vectorizer = TfidfVectorizer(ngram_range=(1,3), smooth_idf=True)
    X_tfidf = vectorizer.fit_transform(df['clean_text'])
    logging.info("TfidfVectorizer Completed ...")

    # Save vectorizer
    with open('vectorizer.pkl', 'wb') as f:
        pickle.dump(vectorizer, f)
    
    logging.info("TfidfVectorizer PIckle Svaed ...")

    # Save processed data
    df.to_csv('processed_data.csv', index=False)

    logging.info("Data preprocessing completed successfully.")
except Exception as e:
    logging.error(f"Error in data preprocessing: {e}")
