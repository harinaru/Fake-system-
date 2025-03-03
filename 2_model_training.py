import torch
import pickle
from transformers import BertForSequenceClassification, BertTokenizer
import logging
logging.info("Loading model and dataset...")
import pandas as pd 
df = pd.read_csv('processed_data.csv')
tokenizer = BertTokenizer.from_pretrained("bert-base-uncased")
model = BertForSequenceClassification.from_pretrained("bert-base-uncased", num_labels=2)

with open('model.pkl', 'wb') as f:
    pickle.dump(model, f)

logging.info("Model training module loaded and saved.")