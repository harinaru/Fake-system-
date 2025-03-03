import requests
import logging
logging.info("Initializing fact-checking module...")

def check_fact_with_snopes(text):
    search_url = f'https://www.snopes.com/?s={text}'
    response = requests.get(search_url)
    return response.url if response.status_code == 200 else "No match found"

def check_fact_with_google_fact_check(text):
    search_url = f'https://toolbox.google.com/factcheck/explorer/search/{text}'
    response = requests.get(search_url)
    return response.url if response.status_code == 200 else "No match found"

logging.info("Fact-checking module loaded.")