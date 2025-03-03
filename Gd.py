import os
import pandas as pd
import logging
import requests
import zipfile
from bs4 import BeautifulSoup

# ----------------- CONFIGURATION -----------------
GDELT_BASE_URL = "http://data.gdeltproject.org/events/"
BASE_FOLDER = os.path.join("datasets", "gdelt")  # Root folder
TRACK_FILE = os.path.join(BASE_FOLDER, "complete_files.csv")
LOG_FILE = os.path.join(BASE_FOLDER, "gdelt_download.log")

# ----------------- SETUP LOGGING -----------------
os.makedirs(BASE_FOLDER, exist_ok=True)  # Ensure base folder exists
logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s", filemode="a"
)

# ----------------- LOAD DOWNLOADED FILES -----------------
if os.path.exists(TRACK_FILE):
    downloaded_files = pd.read_csv(TRACK_FILE)["filename"].tolist()
else:
    downloaded_files = []

def get_storage_path(filename):
    """Determine correct storage path based on the filename."""
    if not filename[:8].isdigit():
        return BASE_FOLDER  # If filename format is unknown, store at base

    year = filename[:4]  # Extract year (e.g., 2025, 2024)
    month = filename[4:6]  # Extract month (e.g., 02, 11)

    # For 2025-2013: Store inside year/month subfolders
    if int(year) >= 2013:
        year_folder = os.path.join(BASE_FOLDER, year)
        month_folder = os.path.join(year_folder, month)
        os.makedirs(month_folder, exist_ok=True)
        return month_folder

    # For 2006-2013: Store inside year folder only
    else:
        year_folder = os.path.join(BASE_FOLDER, year)
        os.makedirs(year_folder, exist_ok=True)
        return year_folder

def save_file_name(filename):
    """Append downloaded file names to tracking CSV."""
    df_new = pd.DataFrame({"filename": [filename]})
    if os.path.exists(TRACK_FILE):
        df_existing = pd.read_csv(TRACK_FILE)
        df_updated = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_updated = df_new
    df_updated.to_csv(TRACK_FILE, index=False)

# ----------------- FUNCTION TO DOWNLOAD & EXTRACT FILE -----------------
def download_and_extract(file_url, file_name):
    storage_path = get_storage_path(file_name)  # Get correct storage directory
    zip_path = os.path.join(storage_path, file_name)  # Path to save ZIP
    csv_path = zip_path.replace(".zip", "")  # Expected CSV file path

    try:
        # Download ZIP file
        logging.info(f"Downloading: {file_name}")
        response = requests.get(file_url, stream=True)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)

        # Extract CSV file from ZIP
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(storage_path)  # Extract to correct folder

        os.remove(zip_path)  # Remove ZIP after extraction
        save_file_name(file_name)  # Update tracking file
        logging.info(f"✅ Successfully downloaded & extracted: {file_name} → {storage_path}")

    except Exception as e:
        logging.error(f"❌ Error processing {file_name}: {e}")

# ----------------- MAIN FUNCTION -----------------
def fetch_gdelt_data():
    try:
        response = requests.get(GDELT_BASE_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract all ZIP links
        csv_links = [link["href"] for link in soup.find_all("a", href=True) if link["href"].endswith(".export.CSV.zip")]

        new_files = [f for f in csv_links if f not in downloaded_files]

        if not new_files:
            logging.info("No new files to download.")
            return

        # Download only new files
        for file_link in new_files:
            file_name = file_link.split("/")[-1]
            file_url = f"{GDELT_BASE_URL}{file_name}"
            download_and_extract(file_url, file_name)

    except Exception as e:
        logging.error(f"❌ Error fetching GDELT data: {e}")

# ----------------- RUN SCRIPT -----------------
if __name__ == "__main__":
    fetch_gdelt_data()
    print("Process completed. Check logs for details.")
