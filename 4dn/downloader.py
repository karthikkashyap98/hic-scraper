import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

# Constants
EXCEL_FILE = "./4dn.xlsx"  # Update with the actual Excel file name
DOWNLOAD_DIR = "downloads"  # Update with the desired download directory
MAX_THREADS = 5  # Adjust the number of threads as needed

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Helper function to get access keys
def get_access_keys():
    """Retrieve access keys from environment variables or prompt the user."""
    access_key_id = os.getenv("4DN_ACCESS_KEY_ID")
    access_key_secret = os.getenv("4DN_ACCESS_KEY_SECRET")

    if not access_key_id or not access_key_secret:
        print("Access keys not found in environment variables.")
        print("Please generate your access keys by following the instructions at:")
        print("https://data.4dnucleome.org/help/user-guide/programmatic-access")  # Replace with the actual link
        access_key_id = input("Enter your 4DN Access Key ID: ").strip()
        access_key_secret = input("Enter your 4DN Access Key Secret: ").strip()

    return access_key_id, access_key_secret

# Retrieve access keys
ACCESS_KEY_ID, ACCESS_KEY_SECRET = get_access_keys()

def download_file(url, file_name):
    """Download a file from the given URL and save it to the specified file name."""
    try:
        print(f"Downloading {file_name}...")
        with requests.get(url, auth=(ACCESS_KEY_ID, ACCESS_KEY_SECRET), stream=True) as response:
            response.raise_for_status()
            with open(file_name, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Downloaded {file_name}")
        return file_name
    except Exception as e:
        print(f"Failed to download {file_name}: {str(e)}")
        return None

def get_file_name_from_url(url):
    """Extract the file name from the URL."""
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)

def main():
    # Read the Excel file
    if not os.path.exists(EXCEL_FILE):
        print(f"Source Excel file {EXCEL_FILE} does not exist. Please run the scraper first or manually create the file.")
        return
    
    df = pd.read_excel(EXCEL_FILE)
    
    # Extract file URLs
    file_urls = df["File"].tolist()
    
    # Prepare download tasks
    download_tasks = []
    for url in file_urls:
        file_name = os.path.join(DOWNLOAD_DIR, get_file_name_from_url(url))
        download_tasks.append((url, file_name))
    
    # Download files using multi-threading
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(download_file, url, file_name) for url, file_name in download_tasks]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                print(f"Success: {result}")

if __name__ == "__main__":
    main()