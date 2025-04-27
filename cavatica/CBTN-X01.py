import os
import requests
import time
from pathlib import Path
from tqdm import tqdm

API_URL = "https://cavatica-api.sbgenomics.com/v2"
PARENT_ID = "6762e5fbd2814e34cfa170e7"
OUTPUT_DIR = "CBTN-X01"
PAGE_LIMIT = 100
RATE_LIMIT_SECONDS = 0.5  # Wait between requests

def get_auth_token():
    token = os.environ.get("SBG_AUTH_TOKEN")
    if not token:
        print("Missing environment variable 'SBG_AUTH_TOKEN'.")
        print("You can create a token here: https://docs.sevenbridges.com/docs/the-authentication-token")
        token = input("Please enter your CAVATICA API token: ").strip()
    return token

def get_file_list(token):
    files = []
    offset = 0

    print("Fetching file list with pagination...")

    while True:
        url = f"{API_URL}/files/{PARENT_ID}/list?offset={offset}&limit={PAGE_LIMIT}"
        headers = {
            "accept": "application/json",
            "X-SBG-Auth-Token": token
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])
        if not items:
            break

        files.extend(items)
        offset += PAGE_LIMIT
        time.sleep(RATE_LIMIT_SECONDS)

    print(f"Total files found: {len(files)}")
    return files

def get_download_url(file_id, token):
    url = f"{API_URL}/files/{file_id}/download_info"
    headers = {
        "accept": "application/json",
        "X-SBG-Auth-Token": token
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return data["url"]

def download_file(download_url, filename):
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

def main():
    token = get_auth_token()
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)
    count = 0
    file_list = get_file_list(token)

    for file_info in tqdm(file_list, desc="⬇️ Downloading files"):
        file_id = file_info["id"]
        file_name = file_info["name"]
        file_path = output_path / file_name
        count += 1
        tqdm.write(f"Downloading {count}/{len(file_list)}: {file_name}")
        
        if count == 10:
            print("Stopping after 10 files for testing.")
            break
        if file_path.exists():
            tqdm.write(f"Skipping (already exists): {file_name}")
            continue

        try:
            download_url = get_download_url(file_id, token)
            download_file(download_url, file_path)
            time.sleep(RATE_LIMIT_SECONDS)
        except Exception as e:
            tqdm.write(f"Failed to download {file_name}: {e}")

    print("🎉 All downloads completed!")

if __name__ == "__main__":
    main()
