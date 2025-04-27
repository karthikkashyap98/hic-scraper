import os
import requests
import time
import pandas as pd
from tqdm import tqdm

# Constants
SEARCH_URL = 'https://repo-prod.prod.sagebase.org/repo/v1/search'
BUNDLE_URL_TEMPLATE = 'https://repo-prod.prod.sagebase.org/repo/v1/entity/{id}/bundle2'
DOWNLOAD_URL_TEMPLATE = 'https://www.synapse.org/Portal/filehandleassociation?associatedObjectId={file_id}&associatedObjectType=FileEntity&fileHandleId={dataFileHandleId}'
TARGET_FILES = ["hic", "bedpe", "bw", "bedgraph", "cool", "tsv", "csv", "bed"]
RESULT_EXCEL_FILE = "synapse_files_metadata.xlsx"
PAGE_SIZE = 50
RATE_LIMIT_SECONDS = 0.4  # To avoid being throttled

def get_auth_token():
    token = os.getenv("SYNAPSE_TOKEN")
    if not token:
        print("🔐 Please set the SYNAPSE_TOKEN environment variable.")
        print("👉 You can generate a token here: https://www.synapse.org/#!PersonalAccessTokens:")
        exit(1)
    return token

def get_common_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def search_files(file_type, headers):
    start = 0
    all_hits = []

    while True:
        query = {
            "queryTerm": [],
            "booleanQuery": [
                {"key": "name", "value": file_type},
                {"key": "node_type", "value": "file"}
            ],
            "facetOptions": [],
            "returnFields": [],
            "start": start,
            "size": PAGE_SIZE
        }

        try:
            response = requests.post(SEARCH_URL, json=query, headers=headers)
            response.raise_for_status()
            data = response.json()
            hits = data.get("hits", [])
            filtered_hits = [hit for hit in hits if hit.get("name").split('.')[-1] == file_type]
            all_hits.extend(filtered_hits)

            if len(hits) < PAGE_SIZE:
                break  # Done paging

            start += PAGE_SIZE
            time.sleep(RATE_LIMIT_SECONDS)
        except requests.exceptions.RequestException as e:
            print(f"❌ Error during search: {e}")
            break

    return all_hits

def fetch_bundle_info(file_id, headers):
    url = BUNDLE_URL_TEMPLATE.format(id=file_id)

    payload = {
        "includeEntity": True,
        "includeAnnotations": True,
        "includePermissions": True,
        "includeEntityPath": True,
        "includeHasChildren": True,
        "includeAccessControlList": True,
        "includeFileHandles": True,
        "includeTableBundle": True,
        "includeRootWikiId": True,
        "includeBenefactorACL": True,
        "includeDOIAssociation": True,
        "includeFileName": True,
        "includeThreadCount": True,
        "includeRestrictionInformation": True
    }

    extra_headers = {
        "accept": "application/json; charset=UTF-8",
        "origin": "https://www.synapse.org",
        "referer": "https://www.synapse.org/",
        "content-type": "application/json; charset=UTF-8",
    }

    combined_headers = {**headers, **extra_headers}

    try:
        resp = requests.post(url, json=payload, headers=combined_headers)
        if resp.status_code == 403:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching bundle for {file_id}: {e}")
        return None

def collect_metadata():
    token = get_auth_token()
    headers = get_common_headers(token)
    records = []

    for filetype in TARGET_FILES:
        print(f"🔍 Searching files with type: {filetype}...")
        hits = search_files(filetype, headers)

        for hit in tqdm(hits, desc=f"Processing {filetype}"):
            file_id = hit.get("id")
            name = hit.get("name")

            bundle = fetch_bundle_info(file_id, headers)
            time.sleep(RATE_LIMIT_SECONDS)

            if bundle is None:
                continue

            entity = bundle.get("entity", {})
            annotations = bundle.get("annotations", {}).get("annotations", {})
            dataFileHandleId = entity.get("dataFileHandleId")

            download_url = DOWNLOAD_URL_TEMPLATE.format(
                file_id=file_id, dataFileHandleId=dataFileHandleId
            ) if dataFileHandleId else None

            metadata = {
                "id": file_id,
                "name": name,
                "download_url": download_url,
            }

            for key, value in annotations.items():
                metadata[key] = ", ".join(value.get("value", []))

            records.append(metadata)

    return records

def write_to_excel(records):
    if not records:
        print("⚠️ No data to write.")
        return

    df = pd.DataFrame(records)
    df.to_excel(RESULT_EXCEL_FILE, index=False)
    print(f"✅ Metadata written to {RESULT_EXCEL_FILE}")

def main():
    records = collect_metadata()
    write_to_excel(records)

if __name__ == "__main__":
    main()
