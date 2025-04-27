import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

TARGET_FILES = ["hic", "bedpe", "bw", "bedgraph", "cool", "tsv", "csv", "bed"]

def setup_session():
    """Configure requests session with headers and retry policy"""
    session = requests.Session()

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Referer": "https://data.4dnucleome.org/browse/?experiments_in_set.experiment_type.display_title=in+situ+Hi-C&experiments_in_set.experiment_type.display_title=Dilution+Hi-C&experiments_in_set.experiment_type.display_title=Micro-C&experiments_in_set.experiment_type.display_title=DNase+Hi-C&experiments_in_set.experiment_type.display_title=TCC&experimentset_type=replicate&type=ExperimentSetReplicate"
    }

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(headers)

    return session

def fetch_experiment_sets(session):
    """Main scraping function with pagination and deduplication"""
    base_params = [
        ('experiments_in_set.experiment_type.display_title', 'in situ Hi-C'),
        ('experiments_in_set.experiment_type.display_title', 'Dilution Hi-C'),
        ('experiments_in_set.experiment_type.display_title', 'Micro-C'),
        ('experiments_in_set.experiment_type.display_title', 'DNase Hi-C'),
        ('experiments_in_set.experiment_type.display_title', 'TCC'),
        ('experiments_in_set.experiment_type.display_title', 'Electron Tomography'),
        ('experiments_in_set.experiment_type.display_title', 'BLISS'),
        ('experiments_in_set.experiment_type.display_title', 'MC-3C'),
        ('experiments_in_set.experiment_type.display_title', 'MARGI'),
        ('experiments_in_set.experiment_type.display_title', 'GAM'),
        ('experiments_in_set.experiment_type.display_title', 'ChIA-Drop'),
        ('experiments_in_set.experiment_type.display_title', 'DNA SPRITE'),
        ('experiments_in_set.experiment_type.display_title', 'TrAC-loop'),
        ('experiments_in_set.experiment_type.display_title', 'RNA-DNA SPRITE'),
        ('experiments_in_set.experiment_type.display_title', 'Capture Hi-C'),
        ('experiments_in_set.experiment_type.display_title', '4C-seq'),
        ('experiments_in_set.experiment_type.display_title', 'sci-Hi-C'),
        ('experiments_in_set.experiment_type.display_title', 'sn-Hi-C'),
        ('experiments_in_set.experiment_type.display_title', 'single cell Hi-C'),
        ('experiments_in_set.experiment_type.display_title', 'PLAC-seq'),
        ('experiments_in_set.experiment_type.display_title', 'in situ ChIA-PET'),
        ('experiments_in_set.experiment_type.display_title', 'HiChIP'),
        ('experiments_in_set.experiment_type.display_title', 'ChIA-PET'),
        ('experimentset_type', 'replicate'),
        ('type', 'ExperimentSetReplicate'),
    ]

    processed_titles = set()
    experiment_data = []
    page_size = 25
    current_page = 0

    while True:
        params = base_params.copy()
        params.append(('from', str(current_page * page_size)))
        print("Fetching page", current_page)
        try:
            response = session.get(
                "https://data.4dnucleome.org/browse/",
                params=params
            )
            response.raise_for_status()

            data = response.json()
            items = data.get("@graph", [])

            if not items:
                print("All items exhausted!")
                break  # Exit loop when no more items

            for item in items:
                title = item.get("display_title")
                if not title or title in processed_titles:
                    continue

                # Fetch experiment details
                exp_response = session.get(
                    f"https://data.4dnucleome.org/experiment-set-replicates/{title}/?format=json"
                )
                exp_response.raise_for_status()
                print("Fetching experiment: ", title)
                # print("")
                experiment_data.append(exp_response.json())
                processed_titles.add(title)

                # Rate limiting between item requests
                time.sleep(0.5)

            current_page += 1
            # Rate limiting between page requests
            # time.sleep(0.5)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break

    return experiment_data


def process_experiment_data(experiment_data):
    """Process raw API data into structured format for Excel with one row per file"""
    processed_rows = []

    for item in experiment_data:
        # Extract basic fields
        base_data = {
            "Title": item.get("dataset_label", ""),
            "Description": item.get("description", ""),
            "Study": item.get("study", ""),
            "Condition": item.get("condition", ""),
            "Source Lab": item.get("lab", {}).get("display_title", ""),

            "4DN Link": "https://data.4dnucleome.org/files-processed/" + item.get("display_title", ""),
        }

        # Extract files
        files = [
            f for f in item.get("processed_files", [])
            if f.get("file_format", {}).get("display_title") in TARGET_FILES
        ]

        print(f"Title: {item.get('dataset_label', 'N/A')}, Found {len(files)} files")

        # Create one row per file or one row if no files
        if not files:
            continue
        else:
            for file in files:
                processed_rows.append({
                    **base_data,
                    "File": "https://data.4dnucleome.org" + file.get("href", ""),
                    "File Size": file.get("file_size", ""),
                    "File Type": file.get("file_type", {}),
                    "File Type Detailed": file.get("file_type_detailed", {}), 
                    "File Description": file.get("file_format", {}).get("display_title", ""),
                    "Open Data URL": file.get("open_data_url", "N/A"),
                    "Bio Source": file.get("track_and_facet_info", {}).get("biosource_name", "N/A")
                })

    return processed_rows


if __name__ == "__main__":
    session = setup_session()
    data = fetch_experiment_sets(session)

    print(f"Collected {len(data)} experiment sets")
    processed_data = process_experiment_data(data)
    df = pd.DataFrame(processed_data)
    df.to_excel("experiment_sets_test.xlsx", index=False)
    print("Excel file created successfully")
