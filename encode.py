# encode.py
import requests
import time
import json
import pandas as pd
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Shared utility functions
def setup_session():
    """Configure requests session with headers and retry policy"""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json"
    })
    return session

def save_ids(ids, filename):
    """Save IDs to a JSON file"""
    with open(filename, 'w') as f:
        json.dump(ids, f)

# ENCODE-specific functions
def fetch_experiment_list(session):
    """Fetch initial list of experiments"""
    url = "https://www.encodeproject.org/search/"
    params = {
        "type": "Experiment",
        "control_type!": "*",
        "status": "released",
        "perturbed": "false",
        "limit": "all",
        "format": "json"
    }
    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()["@graph"]

def fetch_experiment_details(session, experiment_id):
    """Fetch detailed experiment data"""
    url = f"https://www.encodeproject.org{experiment_id}"
    response = session.get(url, params={"format": "json"})
    response.raise_for_status()
    return response.json()

def process_encode_data(experiment):
    """Process experiment data into structured format"""
    base_data = {
        "Assay": experiment.get("assay_term_name", ""),
        "Description": experiment.get("description", ""),
        "Date Released": experiment.get("date_released", ""),
        "Lab": experiment.get("lab", {}).get("title", ""),
        "Institute": experiment.get("lab", {}).get("institute_name", ""),
        "Biosample Summary": experiment.get("biosample_summary", ""),
        "Experiment ID": experiment["@id"].split("/")[-2]
    }
    
    processed = []
    for file in experiment.get("files", []):
        processed.append({
            **base_data,
            "File URL": f"https://www.encodeproject.org{file.get('href', '')}",
            "File Format": file.get("file_format", ""),
            "File Type": file.get("output_type", ""),  # Most relevant type field
            "File Size": file.get("file_size", "")
        })
    return processed

def main():
    session = setup_session()
    
    # Fetch experiment list
    experiments = fetch_experiment_list(session)
    print(f"Found {len(experiments)} experiments")
    
    # Save experiment IDs
    experiment_ids = [exp["@id"] for exp in experiments]
    save_ids(experiment_ids, "encode_ids.json")
   
    counter = 0 
    # Process all experiments
    all_data = []
    for idx, exp in enumerate(experiments):
        if counter == 25:
            break
        
        
        counter += 1
        try:
            print(f"Processing {idx+1}/{len(experiments)}: {exp['@id']}")
            details = fetch_experiment_details(session, exp["@id"])
            all_data.extend(process_encode_data(details))
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"Failed to process {exp['@id']}: {str(e)}")
    
    # Create DataFrame and save
    df = pd.DataFrame(all_data)
    df.to_excel("encode_experiments.xlsx", index=False)
    print("Done! Saved to encode_experiments.xlsx")

if __name__ == "__main__":
    main()