import requests
import pandas as pd
from time import sleep
import traceback
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_session():
    """Configure a requests session with retries."""
    retry_strategy = Retry(
        total=3,  # Retry up to 3 times
        backoff_factor=1,  # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504, 400],  # Retry on these errors
        allowed_methods=["GET"]
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session


def fetch_gds_ids(session, search_terms, retstart=0, retmax=1000):
    """Fetch GEO Dataset IDs using ESearch API."""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    # search_query = " OR ".join([f'"{term}"' for term in search_terms])
    search_query = "((hic) OR human[Organism]) OR Tads[Description]"

    params = {
        "db": "gds",
        "term": search_query,
        "retstart": retstart,
        "retmax": retmax,
        "retmode": "json"
    }

    response = session.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()
    
    return data.get("esearchresult", {}).get("idlist", [])


def fetch_dataset_details(session, gds_id):
    """Fetch dataset metadata using ESummary API and parse structured JSON."""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {"db": "gds", "id": gds_id, "retmode": "json"}

    response = session.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()

    # Extract dataset details
    try:
        result = data["result"]
        dataset_info = result.get(gds_id, {})

        dataset = {
            "GDS_ID": dataset_info.get("uid", ""),
            "Accession": dataset_info.get("accession", ""),
            "Title": dataset_info.get("title", ""),
            "Summary": dataset_info.get("summary", ""),
            "Organism": dataset_info.get("taxon", ""),
            "Dataset_Type": dataset_info.get("gdstype", ""),
            "Num_Samples": dataset_info.get("n_samples", ""),
            "Bioproject": dataset_info.get("bioproject", ""),
            "PubMed_IDs": "; ".join(dataset_info.get("pubmedids", [])),
            "FTP_Link": dataset_info.get("ftplink", ""),
            "Samples": "; ".join([f"{s['accession']} ({s['title']})" for s in dataset_info.get("samples", [])])
        }
        return dataset

    except Exception as e:
        print(f"Error parsing dataset {gds_id}: {str(e)}")
        print(traceback.format_exc())
        return None


def process_geo_datasets(session, search_terms, max_datasets=100):
    """Fetch and process GEO datasets based on search terms."""
    all_data = []
    retmax = 1000
    retstart = 0
    counter = 0

    while counter < max_datasets:
        gds_ids = fetch_gds_ids(session, search_terms, retstart=retstart, retmax=retmax)
        if not gds_ids:
            break  # No more results

        for gds_id in gds_ids:
            try:
                print(f"Processing {counter + 1}: {gds_id}")
                dataset = fetch_dataset_details(session, gds_id)
                if dataset:
                    all_data.append(dataset)
                counter += 1

                if counter >= max_datasets:
                    break  # Stop if we reach the limit

                sleep(0.34)  # Rate limiting

            except Exception as e:
                print(f"Error processing {gds_id}: {str(e)}")
                print(traceback.format_exc())

        retstart += retmax  # Move to the next page

    return pd.DataFrame(all_data)


if __name__ == "__main__":
    search_terms = ["intact Hi-C", "in situ Hi-C", "dilution Hi-C", "SPRITE"]
    session = get_session()
    
    df = process_geo_datasets(session, search_terms, max_datasets=float('inf'))
    df.to_excel(f"geo_datasets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", index=False)
    print(f"Saved {len(df)} records to geo_datasets.xlsx")
