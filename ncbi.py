import requests
import xml.etree.ElementTree as ET
import pandas as pd
from time import sleep
import traceback

def fetch_gds_ids(search_term=".hic"):
    """Fetch GEO Dataset IDs using ESearch"""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "gds",
        "term": search_term,
        "retmax": 1000,
        "retmode": "json"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()

    return data.get("esearchresult", {}).get("idlist", [])

def parse_dataset_xml(xml_content):
    """Parse EFetch XML response for GDS details"""

    root = ET.fromstring(xml_content)
    dataset = {
        "title": root.findtext(".//DocumentSummary/Title"),
        "summary": root.findtext(".//DocumentSummary/Summary"),
        "organism": root.findtext(".//DocumentSummary/taxon"),
        "dataset_type": root.findtext(".//DocumentSummary/gdsType"),
        "accession": root.findtext(".//DocumentSummary/Accession"),
        "files": []
    }

    # Extract supplementary files
    for supp_file in root.findall(".//DocumentSummary/SuppFile"):
        dataset["files"].append({
            "url": supp_file.findtext("url"),
            "file_type": supp_file.findtext("type"),
            "size": supp_file.findtext("size")  # May not always be present
        })

    return dataset

def parse_geo_entry(content):
    """Parse GEO dataset entry into structured dictionary"""
    entry = {}
    lines = content.decode('utf-8').split('\n')

    # Extract title and description
    entry['Title'] = lines[1].strip()

    for line in lines[2:]:
        line = line.strip()
        if not line:
            continue

        # Handle key-value pairs
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()



            if key == 'Submitter supplied':
                entry['Description'] = value
            elif key == 'Organism':
                entry['Organism'] = value
            elif key == 'Type':
                entry['Dataset_Type'] = value
            elif key == 'Platform':
                # Split platform and sample count
                platform_parts = value.split()
                entry['Platform_ID'] = platform_parts[0]
                entry['Samples'] = int(platform_parts[1].replace('Samples', ''))
            elif key == 'FTP download':
                entry['FTP_URL'] = value.split()[-1]
                entry['File_Types'] = value[:value.index('ftp')-1]
            elif key == 'Series':
                # Handle series accession and ID
                parts = [p.strip() for p in value.split('\t') if p]
                for part in parts:
                    if 'Accession:' in part:
                        entry['Accession'] = part.split()[-1]
                    elif 'ID:' in part:
                        entry['GEO_ID'] = part.split()[-1]

        # Handle multi-line description
        elif line.startswith('(Submitter supplied)'):
            entry['Description'] = line.split(')', 1)[1].strip()
        elif 'Description' in entry and not line.startswith(('Organism:', 'Type:')):
            entry['Description'] += ' ' + line

    # Clean up description
    if 'Description' in entry:
        entry['Description'] = entry['Description'].replace('more...', '').strip()

    return entry



def fetch_dataset_details(gds_id):
    """Fetch dataset metadata using EFetch"""
    base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "gds",
        "id": gds_id,
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    return parse_geo_entry(response.content)


def process_geo_datasets():
    """Main processing function"""
    all_data = []

    # Get list of dataset IDs
    gds_ids = fetch_gds_ids()
    print(f"Found {len(gds_ids)} datasets")

    # Save IDs for future reference
    pd.Series(gds_ids).to_csv("geo_dataset_ids.csv", index=False)
    counter = 0
    # Process each dataset
    for idx, gds_id in enumerate(gds_ids):
        counter += 1
        try:
            print(f"Processing {idx+1}/{len(gds_ids)}: {gds_id}")
            dataset = fetch_dataset_details(gds_id)

            all_data.append(dataset)

            # Create rows for files
            if counter == 100:
                break
            # NCBI rate limiting (3 requests/sec)
            sleep(0.34)

        except Exception as e:
            print(f"Error processing {gds_id}: {str(e)}")
            print(traceback.format_exc())


    return pd.DataFrame(all_data)

if __name__ == "__main__":
    df = process_geo_datasets()


    # Save to Excel
    df.to_excel("geo_datasets.xlsx", index=False)
    print(f"Saved {len(df)} records to geo_datasets.xlsx")
