import os
import argparse
import json
from datetime import datetime
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
import pycountry

def list_and_save_data_types_with_metadata(country, base_folder):
    """
    Lists available data types with metadata for a country and saves to JSON files.
    """
    Configuration.create(hdx_site='prod', user_agent='My HDX App', hdx_read_only=True)

    try:
        pycountry.countries.search_fuzzy(country)[0]
    except LookupError:
        print(f"Invalid country name: {country}")
        exit(1)

    query = f'title:{country.upper()}'
    datasets = Dataset.search_in_hdx(query, rows=1000)

    data_types = set()

    for dataset in datasets:
        if 'tags' in dataset:
            for tag in dataset['tags']:
                data_type = tag['name']
                data_types.add(data_type) 
                
                
                # Split and join data type with underscores for directory name
                dir_name = "_".join(data_type.split(" "))
                dir_name = "_".join(dir_name.split("-"))

                output_dir = os.path.join(base_folder, dir_name)  # Use modified dir_name
                os.makedirs(output_dir, exist_ok=True)

                metadata = {
                    "Source": dataset.get('organization', 'N/A'),  # Organization name
                    "Contributor": dataset.get('maintainer', 'N/A'),  # Maintainer name
                    "Time Period of the Dataset [?]": dataset.get('dataset_date', 'N/A'),
                    # Handle metadata_modified if it's a string:
                    "Modified [?]": (
                        datetime.fromtimestamp(int(dataset['metadata_modified'])).strftime('%d %B %Y')
                        if dataset['metadata_modified'].isdigit()
                        else dataset['metadata_modified'] # Leave as is if not a digit
                    ),
                    "Dataset Added on HDX [?]": (
                        datetime.fromtimestamp(int(dataset['metadata_created'])).strftime('%d %B %Y')
                        if dataset['metadata_created'].isdigit()
                        else dataset['metadata_created'] # Leave as is if it's not a digit
                    ),
                    "Expected Update Frequency": dataset.get('data_update_frequency', 'N/A'),
                    "Location": country,
                    "Visibility": dataset.get('visibility', 'N/A'),
                    "License": dataset.get('license_id', 'N/A'),
                    "Methodology": dataset.get('methodology', 'N/A'),
                    "Caveats / Comments": dataset.get('caveats', 'N/A'),
                    "Tags": ", ".join(t['name'] for t in dataset['tags']),
                    "File Format": ", ".join(set(r['format'] for r in dataset.get_resources()))
                }
                
                filename = os.path.join(output_dir, f"{dir_name}_metadata.json")
                with open(filename, 'w') as file:
                    json.dump(metadata, file, indent=4)
                print(f"Metadata for {data_type} in {country} saved to {filename}")

    # Save data types to text file
    data_types_file = os.path.join(base_folder, 'hdx_datatypes.txt')
    with open(data_types_file, 'w') as file:
        for data_type in sorted(data_types):
            file.write(data_type + '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List and save HDX data types with metadata for a country.")
    parser.add_argument("country", help="Name of the country (e.g., Mali)")
    parser.add_argument("country_code", help="Name of the country (e.g., mli)")
    parser.add_argument(
        "--input_folder",
        default="/home/gis/air-cint/geocint-airflow-pipeline/data/input",
        help="Base folder for data and data type files (default: /home/gis/air-cint/geocint-airflow-pipeline/data/input/hdx)")
    
    args = parser.parse_args()

    base_folder = f"{args.input_folder}/{args.country_code}/hdx"

    list_and_save_data_types_with_metadata(args.country, base_folder)
