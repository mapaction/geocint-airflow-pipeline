import os
import argparse
from hdx.utilities.downloader import Download
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

hdx_config = None

def download_country_data(country, data_type, destination_folder):
    """
    Downloads specified data type for a country from HDX to a destination folder.

    Args:
        iso3 (str): The ISO3 code of the country (e.g., 'GBR' for United Kingdom).
        data_type (str): The type of data to download (e.g., 'roads', 'railways').
        destination_folder (str): Path to the folder where data should be saved.

    Raises:
        ValueError: If no matching datasets are found or download fails.
    """
    
    # Initialize HDX configuration (adjust HDX_KEY if necessary)
    global hdx_config
    if hdx_config is None:
        # Initialize HDX configuration only once
        hdx_config = Configuration.create(
            hdx_site="prod", user_agent="My HDX App", hdx_read_only=True
        )
    
    # Search for relevant datasets
    query = f'{country} AND {data_type}'  # Customize the query as needed
    datasets = Dataset.search_in_hdx(query, rows=5)  # Get up to 5 results
    
    if not datasets:
        raise ValueError(f"No {data_type} datasets found for {country}.")

    # Select and download the first dataset
    dataset = datasets[0] 
    resources = dataset.get_resources()
    if resources:
        resource = resources[0]  # Take the first resource 
        
        # Create download object
        url, path = resource.download(folder=destination_folder)

        if not os.path.exists(path):
            raise ValueError("Download failed. Check network connection or resource availability.")
        
        print(f"Downloaded {data_type} data for {country} to {path}")
    else:
        raise ValueError(f"No resources found for {dataset['title']}.")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download HDX data for a country.")
    parser.add_argument("country", help="Name of the country (e.g., mali)")
    parser.add_argument("data_type", help="Type of data to download (e.g., roads, railways)")
    parser.add_argument("dest_folder", help="Type of data to download (e.g., roads, railways)")

    #parser.add_argument("destination_folder", help="Path to save the downloaded data")
    # parser.add_argument("country_code", help="Iso3 of the country (e.g., mli)")

    args = parser.parse_args()
    
 
    # base_folder = "/home/gis/air-cint/geocint-airflow-pipeline/data/output/country_extractions" 
    # dest_folder = os.path.join(base_folder, args.country_code, 'hdx' ,args.data_type)
    os.makedirs(args.dest_folder, exist_ok=True)  # Create folders if they don't exist 

    download_country_data(args.country, args.data_type, args.dest_folder)