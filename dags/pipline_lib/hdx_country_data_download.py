import os
import argparse
from hdx.utilities.downloader import Download
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
import logging

# Setup logging for better debugging
logger = logging.getLogger(__name__)

# Configuration for HDX
hdx_config = None

def download_country_data(country, data_type, destination_folder):
    global hdx_config
    if hdx_config is None:
        hdx_config = Configuration.create(
            hdx_site="prod", user_agent="My HDX App", hdx_read_only=True
        )

    query = f'{country} AND {data_type}'
    datasets = Dataset.search_in_hdx(query, rows=10) 

    if not datasets:
        logger.warning(f"No {data_type} datasets found for {country}.")
        return 

    for dataset in datasets:
        resources = dataset.get_resources()
        if resources:
            for resource in resources:
                resource_object = Resource.read_from_hdx(resource['id'])
                
                resource_url = resource_object.get('url')
                
                if resource_url:
                    filename = resource_url.split("/")[-1]
                    dest_filepath = os.path.join(destination_folder, filename)

                    if os.path.exists(dest_filepath):
                        if os.path.getsize(dest_filepath) == resource_object.get('file_size'):
                            logger.info(f"Skipping download: {filename} already exists with the same size at {dest_filepath}")
                        else:
                            # Overwrite the file if sizes are different
                            logger.info(f"Overwriting {filename} in {dest_filepath} (size mismatch)")
                            os.remove(dest_filepath)

                    try:
                        with Download(user_agent='My HDX App') as downloader:
                            downloader.download_file(
                                resource_url,
                                folder=destination_folder,
                                filename=filename,
                            )
                        logger.info(f"Downloaded {filename} to {dest_filepath}")
                        return  
                    except Exception as e:
                        logger.error(f"Error downloading {filename}: {e}")
                else:
                    logger.warning(f"Resource '{resource['name']}' in dataset '{dataset['title']}' has no valid URL.")
        else:
            logger.warning(f"No resources found for dataset '{dataset['title']}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download HDX data for a country.")
    parser.add_argument("country", help="Name of the country (e.g., 'Mali')")
    parser.add_argument("data_type", help="Type of data to download (e.g., 'roads')")
    parser.add_argument("dest_folder", help="Path to save the downloaded data")
    args = parser.parse_args()

    os.makedirs(args.dest_folder, exist_ok=True)  
    download_country_data(args.country, args.data_type, args.dest_folder)
