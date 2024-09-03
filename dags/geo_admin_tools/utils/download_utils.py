import requests
from bs4 import BeautifulSoup
import os
from geo_admin_tools.utils.file_operations import contains_shp, extract_shapefile_components
from geo_admin_tools.utils.metadata_utils import update_metadata

base_url = "https://data.humdata.org/dataset/cod-ab-{iso}"

def download_file(url, output_path):
    """Download a file from a URL."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Download completed: {output_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to download file from {url}. Error: {e}")
        return False

def scrape_and_download_zip_files(iso_code, data_in_directory, data_mid_directory):
    """Scrape and download .zip files for a given ISO code."""
    page_url = base_url.replace("{iso}", iso_code)
    try:
        response = requests.get(page_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        
        zip_links = soup.find_all('a', href=True)
        zip_files = [link['href'] for link in zip_links if ".zip" in link['href']]
        
        if not zip_files:
            print(f"No .zip files found on the page for {iso_code}.")
            return
        
        for zip_file in zip_files:
            if not zip_file.startswith("http"):
                zip_file = "https://data.humdata.org" + zip_file
            
            # Correct the path construction here
            zip_dir = os.path.join(data_in_directory, "zip")
            file_name = os.path.join(zip_dir, os.path.basename(zip_file))

            os.makedirs(zip_dir, exist_ok=True)
            print(f"Downloading {zip_file} to {file_name}")
            if download_file(zip_file, file_name):
                if contains_shp(file_name):
                    os.makedirs(data_mid_directory, exist_ok=True)
                    extract_shapefile_components(file_name, data_mid_directory)
                    update_metadata(iso_code, "download", file_name)
                    # Remove or refactor this call to prevent duplicate processing
                    # process_admALL_files(data_mid_directory, iso_code, data_mid_directory)

    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve the page for {iso_code}. Error: {e}")
