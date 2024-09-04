import requests
from bs4 import BeautifulSoup
import os
import zipfile
import io
import shutil
import time

def fetch_page(url, headers={'User-Agent': 'Mozilla/5.0'}):
    """Fetches the page content from a URL with error handling."""
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page: {e}")
        return None

def download_and_extract_zip(link, dest_folder):
    """Downloads a zip file and extracts its contents."""
    filename = link.find("span", class_="ga-download-resource-title").text.strip()
    zip_url = link['href'] if link['href'].startswith('http') else "https://data.humdata.org" + link['href']

    print(f"Downloading: {filename}")
    try:
        response = requests.get(zip_url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            zf.extractall(dest_folder)
            print(f" - Extracted to {dest_folder}")

    except Exception as e: 
        print(f" - Error downloading/extracting {filename}: {e}")

def rename_and_copy_files(shapefiles_dir, dest_path, filename_prefix):
    # List of possible shapefile extensions
    shapefile_extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg']

    for filename in os.listdir(shapefiles_dir):
        src_path = os.path.join(shapefiles_dir, filename)
        file_extension = os.path.splitext(filename)[1].lower()
        if file_extension in shapefile_extensions:
            dest_filename = f"{filename_prefix}{file_extension}"
            dest_path_with_filename = os.path.join(dest_path, dest_filename)
            print(f"Copying: {src_path} to {dest_path_with_filename}")
            shutil.copy2(src_path, dest_path_with_filename)

def download_shapefiles_from_page(country_name, dest_path, filename_prefix):
    country_name = country_name.lower()
    base_url = f"https://data.humdata.org/dataset/{country_name}-healthsites"
    print(f"\n----- Starting download from: {base_url} -----")

    soup = fetch_page(base_url)
    if soup is None: 
        return

    # Extract to a temporary 'shapefiles' subdirectory
    shapefiles_dir = os.path.join(dest_path, "shapefiles")
    os.makedirs(shapefiles_dir, exist_ok=True)

    zip_links = soup.select("li.resource-item a.ga-download[href$='.zip']")
    print(f"Found {len(zip_links)} ZIP download links.")
    for link in zip_links:
        download_and_extract_zip(link, dest_path)  

    shapefiles_dir = os.path.join(dest_path, "shapefiles")

   # Rename and copy files from the 'shapefiles' subdirectory
    rename_and_copy_files(shapefiles_dir, dest_path, filename_prefix)

    time.sleep(2)

    # Remove the temporary 'shapefiles' directory
    shutil.rmtree(shapefiles_dir)
    print(f"Removed temporary directory: {shapefiles_dir}")

if __name__ == "__main__":
    country_name = "zimbabwe"
    country_code = "zwe"
    dest_path = f"data/{country_name}"
    filename_prefix = f"{country_code}heal_hea_pt_s3_osm_pp_healthsites" 

    download_shapefiles_from_page(country_name, dest_path, filename_prefix)