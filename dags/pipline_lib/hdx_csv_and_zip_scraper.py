import requests
from bs4 import BeautifulSoup
import argparse
import os
import json
import zipfile
import io

def fetch_page(url, headers={'User-Agent': 'Mozilla/5.0'}):
    """Fetches the page content from a URL with error handling."""
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page: {e}")
        return None

def extract_metadata(soup, iso3_code, dest_folder):
    """Extracts metadata from a BeautifulSoup object and saves it as JSON."""
    info_table = soup.find("table", class_="table-condensed")
    if not info_table:
        print("Metadata table not found.")
        return

    data = {}
    for row in info_table.find_all("tr"):
        key = row.find("th").text.strip()
        value = row.find("td").text.strip()
        data[key] = value

    os.makedirs(dest_folder, exist_ok=True)
    metadata_filename = f"{iso3_code}_metadata.json"
    metadata_path = os.path.join(dest_folder, metadata_filename)
    with open(metadata_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Saved metadata to {metadata_path}")

def download_and_save_csv(link, dest_folder):
    """Downloads and saves a CSV file based on a link."""
    filename = link.find("span", class_="ga-download-resource-title").text.strip()
    csv_url = link['href'] if link['href'].startswith('http') else "https://data.humdata.org" + link['href']

    year = link.find_parent("li").find("div", class_="update-date").text.strip().split()[-1]
    filename_with_year = f"{filename.split('.')[0]}_{year}.csv"

    print(f"Downloading: {filename_with_year}")
    try:
        response = requests.get(csv_url)
        response.raise_for_status()

        filepath = os.path.join(dest_folder, filename_with_year)
        with open(filepath, "wb") as csv_file:
            csv_file.write(response.content)

        print(f" - Saved to {filepath}")
    except requests.exceptions.RequestException as e:
        print(f" - Error downloading {filename_with_year}: {e}")


def download_and_extract_zip(link, dest_folder):
    """Downloads a zip file and extracts csv files from it."""
    filename = link.find("span", class_="ga-download-resource-title").text.strip()
    zip_url = link['href'] if link['href'].startswith('http') else "https://data.humdata.org" + link['href']

    year = link.find_parent("li").find("div", class_="update-date").text.strip().split()[-1]
    filename_with_year = f"{filename.split('.')[0]}_{year}.zip"
    
    print(f"Downloading: {filename_with_year}")
    try:
        response = requests.get(zip_url)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            for info in zf.infolist():
                if info.filename.endswith(".csv"):
                    zf.extract(info, dest_folder)
                    print(f" - Extracted: {info.filename} to {dest_folder}")

    except Exception as e:  # Catch more specific exceptions if possible
        print(f" - Error downloading/extracting {filename_with_year}: {e}")


def download_csv_and_zip_from_hdx(iso3_code, base_folder):
    base_url = f"https://data.humdata.org/dataset/cod-ps-{iso3_code}"
    print(f"\n----- Starting download for ISO3 code: {iso3_code} -----")

    # Fetch and parse the page
    soup = fetch_page(base_url)
    if soup is None:  # Handle fetch errors
        return

    dest_folder = os.path.join(base_folder, "population_with_sadd")
    
    # Extract metadata 
    extract_metadata(soup, iso3_code, dest_folder)

    # Download CSV files
    csv_links = soup.select("li.resource-item a.ga-download[href$='.csv']")
    print(f"Found {len(csv_links)} CSV download links.")
    for link in csv_links:
        download_and_save_csv(link, dest_folder)

    # Download and extract ZIP files
    zip_links = soup.select("li.resource-item a.ga-download[href$='.zip']")
    print(f"Found {len(zip_links)} ZIP download links.")
    for link in zip_links:
        download_and_extract_zip(link, dest_folder)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download CSV files from HDX for a given ISO3 code.")
    parser.add_argument("iso3_code", help="The ISO3 code of the country (e.g., afg, usa)")
    parser.add_argument(
        "--base_folder",
        default="/home/gis/air-cint/geocint-airflow-pipeline/data/input",
        help="Base folder for data and data type files (default: /home/gis/air-cint/geocint-airflow-pipeline/data/input/hdx)",
    )
    args = parser.parse_args()

    if not args.iso3_code.isalpha() or len(args.iso3_code) != 3:
        print("Invalid ISO3 code. Please enter a 3-letter code.")
    else:
        download_csv_and_zip_from_hdx(args.iso3_code, args.base_folder)
