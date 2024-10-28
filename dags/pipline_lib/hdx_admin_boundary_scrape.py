import requests
from bs4 import BeautifulSoup
import argparse
import os
import zipfile
import io
from concurrent.futures import ThreadPoolExecutor

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
    """Downloads a zip file and extracts shapefiles from it."""
    filename = link.find("span", class_="ga-download-resource-title").text.strip()
    zip_url = link['href'] if link['href'].startswith('http') else "https://data.humdata.org" + link['href']
    year = link.find_parent("li").find("div", {"class": "update-date"}).text.strip().split()[-1]
    filename_with_year = f"{filename.split('.')[0]}_{year}.zip"

    filepath = os.path.join(dest_folder, filename_with_year)
    if os.path.exists(filepath):
        print(f"Skipping: {filename_with_year} already exists.")
        return

    print(f"Downloading: {filename_with_year}")
    try:
        response = requests.get(zip_url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Selective extraction
            for member in zf.infolist():
                if member.filename.endswith(('.shp', '.dbf', '.shx', '.prj')): 
                    zf.extract(member, dest_folder)
            print(f" - Extracted to {dest_folder}")

    except Exception as e:
        print(f" - Error downloading/extracting {filename_with_year}: {e}")


def download_zip_from_hdx(iso3_code, base_folder):
    base_url = f"https://data.humdata.org/dataset/cod-ab-{iso3_code}"
    print(f"\n----- Starting download for ISO3 code: {iso3_code} -----")

    soup = fetch_page(base_url)
    if soup is None:
        return

    dest_folder = os.path.join(base_folder)
    os.makedirs(dest_folder, exist_ok=True)

    zip_links = soup.select("li.resource-item a.ga-download[href$='.zip']")
    print(f"Found {len(zip_links)} ZIP download links.")

    # Use a ThreadPoolExecutor to download concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers as needed
        executor.map(download_and_extract_zip, zip_links, [dest_folder] * len(zip_links))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download zipped shapefiles from HDX for a given ISO3 code.")
    parser.add_argument("iso3_code", help="The ISO3 code of the country (e.g., afg, usa)")
    parser.add_argument(
        "--base_folder",
        default="my_boundary_files",
        help="Base folder for data files",
    )
    args = parser.parse_args()

    args.iso3_code = args.iso3_code.lower()

    download_dir = args.base_folder
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    if len(args.iso3_code) != 3:
        print(f"Invalid ISO3 code. Please enter a 3-letter code.  >>>>>> you typed {args.iso3_code} {len(args.iso3_code)}")
    else:
        download_zip_from_hdx(args.iso3_code, download_dir)