import os
import json
import requests
import shutil
import tempfile
import subprocess
import logging

# Configure logging
logging.basicConfig(filename='process_country_data.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def download_file(url, dest):
    try:
        logging.info(f"Downloading {url} to {dest}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status() 
            with open(dest, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"Downloaded {url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")

def process_country_file(country_geojson_filename):
    try:
        country_code = os.path.splitext(os.path.basename(country_geojson_filename))[0]
        logging.info(f"Processing country: {country_code}")

        ckan_package_json_path = tempfile.mktemp()
        url = f"https://data.humdata.org/api/3/action/package_show?id=cod-ab-{country_code}"
        response = requests.get(url)

        if response.status_code == 200:
            with open(ckan_package_json_path, 'w') as f:
                f.write(response.text)

            country_in_dir = os.path.join("data", "input", f"{country_code}", "ocha_admin_boundaries")
            os.makedirs(country_in_dir, exist_ok=True)
            with open(ckan_package_json_path) as f:
                package_data = json.load(f)

            resources = package_data["result"]["resources"]
            for resource in resources:
                download_url = resource["download_url"]
                filename = os.path.basename(download_url)
                file_extension = filename.split('.')[-1]
                dest_path = os.path.join(country_in_dir, filename)

                logging.info(f"Resource: {filename} ({file_extension})")
                download_file(download_url, dest_path)

                if file_extension == "zip":
                    try:
                        logging.info(f"Unzipping {dest_path} to {country_in_dir}")
                        shutil.unpack_archive(dest_path, country_in_dir)
                    except shutil.ReadError as e:
                        logging.error(f"Error unzipping {dest_path}: {e}")

            last_modified = package_data["result"].get("last_modified")
            if last_modified:
                logging.info(f"Last modified date: {last_modified}")
            else:
                logging.warning(f"Last modified date not found for {country_code}")

            with open('dags/static_data/admin_level_display_names.json') as f:
                admin_level_display_names = json.load(f)

            source = admin_level_display_names.get(country_code, {}).get("source")
            if source == "no cod data":
                logging.info(f"No COD data for country: {country_code}")
                return

            country_out_dir = os.path.join("data", "output", "country_extractions", country_code, "202_admn")
            for adm_level in range(5):
                display_name = admin_level_display_names.get(country_code, {}).get(f"adm{adm_level}", f"adminboundary{adm_level}")
                for root, _, files in os.walk(country_in_dir):
                    for file in files:
                        if f"adm{adm_level}" in file:
                            src_path = os.path.join(root, file)
                            file_extension = file.split('.')[-1]
                            os.makedirs(country_out_dir, exist_ok=True)
                            dest_path = f"{country_out_dir}/{country_code}_admn_ad{adm_level}_py_s1_{source}_pp_{display_name}.{file_extension}"

                            try:
                                shutil.copy(src_path, dest_path)
                                logging.info(f"Copied {src_path} to {dest_path}")
                            except shutil.Error as e:
                                logging.error(f"Error copying {src_path} to {dest_path}: {e}")

                            if file_extension == "shp":
                                geojson_dest = f"{country_out_dir}/{country_code}_admn_ad{adm_level}_py_s1_{source}_pp_{display_name}.geojson"
                                try:
                                    logging.info(f"Converting {src_path} to {geojson_dest}")
                                    subprocess.run(["ogr2ogr", "-f", "GeoJSON", geojson_dest, src_path], check=True)
                                except subprocess.CalledProcessError as e:
                                    logging.error(f"Error converting {src_path} to GeoJSON: {e}")

                                if last_modified:
                                    with open(f"{country_out_dir}/{country_code}_admn_ad{adm_level}_py_s1_{source}_pp_{display_name}.last_modified.txt", 'w') as lm_file:
                                        lm_file.write(last_modified)
                                        logging.info(f"Written last modified date to {lm_file.name}")

        else:
            logging.error(f"Failed to fetch CKAN package data for {country_code}. Status code: {response.status_code}")

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {country_code}: {e}")

# Example usage
country_codes = ["zwe", "ken", "uga"] 
for country_code in country_codes:
    process_country_file(f"{country_code}.geojson")