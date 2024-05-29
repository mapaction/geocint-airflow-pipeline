import os
import json
import requests
import shutil
import tempfile
import subprocess

def download_file(url, dest):
    print(f"Downloading {url} to {dest}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded {url}")

def process_country_file(country_geojson_filename):
    country_code = os.path.splitext(os.path.basename(country_geojson_filename))[0]
    print(f"Processing country: {country_code}")

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
        zipfile = ""
        for resource in resources:
            download_url = resource["download_url"]
            filename = os.path.basename(download_url)
            file_extension = filename.split('.')[-1]
            dest_path = os.path.join(country_in_dir, filename)
            print(f"Resource: {filename} ({file_extension})")
            download_file(download_url, dest_path)
            
            if file_extension == "zip":
                zipfile = dest_path
                print(f"Unzipping {zipfile} to {country_in_dir}")
                shutil.unpack_archive(zipfile, country_in_dir)

        if not zipfile:
            print(f"No zipfile found for country: {country_code}")
            return

        last_modified = package_data["result"]["last_modified"]
        print(f"Last modified date: {last_modified}")

        with open('dags/static_data/admin_level_display_names.json') as f:
            admin_level_display_names = json.load(f)

        source = admin_level_display_names.get(country_code, {}).get("source")
        if source == "no cod data":
            print(f"No COD data for country: {country_code}")
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
                        shutil.copy(src_path, dest_path)
                        print(f"Copied {src_path} to {dest_path}")
                        
                        if file_extension == "shp":
                            geojson_dest = f"{country_out_dir}/{country_code}_admn_ad{adm_level}_py_s1_{source}_pp_{display_name}.geojson"
                            print(f"Converting {src_path} to {geojson_dest}")
                            subprocess.run(["ogr2ogr", "-f", "GeoJSON", geojson_dest, src_path], check=True)
                            with open(f"{country_out_dir}/{country_code}_admn_ad{adm_level}_py_s1_{source}_pp_{display_name}.last_modified.txt", 'w') as lm_file:
                                lm_file.write(last_modified)
                                print(f"Written last modified date to {lm_file.name}")