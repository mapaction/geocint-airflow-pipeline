import os
import sys
import json
import subprocess
import logging

logger = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_and_download(api_key, file_path):

    country_name, country_code = extract_country_name(file_path)
    if country_name and country_code:
        download_healthsites(country_name, country_code, api_key)

def extract_country_name(file_path):
    with open(file_path, 'r') as file:
        try:
            data = json.load(file)
            if 'features' in data and isinstance(data['features'], list):
                for feature in data['features']:
                    if 'properties' in feature and 'name_en' in feature['properties'] and 'iso' in feature['properties']:
                        country_properties = feature['properties']
                        country_name = country_properties.get('name_en', '').lower()
                        country_code = country_properties.get('iso', '').lower()
                        return country_name, country_code
            else:
                print(f"Error: Missing or invalid 'features' array in file {file_path}")
        except json.JSONDecodeError:
            print(f"Error: JSON decode error in file {file_path}")
    return None, None

   
def download_healthsites(country_name, country_code, api_key):
    subprocess.run(['python', 'scripts/download_healthsites.py', api_key, country_name, country_code])