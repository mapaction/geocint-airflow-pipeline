import os
import zipfile
import json

def get_country_codes(countries_dir):
    """Get country codes from JSON files."""
    country_details = []
    for filename in os.listdir(countries_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(countries_dir, filename)
            try:
                with open(filepath, 'r') as file:
                    data = json.load(file)
                    if 'features' in data and len(data['features']) > 0:
                        for feature in data['features']:
                            if ('properties' in feature and 
                                'iso' in feature['properties'] and 
                                'name_en' in feature['properties']):
                                country_code = feature['properties']['iso'].lower()
                                country_name = feature['properties']['name_en']
                                country_details.append((country_code, country_name))
                            else:
                                print(f"Missing 'iso' or 'name_en' in properties for file: {filename}")
                    else:
                        print(f"No features found in file: {filename}")
            except json.JSONDecodeError:
                print(f"Error decoding JSON from file: {filename}")
            except Exception as e:
                print(f"Unexpected error while processing file {filename}: {e}")
    return country_details

def contains_shp(zip_path):
    """Check if a .zip file contains any .shp files."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                if file.endswith('.shp'):
                    return True
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file.")
    return False

def extract_shapefile_components(zip_path, extract_dir):
    """Extract all shapefile-related files from a .zip file to the specified directory."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                if file.endswith(('.shp', '.shx', '.dbf', '.prj', '.cpg', '.qix', '.fix')):
                    zip_ref.extract(file, extract_dir)
                    print(f"Extracted {file} to {extract_dir}")
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file.")
    except Exception as e:
        print(f"Unexpected error while extracting files from {zip_path}: {e}")
