import requests
import shutil
import os
import sys
import logging
import zipfile

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_and_organize_files(zip_file_path, extract_dir):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
        for root, _, files in os.walk(extract_dir):
            for file_name in files:
                if "reservoirs" in file_name:
                    dest_folder = "reservoirs"
                elif "dams" in file_name:
                    dest_folder = "dams"
                else:
                    continue
                
                old_file_path = os.path.join(root, file_name)
                new_folder_path = os.path.join(extract_dir, dest_folder)
                os.makedirs(new_folder_path, exist_ok=True)  # Create the destination folder if it doesn't exist
                new_file_path = os.path.join(new_folder_path, file_name)
                shutil.move(old_file_path, new_file_path)
                logging.info(f"Moved file: {file_name} to {new_file_path}")

def download_shapefile_zip():
    url = "https://zenodo.org/records/6163413/files/GeoDAR_v10_v11.zip?download=1"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        filename = "geodar"  # Define the filename
        save_directory = f"./data/input/geodar"
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)
        save_path = os.path.join(save_directory, filename + ".zip")
        with open(save_path, 'wb') as f:
            shutil.copyfileobj(response.raw, f)
        logging.info(f"File downloaded successfully to: {save_path}")
        
        extract_dir = save_directory
        extract_and_organize_files(save_path, extract_dir)
        
        logging.info("Files extracted and reorganized into 'reservoirs' and 'dams' folders within the same directory.")
    else:
        logging.error(f"Failed to download file. Status code: {response.status_code}")