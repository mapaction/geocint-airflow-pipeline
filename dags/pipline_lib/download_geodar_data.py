# import requests
# import shutil
# import os
# import logging
# import zipfile

# # Set up logging configuration
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def extract_and_organize_files(zip_file_path, extract_dir, layer_1, layer_2):
#     with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#         zip_ref.extractall(extract_dir)
        
#         for root, _, files in os.walk(extract_dir):
#             for file_name in files:
#                 if layer_1 in file_name:
#                     dest_folder = layer_1
#                 elif layer_2 in file_name:
#                     dest_folder = layer_2
#                 else:
#                     continue
                
#                 old_file_path = os.path.join(root, file_name)
#                 new_folder_path = os.path.join(extract_dir, dest_folder)
#                 os.makedirs(new_folder_path, exist_ok=True)  # Create the destination folder if it doesn't exist
#                 new_file_path = os.path.join(new_folder_path, file_name)
#                 shutil.move(old_file_path, new_file_path)
#                 logging.info(f"Moved file: {file_name} to {new_file_path}")

# def download_shapefile_zip(url, layer_1, layer_2):
#     response = requests.get(url, stream=True)
#     if response.status_code == 200:
#         filename = f"{layer_1}_{layer_2}"  # Define the filename
#         save_directory = f"./data/input/geodar"
#         if not os.path.exists(save_directory):
#             os.makedirs(save_directory)
#         save_path = os.path.join(save_directory, filename + ".zip")
#         with open(save_path, 'wb') as f:
#             shutil.copyfileobj(response.raw, f)
#         logging.info(f"File downloaded successfully to: {save_path}")
        
#         extract_dir = save_directory
#         extract_and_organize_files(save_path, extract_dir)
        
#         logging.info(f"Files extracted and reorganized into {layer_1} and {layer_2} folders within the same directory.")
#     else:
#         logging.error(f"Failed to download file. Status code: {response.status_code}")
# v2
import requests
import shutil
import os
import logging
import zipfile
import json

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_and_organize_files(zip_file_path, extract_dir, layer_1, layer_2):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
        for root, _, files in os.walk(extract_dir):
            for file_name in files:
                if layer_1 in file_name:
                    dest_folder = layer_1
                elif layer_2 in file_name:
                    dest_folder = layer_2
                else:
                    continue
                
                old_file_path = os.path.join(root, file_name)
                new_folder_path = os.path.join(extract_dir, dest_folder)
                os.makedirs(new_folder_path, exist_ok=True)
                new_file_path = os.path.join(new_folder_path, file_name)
                shutil.move(old_file_path, new_file_path)
                logging.info(f"Moved file: {file_name} to {new_file_path}")

def add_doi_metadata(metadata_path, doi):
    metadata = {
        "DOI": doi,
        "Source": "GeoDAR"
    }
    with open(metadata_path, 'w') as metadata_file:
        json.dump(metadata, metadata_file)
    logging.info(f"DOI metadata added to {metadata_path}")

def download_shapefile_zip(url, layer_1, layer_2, doi):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        filename = "GeoDAR_v10_v11"
        save_directory = "./data/input/geodar"
        if not os.path.exists(save_directory):
            os.makedirs(save_directory)
        save_path = os.path.join(save_directory, filename + ".zip")
        with open(save_path, 'wb') as f:
            shutil.copyfileobj(response.raw, f)
        logging.info(f"File downloaded successfully to: {save_path}")
        
        extract_dir = save_directory
        extract_and_organize_files(save_path, extract_dir, layer_1, layer_2)
        
        metadata_path = os.path.join(extract_dir, "metadata.json")
        add_doi_metadata(metadata_path, doi)
        
        logging.info(f"Files extracted, reorganized, and metadata added in {layer_1} and {layer_2} folders within the same directory.")
    else:
        logging.error(f"Failed to download file. Status code: {response.status_code}")

