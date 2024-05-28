import coreapi
import shutil
import os
import sys
import logging
import zipfile
import geopandas as gpd
import pandas as pd

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_shapefile_zip(api_key, country, country_code):
    # Load the schema document
    client = coreapi.Client()
    schema = client.get("https://healthsites.io/api/docs/")
    
    # Interact with the API endpoint
    action = ["shapefile", "download"]
    params = {"country": country, "api-key": api_key}
    
    try:
        response = client.action(schema, action, params=params)
        if response:
            # Get the filename from the response URL
            filename = f"{country_code}.zip"
            save_directory = f"./data/input/mapaction/healthsites/{country_code}" 
            extract_dir = f"./data/mid/mapaction/healthsites/{country_code}"
            out_dir = f"./data/output/country_extractions/{country_code}/215_heal"
            
            logging.info(f"Filename - {filename} ------ save dir - {save_directory}")
            
            if not os.path.exists(save_directory):
                os.makedirs(save_directory)
            
            # Specify the full path to save the file
            save_path = os.path.join(save_directory, filename)
            
            # Open the file in binary write mode
            with open(save_path, 'wb') as f:
                # Copy the content of the response file-like object to the new file
                shutil.copyfileobj(response, f)
            
            logging.info(f"File downloaded successfully to: {save_path}")
            
            # Extract and rename files
            extract_zip(save_path, extract_dir, country_code)

            convert_to_gpkg(extract_dir, out_dir, country_code)

            # Delete the original ZIP file
            shutil.rmtree(extract_dir)
        else:
            logging.error("Unexpected response format: %s", response)
    except coreapi.exceptions.ErrorMessage as e:
        logging.error("Error: %s", e)

def extract_zip(zip_file_path, extract_dir, country_code):
    # Create the extraction directory if it doesn't exist
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
    
    # Extract the contents of the ZIP file to the extraction directory
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

def convert_to_gpkg(zip_file_path, extract_dir, country_code):
    layers = {}
    for root, _, files in os.walk(zip_file_path):
        for file_name in files:
            if file_name.endswith(".shp"):
                shapefile_path = os.path.join(root, file_name)
                gdf = gpd.read_file(shapefile_path)
                
                # Handle field name collisions
                gdf.columns = [f"{col}_{i}" if gdf.columns.tolist().count(col) > 1 else col for i, col in enumerate(gdf.columns)]
                
                # Reproject to a projected CRS before calculating centroids
                gdf = gdf.to_crs(epsg=4326)
                gdf['geometry'] = gdf.centroid
                gdf = gdf.to_crs(epsg=4326)  # Convert back to geographic CRS
                
                # Add to layers dictionary
                layer_name = os.path.splitext(file_name)[0]
                layers[layer_name] = gdf

    # Combine layers into one GeoDataFrame
    combined_gdf = gpd.GeoDataFrame(pd.concat(layers.values(), ignore_index=True), crs=layers[list(layers.keys())[0]].crs)
    gpkg_path = os.path.join(extract_dir, f"{country_code}_heal_hea_pt_s3_healthsites_pp_healthfacilities.gpkg")
    combined_gdf.to_file(gpkg_path, layer="combined_layers", driver="GPKG")
    
    logging.info(f"Saved combined GeoPackage to: {gpkg_path}")