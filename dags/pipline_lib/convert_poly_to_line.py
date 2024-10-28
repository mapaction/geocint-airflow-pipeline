import os
import sys
import arcpy
from pathlib import Path
import logging
import json
import shutil
import subprocess

# Configure logging
logging.basicConfig(filename='process_country_data.log', level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

def process_country_data(docker_worker_dir, country_code, last_modified=None):
    """
    Processes country data by converting polygons to lines, removing duplicates,
    and copying/processing administrative files.

    Args:
        docker_worker_dir (str or Path): The Docker worker working directory.
        country_code (str): The country code to process.
        last_modified (str, optional): The last modified date (if available).
    """
    try:
        admn_folder_path, temp_gdb_path = setup_environment(docker_worker_dir, country_code)
        convert_polygons_to_lines(admn_folder_path, temp_gdb_path)
        remove_duplicate_linework(temp_gdb_path)  # Implement your duplicate removal logic here
        copy_and_process_admin_files(docker_worker_dir, country_code, last_modified)
        export_lines_to_shapefiles(temp_gdb_path, admn_folder_path)

        print("Process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

def setup_environment(docker_worker_dir, country_code):
    """Sets up the workspace and creates a temporary geodatabase."""
    admn_folder_path = Path(docker_worker_dir / "data" / "output" / "country_extractions" / country_code / "202_admn")
    arcpy.env.workspace = str(admn_folder_path)

    temp_gdb_name = "tempGDB.gdb"
    temp_gdb_path = str(admn_folder_path / temp_gdb_name)
    logging.info(f"Creating temporary geodatabase at: {temp_gdb_path}")

    try:
        if arcpy.Exists(temp_gdb_path):
            arcpy.Delete_management(temp_gdb_path)
        arcpy.CreateFileGDB_management(str(admn_folder_path), temp_gdb_name)
    except Exception as e:
        logging.error(f"Error creating geodatabase: {e}")
        raise

    return admn_folder_path, temp_gdb_path

def convert_polygons_to_lines(admn_folder_path, temp_gdb_path):
    """Converts polygon shapefiles to lines and stores them in the geodatabase."""
    logging.info(f"Searching for shapefiles in: {admn_folder_path}")
    fc_list = arcpy.ListFeatureClasses("*_py_*.shp")

    if not fc_list:
        logging.error(f"No shapefiles with '_py_' found in {admn_folder_path}")
        raise FileNotFoundError(f"No shapefiles with '_py_' found in {admn_folder_path}")

    for poly_fc in fc_list:
        poly_fc_path = str(admn_folder_path / poly_fc)
        logging.info(f"Processing polygon shapefile: {poly_fc_path}")

        try:
            arcpy.RepairGeometry_management(poly_fc)
            line_fc_name = poly_fc.replace("_py_", "_ln_")
            line_fc_path = str(Path(temp_gdb_path) / line_fc_name)

            if arcpy.Exists(line_fc_path):
                arcpy.Delete_management(line_fc_path)

            arcpy.PolygonToLine_management(poly_fc_path, line_fc_path[:-4], "IDENTIFY_NEIGHBORS")
            logging.info(f"Converted to line: {line_fc_path[:-4]}")
        except Exception as e:
            logging.error(f"Error processing {poly_fc_path}: {e}")

def remove_duplicate_linework(temp_gdb_path):
    """Removes duplicate linework within the geodatabase."""
    arcpy.env.workspace = temp_gdb_path
    line_fcs = arcpy.ListFeatureClasses(feature_type='Line')
    logging.info(f"Processing line feature classes in geodatabase: {temp_gdb_path}")
    # Add your duplicate removal logic here (using line_fcs)

def copy_and_process_admin_files(docker_worker_dir, country_code, last_modified=None):
    """Copies and processes files for each administrative level."""
    with open('dags/static_data/admin_level_display_names.json') as f:
        admin_level_display_names = json.load(f)

    source = admin_level_display_names.get(country_code, {}).get("source")
    if source == "no cod data":
        logging.info(f"No COD data for country: {country_code}")
        sys.exit(1)

    for adm_level in range(5):
        arcpy.env.workspace = str(Path(docker_worker_dir) / "data" / "output" / country_code / "ocha_admin_boundaries")
        files_to_process = [f for f in os.listdir() if f"adm{adm_level}" in f]
        country_out_dir = str(Path(docker_worker_dir) / "data" / "output" / country_code / "ocha_admin_boundaries")

        if not files_to_process:
            logging.warning(f"No files found for adm_level {adm_level} in {country_code}")
            continue

        for file in files_to_process:
            src_path = os.path.join(arcpy.env.workspace, file)
            file_extension = file.split('.')[-1]
            # ... (rest of the copy_and_process_admin_files logic)

def export_lines_to_shapefiles(temp_gdb_path, admn_folder_path):
    """Exports line feature classes back to shapefiles in the 202_admn folder."""
    if arcpy.Exists(temp_gdb_path):
        arcpy.env.workspace = temp_gdb_path
        for line_fc in arcpy.ListFeatureClasses(feature_type='Line'):
            output_folder = str(admn_folder_path)
            arcpy.FeatureClassToShapefile_conversion([line_fc], output_folder)

if __name__ == "__main__":
    try:
        if len(sys.argv) < 3:
            raise ValueError("Please provide the Docker worker working directory and country code as command-line arguments.")
        docker_worker_dir = Path(sys.argv[1])
        country_code = sys.argv[2]

        admn_folder_path, temp_gdb_path = setup_environment(docker_worker_dir, country_code)
        convert_polygons_to_lines(admn_folder_path, temp_gdb_path)
        remove_duplicate_linework(temp_gdb_path)
        copy_and_process_admin_files(docker_worker_dir, country_code)
        export_lines_to_shapefiles(temp_gdb_path, admn_folder_path)

        print("Process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")