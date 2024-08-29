import os
import geopandas as gpd
from geo_admin_tools.utils.file_operations import get_country_codes
from geo_admin_tools.utils.download_utils import scrape_and_download_zip_files
from geo_admin_tools.utils.metadata_utils import capture_metadata
from geo_admin_tools.src.admin_linework import find_admlevel_column, generate_admin_linework
import logging
import shutil

def main_method(country_codes, data_in_path, data_mid_path, data_out_path):
    """Main function to iterate over country codes and handle data processing."""
    
    for iso_code, country_name in country_codes:
        download_dir = data_in_path
        os.makedirs(download_dir, exist_ok=True)
        
        logging.info(f"Processing country: {country_name} ({iso_code})")
        scrape_and_download_zip_files(iso_code, download_dir, data_mid_path)

        # Process shapefiles and generate linework using the new mid directory
        for root, dirs, files in os.walk(data_mid_path):
            for file in files:
                if file.endswith('.shp'):  # Only process .shp files
                    file_path = os.path.join(root, file)
                    process_shapefiles(file_path, iso_code, data_out_path)
        
        def cleanup(data_out_path):
            """Clean up temporary directories after processing."""
            adm_level_out_dir = os.path.join(data_out_path, "202_admn/admin_level")
            shutil.rmtree(adm_level_out_dir)
            logging.info(f"Cleaned up temporary directories.")
        
        cleanup(data_out_path)

    
def process_shapefiles(filepath, iso_code, data_out_path):
    """Process shapefiles and extract relevant administrative boundary levels."""
    adm_level_out_dir = os.path.join(data_out_path, "202_admn/admin_level")
    linework_out_dir = os.path.join(data_out_path, "202_admn")
    
    source_abbr = "hdx"

    realname_mapping = {
        0: "country",
        1: "parish",  # Changed from "region"
        2: "province",
        3: "district",
        4: "subdistrict",
    }

    try:
        gdf = gpd.read_file(filepath)
        adm_column = find_admlevel_column(gdf) # cases were admnlevel collumn found

        if adm_column:
            logging.info(f"Found {adm_column} column in {filepath}. Available levels: {gdf[adm_column].unique()}")
            
            
            for level in gdf[adm_column].unique():
                level_gdf = gdf[gdf[adm_column] == level]
                output_dir = adm_level_out_dir
                realname = realname_mapping.get(level, "Admin")
                output_file = f"{iso_code}_admn_ad{level}_py_s1_{source_abbr}_pp_{realname}.shp"
                level_outfile = os.path.join(output_dir, output_file)
                os.makedirs(output_dir, exist_ok=True)
                level_gdf.to_file(level_outfile)
                logging.info(f"Created {level_outfile}")
                
            
            generate_admin_linework(gdf, linework_out_dir, iso_code, source_abbr, realname, adm_column)
        else:
            # Handle cases where no 'admLevel' column is found by using filenames
            logging.info(f"No 'admLevel' column found in {filepath}. Using filename-based processing.")
            process_shapefiles_by_filename(filepath, iso_code, data_out_path)

    except Exception as e:
        logging.error(f"Error processing {filepath}: {e}")

def process_shapefiles_by_filename(filepath, iso_code, data_out_path):
    """Fallback processing when no admLevel column is found. Derives levels from filenames."""
    adm_level_out_dir = os.path.join(data_out_path, "202_admn/admin_level")
    linework_out_dir = os.path.join(data_out_path, "202_admn")
    
    source_abbr = "hdx"  # Placeholder for source abbreviation
    filename = os.path.basename(filepath).lower()

    level_mapping = {
        "adm0": 0,
        "adm1": 1,
        "adm2": 2,
        "adm3": 3,
        "adm4": 4,
        "disputed": 86,
        "coastline": 99
    }
    
    level = None
    for key in level_mapping:
        if key in filename:
            level = level_mapping[key]
            break
    
    if level is not None:
        realname_mapping = {
            0: "country",
            1: "parish",
            2: "province",
            3: "district",
            4: "subdistrict",
        }
        realname = realname_mapping.get(level, "Admin")
        output_file = f"{iso_code}_admn_ad{level}_py_s1_{source_abbr}_pp_{realname}.shp"
        level_outfile = os.path.join(adm_level_out_dir, output_file)
        gdf = gpd.read_file(filepath)
        os.makedirs(adm_level_out_dir, exist_ok=True)
        gdf.to_file(level_outfile)
        logging.info(f"Created {level_outfile} based on filename pattern.")
        
        # Generate admin linework after processing
        generate_admin_linework(gdf, linework_out_dir, iso_code, source_abbr, realname, level)
        
    else:
        logging.warning(f"Could not determine level from filename: {filename}")



