import os
import geopandas as gpd
from geo_admin_tools.utils.file_operations import get_country_codes
from geo_admin_tools.utils.download_utils import scrape_and_download_zip_files
from geo_admin_tools.utils.metadata_utils import capture_metadata
from geo_admin_tools.src.admin_linework import find_admlevel_column, generate_admin_linework
from geo_admin_tools.src.constants import REALNAME_MAPPING, LEVEL_MAPPING  # Import constants
import logging
import shutil

def main_method(country_codes, data_in_path, data_mid_path, data_out_path):
    """Main function to iterate over country codes and handle data processing."""

    for iso_code, country_name in country_codes:
        download_dir = data_in_path
        os.makedirs(download_dir, exist_ok=True)

        logging.info(f"Processing country: {country_name} ({iso_code})")
        scrape_and_download_zip_files(iso_code, download_dir, data_mid_path)

        # Collect all shapefiles
        all_shapefiles = []
        for root, dirs, files in os.walk(data_mid_path):
            for file in files:
                if file.endswith('.shp'):
                    file_path = os.path.join(root, file)
                    all_shapefiles.append(file_path)

        # Categorize shapefiles based on the presence of 'admLevel' column
        admLevel_shapefiles = []
        non_admLevel_shapefiles = []

        for file_path in all_shapefiles:
            try:
                gdf = gpd.read_file(file_path)
                adm_column = find_admlevel_column(gdf)
                if adm_column:
                    admLevel_shapefiles.append(file_path)
                else:
                    non_admLevel_shapefiles.append(file_path)
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")

        # Decide which shapefiles to process
        if admLevel_shapefiles:
            logging.info("Found shapefiles with 'admLevel' column. Processing these only.")
            for file_path in admLevel_shapefiles:
                process_shapefiles(file_path, iso_code, data_out_path)
        else:
            logging.info("No shapefiles with 'admLevel' column found. Using filename-based processing.")
            for file_path in non_admLevel_shapefiles:
                process_shapefiles_by_filename(file_path, iso_code, data_out_path)

        cleanup(data_out_path)


def process_shapefiles(filepath, iso_code, data_out_path):
    """Process shapefiles that contain the 'admLevel' column."""

    adm_level_out_dir = os.path.join(data_out_path, "202_admn/admin_level")
    linework_out_dir = os.path.join(data_out_path, "202_admn")
    disputed_out_dir = os.path.join(data_out_path, "202_admn")
    coastline_out_dir = os.path.join(data_out_path, "211_elev")
    os.makedirs(adm_level_out_dir, exist_ok=True)
    os.makedirs(linework_out_dir, exist_ok=True)
    os.makedirs(disputed_out_dir, exist_ok=True)
    os.makedirs(coastline_out_dir, exist_ok=True)

    source_abbr = determine_source_abbr(filepath)

    try:
        gdf = gpd.read_file(filepath)
        adm_column = find_admlevel_column(gdf)

        if adm_column:
            logging.info(f"Found {adm_column} column in {filepath}. Available levels: {gdf[adm_column].unique()}")

            for level in gdf[adm_column].unique():
                level_gdf = gdf[gdf[adm_column] == level]
                output_dir = adm_level_out_dir
                realname = REALNAME_MAPPING.get(level, "Admin")
                output_file = f"{iso_code}_admn_ad{level}_py_s1_{source_abbr}_pp_{realname}.shp"

                if level in [86, 87]:
                    output_dir = disputed_out_dir
                    output_file = f"{iso_code}_admn_ad0_ln_s0_{source_abbr}_pp_disputedBoundaries.shp"
                elif level == 99:
                    output_dir = coastline_out_dir
                    output_file = f"{iso_code}_elev_cst_ln_s0_{source_abbr}_pp_coastline.shp"

                level_outfile = os.path.join(output_dir, output_file)
                os.makedirs(output_dir, exist_ok=True)
                level_gdf.to_file(level_outfile)
                logging.info(f"Created {level_outfile}")

                # Generate admin linework for the specific level
                generate_admin_linework(level_gdf, linework_out_dir, iso_code, source_abbr, realname, level)
        else:
            logging.info(f"No 'admLevel' column found in {filepath}. Skipping processing.")

    except Exception as e:
        logging.error(f"Error processing {filepath}: {e}")


def process_shapefiles_by_filename(filepath, iso_code, data_out_path):
    """Fallback processing when no admLevel column is found. Derives levels from filenames."""

    adm_level_out_dir = os.path.join(data_out_path, "202_admn/admin_level")
    linework_out_dir = os.path.join(data_out_path, "202_admn")

    source_abbr = determine_source_abbr(filepath)
    filename = os.path.basename(filepath).lower()

    level = None
    for key in LEVEL_MAPPING:
        if key in filename:
            level = LEVEL_MAPPING[key]
            break

    if level is not None:
        realname = REALNAME_MAPPING.get(level, "Admin")
        output_dir = adm_level_out_dir
        output_file = f"{iso_code}_admn_ad{level}_py_s1_{source_abbr}_pp_{realname}.shp"

        if level in [86, 87]:
            output_dir = os.path.join(data_out_path, "202_admn")
            output_file = f"{iso_code}_admn_ad0_ln_s0_{source_abbr}_pp_disputedBoundaries.shp"
        elif level == 99:
            output_dir = os.path.join(data_out_path, "211_elev")
            output_file = f"{iso_code}_elev_cst_ln_s0_{source_abbr}_pp_coastline.shp"

        try:
            gdf = gpd.read_file(filepath)
            os.makedirs(output_dir, exist_ok=True)
            gdf.to_file(os.path.join(output_dir, output_file))
            logging.info(f"Created {output_file} based on filename pattern.")

            # Generate admin linework after processing
            generate_admin_linework(gdf, linework_out_dir, iso_code, source_abbr, realname, level)
        except Exception as e:
            logging.error(f"Error processing {filepath}: {e}")
    else:
        logging.warning(f"Could not determine level from filename: {filename}")


def cleanup(data_out_path):
    """Clean up temporary directories after processing."""
    # Only remove temporary or mid-processing directories if they exist
    adm_level_out_dir = os.path.join(data_out_path, "202_admn/admin_level")
    if os.path.exists(adm_level_out_dir):
        shutil.rmtree(adm_level_out_dir)
        logging.info(f"Cleaned up temporary directory: {adm_level_out_dir}")


def determine_source_abbr(filepath):
    """Determine the source abbreviation based on the filepath."""
    filename = os.path.basename(filepath).lower()
    if "zimstat" in filename:
        return "zimstat"
    else:
        return "hdx"
