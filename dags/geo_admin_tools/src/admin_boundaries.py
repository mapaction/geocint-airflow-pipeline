import os
import geopandas as gpd
from geo_admin_tools.utils.metadata_utils import update_metadata
from geo_admin_tools.src.admin_linework import generate_admin_linework, find_admlevel_column

def process_admALL_files(mid_dir, iso_code, data_out_path):
    """Process all shapefiles in the mid directory."""
    for root, dirs, files in os.walk(mid_dir):
        for file in files:
            if file.endswith('.shp'):
                file_path = os.path.join(root, file)
                print(f"Processing {file_path} in /mid directory")
                process_shapefiles(file_path, iso_code, data_out_path)

def process_shapefiles(filepath, iso_code, base_download_directory):
    """Process shapefiles and extract relevant administrative boundary levels."""
    
    # Update directory paths to match the new structure
    adm_level_out_dir = os.path.join(base_download_directory, "admin_level")
    linework_out_dir = os.path.join(base_download_directory, "admin_linework")
    disputed_out_dir = os.path.join(base_download_directory, "disputed_boundaries")
    coastline_out_dir = os.path.join(base_download_directory, "national_coastline")

    source_abbr = "hdx"  

    realname_mapping = {
        0: "country",
        1: "parish",  # Changed from "region"
        2: "municipality",
        3: "district",
        4: "locality",
    }


    try:
        gdf = gpd.read_file(filepath)

        adm_column = find_admlevel_column(gdf) # admbndl

        if adm_column:
            print(f"Found {adm_column} column in {filepath}. Available levels: {gdf[adm_column].unique()}")
            for level in gdf[adm_column].unique():
                level_gdf = gdf[gdf[adm_column] == level]
                output_dir = None
                output_file = None

                realname = realname_mapping.get(level, "Admin")

                if level == 0:
                    output_dir = adm_level_out_dir
                    output_file = f"{iso_code}_admn_ad0_py_s1_{source_abbr}_pp_{realname}.shp"
                elif level in [86, 87]:
                    output_dir = disputed_out_dir
                    output_file = f"{iso_code}_admn_ad0_ln_s0_{source_abbr}_pp_disputedBoundaries.shp"
                elif level == 99:
                    output_dir = coastline_out_dir
                    output_file = f"{iso_code}_elev_cst_ln_s0_{source_abbr}_pp_coastline.shp"
                elif level in [1, 2, 3, 4]:
                    output_dir = adm_level_out_dir
                    output_file = f"{iso_code}_admn_ad{level}_py_s1_{source_abbr}_pp_{realname}.shp"
                else:
                    print(f"Level {level} not specifically handled, but found in {filepath}")

                if output_dir and output_file:
                    os.makedirs(output_dir, exist_ok=True)
                    level_outfile = os.path.join(output_dir, output_file)
                    level_gdf.to_file(level_outfile)
                    print(f"Created {level_outfile}")
                    update_metadata(iso_code, level, level_outfile)

            generate_admin_linework(gdf, linework_out_dir, iso_code, source_abbr, realname, adm_column)

        else:
            print(f"No 'admLevel' column found in {filepath}. Skipping processing.")
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
