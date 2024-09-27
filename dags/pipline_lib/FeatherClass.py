import os
import sys
import geopandas as gpd
from pathlib import Path

class FeatherCreator:
    GLOBAL_BACKGROUND_PATH = '/opt/airflow/dags/static_data/downloaded_data/global_Background_shp/wrl_carto_ext_py_s0_esri_pp_globalBackground.shp'
    def __init__(self, data_out_directory):
        self.data_out_directory = data_out_directory
        self.admin_dir = os.path.join(self.data_out_directory, '202_admn')
        

    def create_feathers(self):
        if not os.path.isdir(self.admin_dir):
            print(f"Admin directory does not exist: {self.admin_dir}")
            return

        try:
            for filename in os.listdir(self.admin_dir):
                if filename.endswith('.shp') and 'ad0' in filename:
                    admin_shapefile_path = os.path.join(self.admin_dir, filename)
                    self.create_feather(admin_shapefile_path)
            print("Feather creation complete for all valid shapefiles.")
        except Exception as e:
            print(f"An error occurred: {e}")
            sys.exit(1)

    def create_feather(self, admin_shapefile_path):
        try:
            local_admin = gpd.read_file(admin_shapefile_path)
            global_background = gpd.read_file(self.GLOBAL_BACKGROUND_PATH)
            
            if local_admin.crs != global_background.crs:
                local_admin = local_admin.to_crs(global_background.crs)
            
            feather = gpd.overlay(global_background, local_admin, how='symmetric_difference')
            country_code = Path(admin_shapefile_path).stem.split("_")[0]
            fea_text = Path(admin_shapefile_path).stem.split("_")[7]
            feather_name = f'{country_code}_carto_fea_py_s0_mapaction_pp_feather.shp'
            # Change the output directory for the feather files
            feather_path = os.path.join(self.data_out_directory, '207_carto')
            os.makedirs(feather_path, exist_ok=True)
            feather_output_path = os.path.join(feather_path, feather_name)
            
            feather.to_file(feather_output_path)
            print(f"Feather created and saved as {feather_output_path}")
        except Exception as e:
            print(f"Error creating feather for {admin_shapefile_path}: {e}")