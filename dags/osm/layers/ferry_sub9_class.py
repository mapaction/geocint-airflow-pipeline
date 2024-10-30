import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from osm.utils.osm_utils import save_data, unique_column_names

class OSMFerryRouteDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags = {'route': 'ferry'}
        ox.config(log_console=True, use_cache=True)
        self.attributes = ['name', 'name:en', 'name_en']
        self.output_filename = f"data/output/country_extractions/{country_code}/232_tran/{country_code}_tran_fer_ln_s2_osm_pp_ferryroute.shp"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]
        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")
        
        gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)
        
        for key in self.osm_tags.values():
            if key not in gdf.columns:
                gdf[key] = pd.NA

        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

        unwanted_fields = {'manmade', 'image', 'ref', 'source', 'layer', 'surface'}
        columns_to_keep = set(['geometry'] + list(actual_tags)) - unwanted_fields
        columns_to_keep = list(columns_to_keep)
        gdf = gdf[columns_to_keep]

        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        gdf = unique_column_names(gdf)

        if not gdf.empty:
            save_data(gdf, self.output_filename)
        else:
            print("No data to save.")