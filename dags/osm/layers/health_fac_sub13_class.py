import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from osm.utils.osm_utils import ensure_unique_column_names, save_data

class OSMHealthDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags_health = {
            'amenity': ['clinic', 'doctors', 'hospital', 'pharmacy', 'health_post']
        }
        self.attributes = ['name', 'name:en', 'name_en']
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_filename = f"data/output/country_extractions/{country_code}/215_heal/{country_code}_heal_hea_pt_s3_osm_pp_healthfacilities.shp"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf_health = ox.geometries_from_polygon(geometry, tags=self.osm_tags_health)
        gdf_health = self.process_geometries(gdf_health)
        gdf_health = ensure_unique_column_names(gdf_health)
        gdf_health = save_data(gdf_health, self.output_filename)

    def process_geometries(self, gdf):
        gdf = gdf.to_crs(epsg=self.crs_project)
        gdf['geometry'] = gdf.apply(lambda row: row['geometry'].centroid if row['geometry'].geom_type != 'Point' else row['geometry'], axis=1)
        gdf = gdf.to_crs(epsg=self.crs_global)

        if 'amenity' in gdf.columns:
            gdf['fclass'] = gdf['amenity']
        else:
            gdf['fclass'] = 'health_facility'

        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object and col not in ['fclass', 'name']]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

        columns_to_keep = ['geometry','fclass'] + list(actual_tags)
        gdf = gdf[columns_to_keep]

        return gdf