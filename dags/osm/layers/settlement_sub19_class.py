import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from osm.utils.osm_utils import ensure_unique_column_names, save_data

class OSMSettlementsDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        #self.tags = {'place': ['city', 'capital', 'borough', 'town', 'village', 'hamlet']}
        self.tags = {'place': ['city', 'borough', 'town', 'village', 'hamlet'], 'capital': True}
        self.attributes = ['name', 'name:en', 'name_en']
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_filename = f"data/output/country_extractions/{country_code}/229_stle/{country_code}_stle_stl_pt_s3_osm_pp_settlements.shp"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf_settlements = ox.geometries_from_polygon(geometry, tags=self.tags)
        gdf_settlements = self.add_required_fields(gdf_settlements)
        gdf_settlements = self.process_geometries(gdf_settlements)
        gdf_settlements = ensure_unique_column_names(gdf_settlements)
        gdf_settlements = save_data(gdf_settlements)


    def process_geometries(self, gdf):
        gdf = gdf.to_crs(epsg=self.crs_project)
        gdf['geometry'] = gdf.apply(lambda row: row['geometry'].centroid if row['geometry'].geom_type != 'Point' else row['geometry'], axis=1)
        gdf = gdf.to_crs(epsg=self.crs_global)

        # Handle list-type fields
        for col in gdf.columns:
            if pd.api.types.is_object_dtype(gdf[col]) and gdf[col].apply(lambda x: isinstance(x, list)).any():
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        ##
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

         # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry', 'fclass'] + list(actual_tags)   
        gdf = gdf[columns_to_keep]
        ##       
        return gdf

    def add_required_fields(self, gdf):
        # Add 'fclass', 'name', and 'name:en' columns based on the OSM data
        #gdf['fclass'] = gdf['place']
        gdf['fclass'] = gdf.apply(lambda row: 'national_capital' if 'capital' in row and row['capital'] == 'yes' else row['place'], axis=1)
        gdf['name'] = gdf.get('name', pd.NA)
        gdf['name'] = gdf['name'] if 'name' in gdf.columns else pd.NA
        gdf['name:en'] = gdf['name:en'] if 'name:en' in gdf.columns else pd.NA

        return gdf