import os
import osmnx as ox
import geopandas as gpd
from osm.utils.osm_utils import unique_column_names, save_data

class OSMRailwayStationDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_key = 'railway'
        self.osm_values = ['station', 'halt']
        self.osm_min_tags = {'passenger': 'yes', 'cargo': 'yes'}
        self.attributes = ['name', 'name:en', 'name_en', 'amenity','passenger', 'cargo']
        ox.config(log_console=True, use_cache=True)
        self.output_filename = f"data/output/country_extractions/{country_code}/232_tran/{country_code}_tran_rst_pt_s2_osm_pp_railwaystations.shp"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf = ox.geometries_from_polygon(geometry, tags={self.osm_key: self.osm_values})
        gdf = gdf[gdf[self.osm_key].isin(self.osm_values)]

        # Reproject geometries to the specified projection before calculating centroids
        gdf_projected = gdf.to_crs(epsg=self.crs_project)
        gdf_projected['geometry'] = gdf_projected['geometry'].centroid
        gdf_projected = gdf_projected.to_crs(epsg=self.crs_global)

        if gdf_projected.empty:
            raise ValueError("No features to process after filtering.")

        gdf_projected = self.process_list_fields(gdf_projected)
        gdf_projected = unique_column_names(gdf_projected)

        gdf_projected = save_data(gdf_projected, self.output_filename)
    
    def process_list_fields(self, gdf):
        for col in gdf.columns:
            # Check if any entry in the column is a list
            if any(isinstance(item, list) for item in gdf[col]):
                # Convert lists to strings
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        
        gdf['fclass'] = gdf['railway']
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