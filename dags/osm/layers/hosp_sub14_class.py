import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from osm.utils.osm_utils import unique_column_names

class OSMHospitalDataDownloader:

    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags_hospital = {'amenity': 'hospital'}
        self.attributes = ['name', 'name:en', 'name_en', 'emergency', 'operator', 'operator:type', 'beds', 'operator_type', 'operator_ty']
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_filename = f"data/output/country_extractions/{country_code}/215_heal/{country_code}_heal_hea_pt_s3_osm_pp_hospital.shp"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf_hospitals = ox.geometries_from_polygon(geometry, tags=self.osm_tags_hospital)
        gdf_hospitals = unique_column_names(gdf_hospitals)
        gdf_hospitals = self.process_geometries(gdf_hospitals)
        gdf_hospitals = self.ensure_required_fields(gdf_hospitals)

        if 'fclass' not in gdf_hospitals.columns:
            gdf_hospitals['fclass'] = 'hospital'

        columns = ['fclass'] + [col for col in gdf_hospitals.columns if col != 'fclass']
        gdf_hospitals = gdf_hospitals[columns]

        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        if not gdf_hospitals.empty:
            gdf_hospitals.to_file(self.output_filename, driver='ESRI Shapefile')
            print(f"Data successfully saved to {self.output_filename}")
        else:
            print("No data to save.")

    def process_geometries(self, gdf):
        gdf = gdf.to_crs(epsg=self.crs_project)
        gdf['geometry'] = gdf.apply(lambda row: row['geometry'].centroid if row['geometry'].geom_type != 'Point' else row['geometry'], axis=1)
        gdf = gdf.to_crs(epsg=self.crs_global)

        for col in gdf.columns:
            if pd.api.types.is_object_dtype(gdf[col]) and gdf[col].apply(lambda x: isinstance(x, list) if x is not None else False).any():
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

        for tag in missing_tags:
            if tag not in gdf.columns:
                gdf[tag] = None

        columns_to_keep = ['geometry'] + [col for col in self.attributes if col in gdf.columns]
        gdf = gdf[columns_to_keep]

        return gdf

    def ensure_required_fields(self, gdf):
        required_fields = ['emergency', 'operator', 'operator_type', 'beds']
        for field in required_fields:
            if field not in gdf.columns:
                gdf[field] = None

        return gdf
