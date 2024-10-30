import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from osm.utils.osm_utils import unique_column_names

class OSMSchoolDataDownloader:
    osm_key = 'amenity'
    osm_value = 'school'
    additional_tags = [
        "operator", "operator_type", "capacity", "grades",
        "min_age", "max_age", "school:gender", 'name', 'name:en', 
        'name_en','operator_t','operator:t', 'school:gen', 'school_gen', 'osmid'
    ]

    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        ox.config(log_console=True, use_cache=True)
        self.output_filename = f"data/output/country_extractions/{country_code}/210_educ/{country_code}_educ_edu_pt_s3_osm_pp_schools.shp"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf = ox.geometries_from_polygon(geometry, tags={self.osm_key: self.osm_value})
        
        gdf_projected = gdf.to_crs(epsg=self.crs_project)
        gdf_projected['geometry'] = gdf_projected.geometry.centroid
        gdf = gdf_projected.to_crs(epsg=self.crs_global)

        if 'fclass' not in gdf.columns:
            gdf['fclass'] = self.osm_value

        gdf = unique_column_names(gdf)
        actual_tags = gdf.columns.intersection(self.additional_tags)
        
        for tag in self.additional_tags:
            if tag not in actual_tags:
                gdf[tag] = pd.NA

        missing_tags = set(self.additional_tags) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)
        columns_to_keep = ['geometry', 'fclass'] + list(actual_tags)
        gdf = gdf[columns_to_keep]
        gdf = unique_column_names(gdf)
        gdf = gdf.iloc[:, :100]

        if not gdf.empty:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
            print(f"Data successfully saved to {self.output_filename}")
        else:
            print("No data to save.")
