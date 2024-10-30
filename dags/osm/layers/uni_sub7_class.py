import os
import osmnx as ox
import geopandas as gpd
from osm.utils.osm_utils import unique_column_names

class OSMEducationDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags = {'amenity': ['university', 'college']}
        self.attributes = ['name', 'name:en', 'name_en']  # Handle multiple values for the 'amenity' key
        ox.config(log_console=True, use_cache=True)
        self.output_filename = f"data/output/country_extractions/{country_code}/210_educ/{country_code}_educ_edu_pt_s3_osm_pp_university.shp"

    def download_and_process_data(self):
        # Load the AOI from the GeoJSON file
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        # Check if the geometry is a Polygon or MultiPolygon
        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        # Download data from OSM based on the provided tags and the geometry of the AOI
        gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)

        # Convert to the projected CRS to calculate centroids
        gdf_projected = gdf.to_crs(epsg=self.crs_project)
        gdf_projected['geometry'] = gdf_projected['geometry'].centroid
        
        # Convert back to the global CRS
        gdf = gdf_projected.to_crs(epsg=self.crs_global)

        # Add 'fclass' column based on the 'amenity' tag
        gdf['fclass'] = gdf['amenity']

        # Handle list-type fields before saving
        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        # Make directories if they don't exist
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)
        
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

         # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry', 'fclass'] + list(actual_tags)   
        gdf = gdf[columns_to_keep]

        gdf = unique_column_names(gdf)  

        if not gdf.empty:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
            print(f"Data successfully saved to {self.output_filename}")
        else:
            print("No data to save.")