import os
import osmnx as ox
import geopandas as gpd
import logging
class OSMLakeDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.output_filename = f"data/output/country_extractions/{country_code}/221_phys/{country_code}_phys_lak_py_s3_osm_pp_lake.shp"
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags = {'water': ['lake', 'reservoir']}
        self.attributes = ['name', 'name:en', 'name_en']
        ox.settings.log_console = True
        ox.settings.use_cache = True 

    def download_and_process_data(self):
        logging.info(f"Starting data download and processing for {self.__class__.__name__}")

        try:
            region_gdf = gpd.read_file(self.geojson_path)
            logging.info(f"Loaded GeoJSON file: {self.geojson_path}")
        except Exception as e:
            logging.error(f"Error loading GeoJSON file: {e}")
            return  # Exit early if GeoJSON loading fails

        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            logging.error(f"Unsupported geometry type: {geometry.geom_type}. Expected Polygon or MultiPolygon.")
            return  # Exit if geometry type is not supported

        logging.info("Downloading OSM data...")
        try:
            gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)
            logging.info(f"Downloaded {len(gdf)} features from OSM.")
        except Exception as e:
            logging.error(f"Error downloading OSM data: {e}")
            return  # Exit if OSM data download fails

        # Filter for polygon geometries
        gdf_polygons = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]

        if self.crs_project:
            logging.info(f"Reprojecting to CRS: {self.crs_project}")
            gdf_polygons = gdf_polygons.to_crs(epsg=self.crs_project)

        logging.info(f"Reprojecting to global CRS: {self.crs_global}")
        gdf_polygons = gdf_polygons.to_crs(epsg=self.crs_global)

        logging.info("Processing list fields...")
        gdf_polygons = self.process_list_fields(gdf_polygons)

        logging.info("Ensuring unique column names...")
        gdf_polygons = self.ensure_unique_column_names(gdf_polygons)

        logging.info("Saving data...")
        self.save_data(gdf_polygons)
        logging.info("Data download and processing completed successfully.")

    def process_list_fields(self, gdf):
        for col in gdf.columns:
            if gdf[col].dtype == object and gdf[col].apply(lambda x: isinstance(x, list)).any():
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        
        gdf['fclass'] = gdf['water']
        ##
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

         # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry','fclass'] + list(actual_tags)   
        gdf = gdf[columns_to_keep]
        ## 

        return gdf

    def ensure_unique_column_names(self, gdf):
        unique_columns = {}
        for col in gdf.columns:
            new_col = col[:10]
            counter = 1
            while new_col in unique_columns.values():
                new_col = f"{col[:9]}{counter}"
                counter += 1
            unique_columns[col] = new_col
        gdf.rename(columns=unique_columns, inplace=True)
        return gdf

    def save_data(self, gdf):
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)
        try:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
        except Exception as e:
            print(f"An error occurred while saving the GeoDataFrame: {e}")