import os
import osmnx as ox
import geopandas as gpd
import pandas as pd

class OSMPOIDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code, docker_worker_working_dir):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.input_pt_csv = f"{docker_worker_working_dir}/dags/static_data/pois/pois_pt.csv"
        self.input_py_csv = f"{docker_worker_working_dir}/dags/static_data/pois/pois_py.csv"
        self.osm_tags_pois_point = self.load_pois_tags(self.input_pt_csv)
        self.osm_tags_pois_polygon = self.load_pois_tags(self.input_py_csv)
        self.attributes = ['name', 'geometry']
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_point_filename = f"data/output/country_extractions/{country_code}/222_pois/{country_code}_pois_pt_s3_osm_pp_pois.shp"
        self.output_polygon_filename = f"data/output/country_extractions/{country_code}/222_pois/{country_code}_pois_py_s3_osm_pp_pois.shp"

    def load_pois_tags(self, csv_file):
        """
        Load the POI types (fclass) from the given CSV file.
        """
        return pd.read_csv(csv_file)['fclass'].tolist()

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")
        
        osm_tags = {'amenity': self.osm_tags_pois_point}
        
        # Fetch and process points
        gdf_points = ox.features_from_polygon(geometry, tags=osm_tags)
        gdf_points = self.process_geometries(gdf_points, 'point')
        gdf_points = self.ensure_unique_column_names(gdf_points)
        self.save_data(gdf_points, self.output_point_filename)
        
        # Update tags for polygons and fetch polygons
        osm_tags = {'amenity': self.osm_tags_pois_polygon}
        gdf_polygons = ox.features_from_polygon(geometry, tags=osm_tags)
        
        # Filter for polygons specifically
        gdf_polygons = gdf_polygons[gdf_polygons.geometry.type.isin(['Polygon', 'MultiPolygon'])]
        gdf_polygons = self.process_geometries(gdf_polygons, 'polygon')
        gdf_polygons = self.ensure_unique_column_names(gdf_polygons)
        self.save_data(gdf_polygons, self.output_polygon_filename)

    def process_geometries(self, gdf, geometry_type):
        gdf = gdf.to_crs(epsg=self.crs_project)

        if geometry_type == 'point':
            gdf['geometry'] = gdf.apply(lambda row: row['geometry'].centroid if row['geometry'].geom_type != 'Point' else row['geometry'], axis=1)
        gdf = gdf.to_crs(epsg=self.crs_global)

        
        if 'amenity' in gdf.columns:
            gdf['fclass'] = gdf['amenity']
        else:
            gdf['fclass'] = 'point_of_interest' if geometry_type == 'point' else 'polygon_of_interest'

        columns_to_keep = ['geometry', 'fclass', 'name']  
        available_columns = gdf.columns.intersection(columns_to_keep)
        return gdf[available_columns]

    def ensure_unique_column_names(self, gdf):
        new_columns = {}
        for col in gdf.columns:
            new_col = col[:10]  
            counter = 1
            while new_col in new_columns.values():
                new_col = f"{col[:9]}{counter}"  
                counter += 1
            new_columns[col] = new_col
        gdf.rename(columns=new_columns, inplace=True)
        return gdf

    def save_data(self, gdf, output_filename):
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        try:
            gdf.to_file(output_filename, driver='ESRI Shapefile')
        except Exception as e:
            print(f"An error occurred while saving the GeoDataFrame: {e}")
