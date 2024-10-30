import os
import osmnx as ox
import geopandas as gpd
from osm.utils.osm_utils import unique_column_names

class OSMPortDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags = {'landuse': 'harbour', 'harbour': 'port'}
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.attributes = ['name', 'name:en', 'name_en']
        self.output_filename = f"data/output/country_extractions/{country_code}/232_tran/{country_code}_tran_por_pt_s0_osm_pp_port.shp"
        

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        gdf = ox.geometries_from_polygon(geometry, tags=self.osm_tags)
        gdf = gdf.to_crs(epsg=self.crs_project)
        gdf['geometry'] = gdf.geometry.centroid
        gdf = gdf.to_crs(epsg=self.crs_global)

        
        list_type_cols = [col for col, dtype in gdf.dtypes.items() if dtype == object]
        for col in list_type_cols:
            gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)


        gdf['fclass'] = gdf['landuse']
        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")
        
        collumns_to_keep = ['geometry', 'fclass'] + list(actual_tags) #+ list(self.osm_tags)
        gdf = gdf[collumns_to_keep]

        
        gdf = unique_column_names(gdf)


        # Save the data to a shapefile
        try:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
            print(f"Data successfully saved to {self.output_filename}")
        except Exception as e:
            print(f"An error occurred while saving the GeoDataFrame: {e}")