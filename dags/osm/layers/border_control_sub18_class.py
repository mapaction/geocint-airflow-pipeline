import osmnx as ox
import geopandas as gpd
import pandas as pd
from osm.utils.osm_utils import ensure_unique_column_names, save_data

class OSMBorderControlDataDownloader:
    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_filename = f"data/output/country_extractions/{country_code}/222_pois/{country_code}_pois_bor_pt_s3_osm_pp_bordercrossing.shp"

    def download_and_process_data(self):
      
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

      
        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")


        gdf_border_control = ox.geometries_from_polygon(geometry, tags={'border': 'border_control'})

     
        gdf_border_control = self.process_geometries(gdf_border_control)


        gdf_border_control = ensure_unique_column_names(gdf_border_control)

 
        gdf_border_control = save_data(gdf_border_control, self.output_filename)

    def process_geometries(self, gdf):
        
        gdf = gdf.to_crs(epsg=self.crs_project)
        gdf['geometry'] = gdf.apply(lambda row: row['geometry'].centroid if row['geometry'].geom_type != 'Point' else row['geometry'], axis=1)
        gdf = gdf.to_crs(epsg=self.crs_global)

    
        for col in gdf.columns:
            if pd.api.types.is_object_dtype(gdf[col]) and gdf[col].apply(lambda x: isinstance(x, list)).any():
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        return gdf