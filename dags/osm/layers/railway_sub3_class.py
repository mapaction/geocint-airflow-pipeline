import os
import osmnx as ox
import geopandas as gpd
import pandas as pd
from pathlib import Path
from osm.utils.osm_utils import unique_column_names

class OSMRailwayDataDownloader:
    railway_tags = {
        'railway': ['rail', 'narrow_gauge', 'subway'],
        '!railway': ['miniature']
    }

    def __init__(self, geojson_path, country_code):
        self.geojson_path = geojson_path
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_dir = f"data/output/country_extractions/{country_code}/232_tran/"
        self.output_filename = f"{country_code}_tran_rst_pt_s2_osm_pp_railwayline.shp"
    
    def download_and_process_data(self):

        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        region_gdf = gpd.read_file(self.geojson_path)
        geometry_type = region_gdf['geometry'].iloc[0].geom_type
        if geometry_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        polygon = region_gdf['geometry'].iloc[0]
        gdf = ox.geometries_from_polygon(polygon, tags=self.railway_tags)
        

        gdf = gdf[~gdf['railway'].isin(self.railway_tags['!railway'])]
        gdf = gdf[gdf['railway'].isin(self.railway_tags['railway'])]
        

        gdf = gdf[gdf['geometry'].type.isin(['LineString', 'MultiLineString'])]
        

        gdf['fclass'] = gdf['railway']

        for rail_type in ['rail', 'narrow_gauge', 'subway']:
            gdf[rail_type] = gdf['railway'].apply(lambda x: 1 if rail_type in x else 0)


        for col in gdf.columns:
            if isinstance(gdf[col].iloc[0], list):
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        # Ensure unique column names for Shapefile format
        #gdf = self.ensure_unique_column_names(gdf)

        # # Keep only the columns necessary for the final output
        # required_columns = ['geometry', 'fclass', 'rail', 'gauge', 'subway']
        # gdf = gdf[required_columns]

 
        actual_tags = gdf.columns.intersection(['name','name_en','name:en', 'gauge',])
        missing_tags = set(['name','name_en','name:en','gauge']) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")
        
        # Keep only the geometry, fclass, and the actual present tags
        columns_to_keep = ['geometry', 'fclass'] + list(actual_tags)
        gdf = gdf[columns_to_keep]

        gdf = unique_column_names(gdf)

        if not gdf.empty:
            output_path = Path(self.output_dir) / self.output_filename
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            try:
                gdf.to_file(filename=output_path, driver='ESRI Shapefile')
                print(f"GeoDataFrame saved successfully to {output_path}")
            except Exception as e:
                print(f"Failed to save GeoDataFrame: {e}")
        else:
            print("No data to save.")

