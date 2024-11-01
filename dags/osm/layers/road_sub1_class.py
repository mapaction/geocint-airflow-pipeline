import osmnx as ox
import geopandas as gpd
import pandas as pd
from pathlib import Path
from osm.utils.osm_utils import unique_column_names

class OSMRoadDataDownloader:
    osm_road_values = "motorway,trunk,primary,secondary,tertiary,unclassified,residential,motorway_link,trunk_link,primary_link,secondary_link,tertiary_link,lining_street,service,track,road"
    osm_required_tags = ['name', 'oneway', 'maxspeed', 'bridge', 'tunnel', 'surface']

    def __init__(self, geojson_path, country_code):
        self.geojson_path = geojson_path
        self.country_code = country_code
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_dir = f"data/output/country_extractions/{country_code}/232_tran/"
        self.output_filename = f"{country_code}_tran_rds_ln_s0_osm_pp_roads.shp"
    

    def download_and_process_data(self):
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        gdf = gpd.read_file(self.geojson_path)
        geometry_type = gdf['geometry'].iloc[0].geom_type
        if geometry_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")
        
        polygon = gdf['geometry'].iloc[0]
        graph = ox.graph_from_polygon(polygon, network_type='drive')
        _, gdf_edges = ox.graph_to_gdfs(graph)
        
    
        all_roads_gdf = gpd.GeoDataFrame()

        for road_type in self.osm_road_values.split(','):
            gdf_filtered = gdf_edges[gdf_edges['highway'].apply(lambda x: road_type in x if isinstance(x, list) else road_type == x)]

            for tag in self.osm_required_tags:
                if tag not in gdf_filtered.columns:
                    gdf_filtered[tag] = pd.NA

     
            gdf_filtered['fclass'] = road_type
            list_type_cols = gdf_filtered.columns[gdf_filtered.dtypes == 'object']
            for col in list_type_cols:
                gdf_filtered[col] = gdf_filtered[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
            
           
            all_roads_gdf = pd.concat([all_roads_gdf, gdf_filtered], ignore_index=True)

        all_roads_gdf = unique_column_names(all_roads_gdf)

       
        columns_to_keep = ['geometry','osmid' ,'fclass'] + self.osm_required_tags
        all_roads_gdf = all_roads_gdf[columns_to_keep]

        if not all_roads_gdf.empty:
            output_path = Path(self.output_dir) / self.output_filename
            all_roads_gdf.to_file(output_path, driver='ESRI Shapefile')
            print(f"Data saved successfully to {output_path}")
        else:
            print("No data to save.")
