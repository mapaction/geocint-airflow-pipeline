import geopandas as gpd
import pathlib
import logging
from shapely.geometry import box

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
    output_shp_path = pathlib.Path(output_name).with_suffix(".shp")

    logging.info("Reading country boundary GeoJSON...")
    country_gdf = gpd.read_file(country_geojson_path)

    logging.info("Reading input shapefile (lines)...")
    input_gdf = gpd.read_file(input_shp_path)

    logging.info("Creating spatial index for input shapefile...")
    input_gdf.sindex  

    
    if input_gdf.crs != country_gdf.crs:
        logging.info(f"Reprojecting lines to match CRS of country boundary: {country_gdf.crs}")
        input_gdf = input_gdf.to_crs(country_gdf.crs)

   
    logging.info("Pre-filtering HydroRIVERS dataset using the bounding box of the country...")
    bbox = country_gdf.total_bounds  # Get bounding box of the country
    input_gdf = input_gdf[input_gdf.geometry.intersects(box(*bbox))]  

    logging.info(f"Number of features after pre-filtering by bounding box: {len(input_gdf)}")

    logging.info("Clipping line features based on the country boundary...")
    clipped_gdf = gpd.overlay(input_gdf, country_gdf, how='intersection')

    if clipped_gdf.empty:
        logging.warning(f"No line features found within the country boundary for {output_name}. No shapefile will be created.")
        return

    logging.info(f"Saving the resulting line shapefile to {output_shp_path}...")
    clipped_gdf.to_file(output_shp_path, driver='ESRI Shapefile')
    logging.info(f"Line shapefile created successfully: {output_shp_path}")