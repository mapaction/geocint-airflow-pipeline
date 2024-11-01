import geopandas as gpd
import pathlib
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clip_shapefile_by_country_util(country_geojson_path, input_shp_path, output_name, geometry_type="polygon"):
    output_shp_path = pathlib.Path(output_name).with_suffix(".shp")
    
    logging.info("Reading country boundary GeoJSON...")
    country_gdf = gpd.read_file(country_geojson_path)

    logging.info("Reading input shapefile...")
    input_gdf = gpd.read_file(input_shp_path)

    if input_gdf.crs != country_gdf.crs:
        logging.info(f"Reprojecting input shapefile to match CRS of country boundary: {country_gdf.crs}")
        input_gdf = input_gdf.to_crs(country_gdf.crs)

    if geometry_type == "polygon":
        logging.info("Clipping polygons within the country boundary...")
        clipped_gdf = input_gdf[input_gdf.geometry.within(country_gdf.unary_union)]
    elif geometry_type == "railway":
        logging.info("Clipping railway lines within the country boundary...")
        clipped_gdf = gpd.overlay(input_gdf, country_gdf, how='intersection')
    elif geometry_type == "point":
        logging.info("Clipping points within the country boundary...")
        clipped_gdf = input_gdf[input_gdf.geometry.within(country_gdf.unary_union)]
    else:
        logging.error(f"Unsupported geometry type: {geometry_type}. Please use 'polygon', 'railway', or 'point'.")
        return

    if clipped_gdf.empty:
        logging.warning(f"No features found within the country boundary for {output_name}. No shapefile will be created.")
        return

    logging.info(f"Saving the resulting shapefile to {output_shp_path}...")
    clipped_gdf.to_file(output_shp_path, driver='ESRI Shapefile')
    logging.info(f"Shapefile created successfully: {output_shp_path}")