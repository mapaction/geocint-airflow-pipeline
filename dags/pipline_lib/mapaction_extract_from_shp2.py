import geopandas as gpd
import pathlib
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
    output_shp_path = pathlib.Path(output_name).with_suffix(".shp")

    logging.info("Reading country boundary GeoJSON...")
    country_gdf = gpd.read_file(country_geojson_path)

    logging.info("Reading input shapefile...")
    input_gdf = gpd.read_file(input_shp_path)

    if input_gdf.crs != country_gdf.crs:
        logging.info(f"Reprojecting polygons to match CRS of country boundary: {country_gdf.crs}")
        input_gdf = input_gdf.to_crs(country_gdf.crs)

    logging.info("Filtering polygons within the country boundary...")
    clipped_gdf = input_gdf[input_gdf.geometry.within(country_gdf.unary_union)]

    if clipped_gdf.empty:
        logging.warning(f"No polygons found within the country boundary for {output_name}. No shapefile will be created.")
        return

    logging.info(f"Saving the resulting polygon shapefile to {output_shp_path}...")
    clipped_gdf.to_file(output_shp_path, driver='ESRI Shapefile')
    logging.info(f"Polygon shapefile created successfully: {output_shp_path}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python clip_centroids.py <country_geojson> <input_shp> <output_name>")
    else:
        country_geojson_path = sys.argv[1]
        input_shp_path = sys.argv[2]
        output_name = sys.argv[3]

        clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)