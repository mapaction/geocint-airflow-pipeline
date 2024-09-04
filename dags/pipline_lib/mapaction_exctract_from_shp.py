import subprocess
import pathlib
import logging
import sys
import geopandas as gpd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_intersecting_features(country_geojson_path, input_shp_path):
    try:
        country_gdf = gpd.read_file(country_geojson_path)
        input_gdf = gpd.read_file(input_shp_path)
        input_gdf = input_gdf.to_crs(country_gdf.crs)
        intersecting_gdf = gpd.sjoin(input_gdf, country_gdf, op='intersects')
        return not intersecting_gdf.empty

    except Exception as e:
        logging.error(f"Error during spatial intersection check: {e}")
        return False

def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
    if not check_intersecting_features(country_geojson_path, input_shp_path):
        logging.info("No intersecting features found within the country boundary. Skipping output generation.")
        return

    output_shp_path = pathlib.Path(output_name).with_suffix(".shp")
    output_geojson_path = output_shp_path.with_suffix(".geojson")

    output_shp_path.parent.mkdir(parents=True, exist_ok=True)

    ogr2ogr_cmd = (
        f"ogr2ogr -clipsrc {country_geojson_path} "
        f"-f GeoJSON {output_geojson_path} {input_shp_path}"
    )

    try:
        result = subprocess.run(ogr2ogr_cmd, shell=True,
                                stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            logging.error("Error occurred during ogr2ogr clipping:")
            logging.error(result.stderr)
            output_geojson_path.unlink(missing_ok=True)
            return

    except subprocess.CalledProcessError as e:
        logging.error("Error running ogr2ogr:")
        logging.error(e.stderr)
        return

    ogr2ogr_cmd = (
        f"ogr2ogr -lco ENCODING=UTF8 {output_shp_path} {output_geojson_path}"
    )
    subprocess.run(ogr2ogr_cmd, shell=True, check=True)

    output_geojson_path.unlink(missing_ok=True)
    logging.info("Successfully created shapefile with intersecting features.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python clip_shapefile.py <country_geojson> <input_shp> <output_name>")
    else:
        country_geojson_path = sys.argv[1]
        input_shp_path = sys.argv[2]
        output_name = sys.argv[3]

        clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)
