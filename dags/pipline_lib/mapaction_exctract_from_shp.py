import subprocess
import pathlib
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
    """Clips a shapefile by a country's GeoJSON boundary."""

    output_shp_path = pathlib.Path(output_name).with_suffix(".shp")
    output_geojson_path = output_shp_path.with_suffix(".geojson")

    output_shp_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory

    # Extract spatial extent from GeoJSON
    ogrinfo_cmd = f"ogrinfo -so -al {country_geojson_path}"
    ogrinfo_output = subprocess.check_output(ogrinfo_cmd, shell=True, text=True)
    spatial_extent_match = re.search(r"Extent:\s*\((.*?)\)", ogrinfo_output)

    if spatial_extent_match:
        # not used as it gives unexpected results
        # spatial_extent = spatial_extent_match.group(1).replace(",", "")

        # Clip using GeoJSON boundary directly
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
                return  # Exit the function

        except subprocess.CalledProcessError as e:
            logging.error("Error running ogr2ogr:")
            logging.error(e.stderr)
            return  # Exit the function

        # Check if the output GeoJSON has features
        if output_geojson_path.stat().st_size > 0:  
            logging.info("Outputting features...")

            # Convert GeoJSON to shapefile
            ogr2ogr_cmd = (
                f"ogr2ogr -lco ENCODING=UTF8 {output_shp_path} {output_geojson_path}"
            )
            subprocess.run(ogr2ogr_cmd, shell=True, check=True)
        else:
            logging.warning("No features found within the country boundary.")
            output_geojson_path.unlink()  

    else:
        logging.error("Error: Could not determine spatial extent from GeoJSON")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python clip_shapefile.py <country_geojson> <input_shp> <output_name>")
    else:
        country_geojson_path = sys.argv[1]
        input_shp_path = sys.argv[2]
        output_name = sys.argv[3]

        clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)
