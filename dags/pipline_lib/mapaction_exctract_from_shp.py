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
        # Check if the GeoJSON boundary overlaps with the shapefile features
        ogr2ogr_cmd_check = (
            f"ogr2ogr -clipsrc {country_geojson_path} "
            f"-f GeoJSON /vsistdout/ {input_shp_path}"
        )

        try:
            result_check = subprocess.run(ogr2ogr_cmd_check, shell=True,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            if result_check.returncode != 0:
                logging.error("Error occurred during ogr2ogr checking:")
                logging.error(result_check.stderr)
                return

            if result_check.stdout.strip() == "":
                logging.warning("No features found within the country boundary. Skipping GeoJSON creation.")
                return  # No need to proceed further, since no features intersect.

        except subprocess.CalledProcessError as e:
            logging.error("Error running ogr2ogr for checking:")
            logging.error(e.stderr)
            return  # Exit the function

        # Now that we know there are features, create the GeoJSON and shapefile
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

        # Convert GeoJSON to shapefile
        ogr2ogr_cmd = (
            f"ogr2ogr -lco ENCODING=UTF8 {output_shp_path} {output_geojson_path}"
        )
        subprocess.run(ogr2ogr_cmd, shell=True, check=True)

        # Delete the GeoJSON file
        output_geojson_path.unlink(missing_ok=True)
        return

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
# v2
# import subprocess
# import pathlib
# import logging
# import os

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
#     """Clips a shapefile by a country's GeoJSON boundary."""

#     output_shp_path = pathlib.Path(output_name).with_suffix(".shp")
#     temp_geojson_path = pathlib.Path(output_name).with_suffix(".temp.geojson")

#     output_shp_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory

#     # Use ogr2ogr to attempt to clip to a temporary GeoJSON file
#     ogr2ogr_cmd_check = (
#         f"ogr2ogr -clipsrc {country_geojson_path} "
#         f"-f GeoJSON {temp_geojson_path} {input_shp_path}"
#     )

#     try:
#         result_check = subprocess.run(ogr2ogr_cmd_check, shell=True,
#                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

#         if result_check.returncode != 0:
#             logging.error("Error occurred during ogr2ogr checking:")
#             logging.error(result_check.stderr)
#             return

#         # Check if the resulting GeoJSON file contains any features
#         if os.path.getsize(temp_geojson_path) == 0:
#             logging.warning("No features found within the country boundary. Skipping creation of output files.")
#             temp_geojson_path.unlink(missing_ok=True)  # Remove the temporary file
#             return  # Exit without creating any output, as no features intersect.

#     except subprocess.CalledProcessError as e:
#         logging.error("Error running ogr2ogr for checking:")
#         logging.error(e.stderr)
#         return  # Exit the function if there's an error.

#     # Check if the temporary GeoJSON file is empty (0 bytes)
#     if temp_geojson_path.stat().st_size == 0:
#         logging.info(f"No intersecting features found. No shapefile will be created for {output_name}.")
#         temp_geojson_path.unlink(missing_ok=True)  # Delete the temp file
#         return

#     # If we reached this point, it means there are intersecting features.
#     ogr2ogr_cmd = (
#         f"ogr2ogr -clipsrc {country_geojson_path} "
#         f"-f 'ESRI Shapefile' {output_shp_path} {input_shp_path}"
#     )

#     try:
#         result = subprocess.run(ogr2ogr_cmd, shell=True,
#                                 stderr=subprocess.PIPE, text=True)

#         if result.returncode != 0:
#             logging.error("Error occurred during ogr2ogr clipping:")
#             logging.error(result.stderr)
#             return

#     except subprocess.CalledProcessError as e:
#         logging.error("Error running ogr2ogr:")
#         logging.error(e.stderr)
#         return

#     # Remove the temporary GeoJSON file
#     temp_geojson_path.unlink(missing_ok=True)
#     logging.info(f"Shapefile created successfully: {output_shp_path}")

# if __name__ == "__main__":
#     import sys
#     if len(sys.argv) != 4:
#         print("Usage: python clip_shapefile.py <country_geojson> <input_shp> <output_name>")
#     else:
#         country_geojson_path = sys.argv[1]
#         input_shp_path = sys.argv[2]
#         output_name = sys.argv[3]

#         clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)


