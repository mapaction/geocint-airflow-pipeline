import subprocess
import pathlib
import re

def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
    """Clips a shapefile by a country's GeoJSON boundary."""

    output_shp_path = f"{output_name}.shp"
    output_geojson_path = f"{output_name}.geojson"

    # Create the output directory if it doesn't exist
    pathlib.Path(output_shp_path).parent.mkdir(parents=True, exist_ok=True)

    # Get spatial extent from the GeoJSON 
    ogrinfo_cmd = f"ogrinfo -so -al {country_geojson_path}"
    ogrinfo_output = subprocess.check_output(ogrinfo_cmd, shell=True, text=True)
    spatial_extent_match = re.search(r"Extent:\s*\((.*?)\)", ogrinfo_output)

    if spatial_extent_match:
        spatial_extent = spatial_extent_match.group(1).replace(",", "")

        # Clip the shapefile using the spatial extent and GeoJSON
        ogr2ogr_cmd = (
            f"ogr2ogr -spat {spatial_extent} -clipsrc {country_geojson_path} "
            f"-skipfailures {output_geojson_path} {input_shp_path}"
        )
        subprocess.run(ogr2ogr_cmd, shell=True, check=True)

        # Check if any features were clipped
        ogrinfo_cmd = f"ogrinfo -so -al {output_geojson_path}"
        ogrinfo_output = subprocess.check_output(ogrinfo_cmd, shell=True, text=True)
        feature_count_match = re.search(r"Feature Count:\s*(\d+)", ogrinfo_output)
        feature_count = int(feature_count_match.group(1)) if feature_count_match else 0

        if feature_count > 0:
            print("Outputting features")

            # Convert GeoJSON to shapefile
            ogr2ogr_cmd = (
                f"ogr2ogr -lco ENCODING=UTF8 -skipfailures {output_shp_path} {output_geojson_path}"
            )
            subprocess.run(ogr2ogr_cmd, shell=True, check=True)
        else:
            print("No features, exiting and cleaning up")
            pathlib.Path(output_geojson_path).unlink()  # Delete the empty GeoJSON

    else:
        print("Error: Could not determine spatial extent from GeoJSON")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python clip_shapefile.py <country_geojson> <input_shp> <output_name>")
    else:
        country_geojson_path = sys.argv[1]
        input_shp_path = sys.argv[2]
        output_name = sys.argv[3]

        clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)