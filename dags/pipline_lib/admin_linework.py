import os
import json
import geopandas as gpd
import glob
import subprocess

def process_admin_boundaries(country_code: str, input_dir: str, output_dir: str):
    """Processes administrative boundaries for a country, converting polygons to lines, renaming files, and saving in a structured directory."""

    with open('dags/static_data/admin_level_display_names.json') as f:
        admin_level_display_names = json.load(f)

    source = admin_level_display_names.get(country_code, {}).get("source")
    if source == "no cod data":
        print(f"No COD data for country: {country_code}")
        return

    layer_dir = os.path.join(output_dir, '202_admn') 
    os.makedirs(layer_dir, exist_ok=True)  

    for adm_level in range(5):
        display_name = admin_level_display_names.get(country_code, {}).get(f"adm{adm_level}", f"adminboundary{adm_level}")

        shapefiles = [file for file in glob.glob(os.path.join(input_dir, "*.shp")) if f"adm{adm_level}" in file]
        
        for shapefile in shapefiles:
            gdf = gpd.read_file(shapefile)

            if gdf.geom_type[0] not in ['Polygon', 'MultiPolygon']:
                print(f"Skipping {shapefile}: Not a polygon shapefile")
                continue

            gdf_lines = gdf.boundary

            output_file = os.path.join(layer_dir, f"{country_code}_admn_ad{adm_level}_ln_s1_{source}_pp_{display_name}.shp")
            gdf_lines.to_file(output_file)
            print(f"Converted and saved: {output_file}")

            geojson_dest = output_file.replace(".shp", ".geojson")
            print(f"Converting {output_file} to {geojson_dest}")
            subprocess.run(["ogr2ogr", "-f", "GeoJSON", geojson_dest, output_file], check=True)


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python process_admin_boundaries.py <country_code> <input_directory> <output_directory>")
    else:
        country_code = sys.argv[1]
        input_dir = sys.argv[2]
        output_dir = sys.argv[3]

        process_admin_boundaries(country_code, input_dir, output_dir)