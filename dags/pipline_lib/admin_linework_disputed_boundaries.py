import geopandas as gpd
import os
from shapely.geometry import LineString
import json
import fiona 

def is_polygon_shapefile(shp_filepath):
    """Checks if a shapefile contains Polygon or MultiPolygon geometries.

    Args:
        shp_filepath: The path to the shapefile.

    Returns:
        True if it contains Polygons or MultiPolygons, False otherwise.
    """
    with fiona.open(shp_filepath, 'r') as src:
        geom_type = src.schema['geometry']
        if geom_type in ['Polygon', 'MultiPolygon']:
            if geom_type == 'Polygon':
                print("POLYGON SHAPEFILE")
            elif geom_type == 'MultiPolygon':
                print("MULTIPOLYGON SHAPEFILE")
            return True
        else:
            print(f"Unsupported geometry type in shapefile: {geom_type}. Skipping...")
            return False


def process_cod_boundaries(country_iso3, admin_level_field='admLevel', input_dir='input_boundaries',
                           output_dir='output_boundaries', display_names_file='admin_level_display_names.json'):
    """Processes COD sub-national boundaries from a local directory of shapefiles.

    Args:
        country_iso3: ISO3 code of the country (e.g., "AFG" for Afghanistan).
        admin_level_field: Name of the field in the data containing admin levels (default: 'ADM_LEVEL').
        input_dir: Directory where the input shapefiles are stored.
        output_dir: Directory where the output files will be saved.
        display_names_file: Path to the JSON file containing display names for admin levels.

    Returns:
        List of paths to the saved files.
    """

    # Load display names from JSON file
    with open(display_names_file, 'r') as f:
        display_names = json.load(f)

    country = display_names.get(country_iso3)

    if country is None:
        print(f"Warning: No display names found for {country_iso3}. Using default naming.")
        country = {"source": "unknown"}

    shp_filepath = None
    # check if the directory Shapefiles is present and change the input dir
    for file in os.listdir(input_dir):
        if "Shapefiles" in file:
            input_dir = os.listdir(os.path.join(input_dir, 'Shapefiles'))
    
    for file in os.listdir(input_dir):
        if "admALL" in file and 'admbndl' in file and file.endswith('.shp'):  # Check for both .shp extension and admALL
            shp_filepath = os.path.join(input_dir, file)
            print(shp_filepath)
            break

    if shp_filepath is None:
        raise ValueError(f"No shapefile containing 'admALL' found for country '{country_iso3}' in directory '{input_dir}'.")

    # Read the shapefile into a GeoDataFrame
    gdf = gpd.read_file(shp_filepath)

    # Check if downloaded data is polygon and do the conversion if needed
    if is_polygon_shapefile(shp_filepath):  # Use is_polygon_shapefile() here and pass the file path
        gdf = gdf.boundary  # Convert polygons to lines
        # Remove coincident lines using buffer/intersection
        buffered_gdf = gdf.buffer(0.00001)  
        intersections = buffered_gdf.unary_union
        merged_lines = gpd.GeoSeries([geom for geom in intersections.geoms if isinstance(geom, LineString)])
        gdf = gpd.GeoDataFrame(geometry=merged_lines)

    os.makedirs(output_dir, exist_ok=True)

    # Filter and Rename
    # Combine specific levels with admin levels 0-4 for filtering
    specific_levels = [99] + list(range(80, 90))
    admin_levels = [0, 1, 2, 3, 4]
    all_levels = specific_levels + admin_levels
    filtered_gdf = gdf[gdf[admin_level_field].isin(all_levels)]
    filtered_gdf[admin_level_field] = filtered_gdf[admin_level_field].astype(str)
    
    # Print output if there are features found
    if not filtered_gdf.empty:
        print(f"all_levels >>>>> {all_levels}")
        print(f"Found lines with admin levels {specific_levels} in admALL file:")
        print(filtered_gdf[[admin_level_field, 'geometry']])
    else:
        print(f"No lines found with admin levels {specific_levels} in admALL file.")

    # 5. Split, Rename, and Save (Modified for JSON-based naming)
    saved_file_paths = []
    for adm_level, group in filtered_gdf.groupby(admin_level_field):  # Use filtered_gdf here
        if adm_level == "99":
            level_name = "coastline"
            layer_name = "211_elev"
            filename = f"{country_iso3}_elev_cst_ln_s0_{country.get('source', 'unknown')}_pp_{level_name}.geojson"
            file_path = os.path.join(layer_name, filename)
        elif adm_level.startswith("8"):
            layer_name = "202_admn"
            filename = f"{country_iso3}_admn_ad{adm_level}_disputed_boundaries.geojson"
            file_path = os.path.join(layer_name, filename)
        elif adm_level in ["0", "1", "2", "3", "4"]:
            level_name = country.get(f"adm{adm_level}", f"adm{adm_level}")  # Default to admX if not found in JSON
            layer_name = "202_admn"
            filename = f"{country_iso3}_admn_ad{adm_level}_ln_s1_{country.get('source', 'unknown')}_pp_{level_name}.geojson"
            file_path = os.path.join(layer_name, filename)
        else:
            filename = f"{country_iso3}_{adm_level.replace(' ', '_')}.geojson" 

        filepath = os.path.join(output_dir, file_path)
        group.to_file(filepath, driver='GeoJSON')
        saved_file_paths.append(filepath)

    return saved_file_paths


# # Example Usage
# country_code = "pak"  
# input_directory = "my_boundary_files/cod_pak"  # Replace with the actual directory path
# output_directory = f"my_boundary_files/{country_code}"
# display_names_file = "data/admin_level_display_names.json"
# file_paths = process_cod_boundaries(country_code, input_dir=input_directory, output_dir=output_directory, display_names_file=display_names_file)
# if file_paths:  
#     print("Saved files to:")
#     for path in file_paths:
#         print(path)
# else:
#     print(f"Failed to process boundaries for {country_code}")
