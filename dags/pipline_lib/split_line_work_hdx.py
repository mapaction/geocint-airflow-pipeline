import shapefile
from pathlib import Path

# Input parameters
country_code = "your_country_code"
admn_lvl = 4

# Paths
input_dir = Path("data/input") / country_code / "ocha_admin_boundaries"
output_dir = Path("data/output/country_extractions") / country_code
output_dir.mkdir(parents=True, exist_ok=True)

try:
    # Find the correct shapefile (with 'admbndp' and 'admALL' fields)
    shp_file = None
    for file in input_dir.glob("*.shp"):
        sf = shapefile.Reader(file)
        fields = sf.fields[1:] 
        if any(f[0] == 'admbndp' for f in fields) and any(f[0] == 'admALL' for f in fields):
            shp_file = file
            break 

    if shp_file is None:
        raise ValueError("No shapefile found with 'admbndp' and 'admALL' fields")

    # Read the shapefile
    sf = shapefile.Reader(shp_file)
    shapes = sf.shapes()
    records = sf.records()
    fields = sf.fields[1:]

    # Find field indices (same as before)
    admbndp_index = next((i for i, f in enumerate(fields) if f[0] == 'admbndp'), None)
    admALL_index = next((i for i, f in enumerate(fields) if f[0] == 'admALL'), None)

    # Categorize features
    admin_features = []
    coastline_features = []
    disputed_features = []
    for shape, record in zip(shapes, records):
        admin_level = record[admALL_index]
        is_boundary = record[admbndp_index] == 1
        if is_boundary:
            if admin_level <= admn_lvl:
                admin_features.append((shape, record))
            elif admin_level == 99:
                coastline_features.append((shape, record))
            elif admin_level > admn_lvl:
                disputed_features.append((shape, record))

    # Write shapefiles
    def write_shapefile(features, output_filename):
        w = shapefile.Writer(output_dir / output_filename)
        w.fields = fields
        for shape, record in features:
            w.shape(shape)
            w.record(*record)
        w.close()

    for admin_level in range(admn_lvl + 1):
        output_filename = f"{country_code}_admn_ad{admin_level}_ln_s0_pp.shp"
        filtered_features = [(s, r) for s, r in admin_features if r[admALL_index] == admin_level]
        write_shapefile(filtered_features, output_filename)

    if coastline_features:
        output_filename = f"{country_code}_elev_cst_ln_s0_pp_coastline.shp"
        write_shapefile(coastline_features, output_filename)

    if disputed_features:
        output_filename = f"{country_code}_admn_ad0_ln_s0_pp_disputedBoundaries.shp"
        write_shapefile(disputed_features, output_filename)

    print("Process completed successfully!")

except ValueError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")