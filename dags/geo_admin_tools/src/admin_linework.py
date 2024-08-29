import os
from geo_admin_tools.utils.metadata_utils import update_metadata

def generate_admin_linework(gdf, linework_out_dir, iso_code, source_abbr, realname, adm_column):
    """Generate admin linework from polygons or handle LineString geometries."""
    try:
        if 'geometry' in gdf.columns:
            if gdf.geom_type.iloc[0] == 'Polygon':
                line_gdf = gdf.copy()
                line_gdf['geometry'] = line_gdf['geometry'].boundary
                line_gdf = line_gdf.drop_duplicates(subset=['geometry'])
            elif gdf.geom_type.iloc[0] == 'LineString':
                line_gdf = gdf.copy()
            else:
                print("Unsupported geometry type. No linework generated.")
                return

            save_linework_shapefiles(line_gdf, linework_out_dir, iso_code, source_abbr, realname, adm_column)

        else:
            print("No geometry column found in the GeoDataFrame.")
    except Exception as e:
        print(f"Error generating admin linework: {e}")
# should convert any .shp one that has admALL and one that has admbndl
def find_admlevel_column(gdf):
    """Function to find the 'admLevel' column in a case-insensitive manner."""
    for col in gdf.columns:
        if col.lower() == 'admlevel':
            return col
    return None

def save_linework_shapefiles(line_gdf, linework_out_dir, iso_code, source_abbr, realname, adm_column):
    try:
        os.makedirs(linework_out_dir, exist_ok=True)
        realname_mapping = {
    0: "country",
    1: "parish",  # Changed from "region"
    2: "province",
    3: "district",
    4: "subdistrict",
}
        for level in line_gdf[adm_column].unique():
            realname = realname_mapping.get(level, "Admin")
            linework_file = f"{iso_code}_admn_ad{level}_ln_s1_{source_abbr}_pp_{realname}.shp"
            line_outfile = os.path.join(linework_out_dir, linework_file)

            line_level_gdf = line_gdf[line_gdf[adm_column] == level]
            if not line_level_gdf.empty:
                line_level_gdf.to_file(line_outfile)
                update_metadata(iso_code, f"linework_{level}", line_outfile)
            else:
                print(f"No linework generated for level {level}.")
    except Exception as e:
        print(f"Error saving linework shapefiles: {e}")

