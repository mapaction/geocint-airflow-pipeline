import os
import logging
from geo_admin_tools.utils.metadata_utils import update_metadata

def generate_admin_linework(gdf, linework_out_dir, iso_code, source_abbr, realname, level):
    """Generate admin linework from polygons, multipolygons, or linestring geometries."""
    try:
        if 'geometry' in gdf.columns:
            geom_type = gdf.geom_type.iloc[0]
            logging.info(f"Geometry type detected: {geom_type}")

            if geom_type in ['Polygon', 'MultiPolygon']:
                line_gdf = gdf.copy()
                line_gdf['geometry'] = line_gdf['geometry'].boundary
                line_gdf = line_gdf.drop_duplicates(subset=['geometry'])
            elif geom_type == 'LineString':
                line_gdf = gdf.copy()
            else:
                logging.error(f"Unsupported geometry type: {geom_type}. No linework generated.")
                return
            save_linework_shapefiles(line_gdf, linework_out_dir, iso_code, source_abbr, realname, level)
        else:
            logging.error("No geometry column found in the GeoDataFrame.")
    except Exception as e:
        logging.error(f"Error generating admin linework: {e}")

def save_linework_shapefiles(line_gdf, linework_out_dir, iso_code, source_abbr, realname, level):
    """Save linework shapefiles."""
    try:
        os.makedirs(linework_out_dir, exist_ok=True)

        linework_file = f"{iso_code}_admn_ad{level}_ln_s1_{source_abbr}_pp_{realname}.shp"
        line_outfile = os.path.join(linework_out_dir, linework_file)

        if not line_gdf.empty:
            try:
                line_gdf.to_file(line_outfile)
                logging.info(f"Created linework: {line_outfile}")
                update_metadata(iso_code, level, line_outfile)
            except Exception as e:
                logging.error(f"Failed to save shapefile {line_outfile}: {e}")
        else:
            logging.warning(f"No linework generated for {realname}. Line GDF was empty.")
    except Exception as e:
        logging.error(f"Error saving linework shapefiles: {e}")

def find_admlevel_column(gdf):
    """Function to find an exact 'admLevel' column match in a case-insensitive manner."""
    logging.info(f"Columns in the shapefile: {gdf.columns}")

    # Check for an exact 'admLevel' match (case-insensitive)
    for col in gdf.columns:
        if col.lower() == 'admlevel':
            logging.info(f"Found exact match for admLevel: {col}")
            return col  # Return the exact 'admLevel' match if found

    return None  # Return None if no exact match is found
