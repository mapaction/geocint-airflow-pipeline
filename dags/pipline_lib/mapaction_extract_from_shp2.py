# # import geopandas as gpd
# # import pathlib
# # import logging

# # # Configure logging
# # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# # def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
# #     """Clips a shapefile by a country's GeoJSON boundary using geopandas."""

# #     output_shp_path = pathlib.Path(output_name).with_suffix(".shp")

# #     # Read the country boundary GeoJSON
# #     logging.info("Reading country boundary GeoJSON...")
# #     country_gdf = gpd.read_file(country_geojson_path)

# #     # Read the input shapefile
# #     logging.info("Reading input shapefile...")
# #     input_gdf = gpd.read_file(input_shp_path)

# #     # Reproject the input shapefile to match the CRS of the country boundary
# #     if input_gdf.crs != country_gdf.crs:
# #         logging.info(f"Reprojecting input shapefile to match CRS of country boundary: {country_gdf.crs}")
# #         input_gdf = input_gdf.to_crs(country_gdf.crs)

# #     # Perform intersection
# #     logging.info("Performing intersection...")
# #     clipped_gdf = gpd.overlay(input_gdf, country_gdf, how='intersection')

# #     # Check if the resulting GeoDataFrame is empty
# #     if clipped_gdf.empty:
# #         logging.warning(f"No features found within the country boundary for {output_name}. No shapefile will be created.")
# #         return  # Exit without creating any output, as no features intersect.

# #     # Save the resulting shapefile
# #     logging.info(f"Saving the resulting shapefile to {output_shp_path}...")
# #     clipped_gdf.to_file(output_shp_path, driver='ESRI Shapefile')
# #     logging.info(f"Shapefile created successfully: {output_shp_path}")

# # if __name__ == "__main__":
# #     import sys
# #     if len(sys.argv) != 4:
# #         print("Usage: python clip_shapefile.py <country_geojson> <input_shp> <output_name>")
# #     else:
# #         country_geojson_path = sys.argv[1]
# #         input_shp_path = sys.argv[2]
# #         output_name = sys.argv[3]

# #         clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)
# #v2
# import geopandas as gpd
# import pathlib
# import logging

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
#     """Clips a point shapefile by a country's GeoJSON boundary and saves the output as points."""

#     output_shp_path = pathlib.Path(output_name).with_suffix(".shp")

#     # Read the country boundary GeoJSON
#     logging.info("Reading country boundary GeoJSON...")
#     country_gdf = gpd.read_file(country_geojson_path)

#     # Read the input point shapefile
#     logging.info("Reading input shapefile...")
#     input_gdf = gpd.read_file(input_shp_path)

#     # Reproject the input shapefile to match the CRS of the country boundary
#     if input_gdf.crs != country_gdf.crs:
#         logging.info(f"Reprojecting input shapefile to match CRS of country boundary: {country_gdf.crs}")
#         input_gdf = input_gdf.to_crs(country_gdf.crs)

#     # Ensure that input is a point geometry
#     if not input_gdf.geom_type.eq('Point').all():
#         logging.error("The input shapefile does not contain point geometries. Exiting.")
#         return

#     # Filter points that fall within the country boundary
#     logging.info("Filtering points within the country boundary...")
#     clipped_gdf = input_gdf[input_gdf.geometry.within(country_gdf.unary_union)]

#     # Check if the resulting GeoDataFrame is empty
#     if clipped_gdf.empty:
#         logging.warning(f"No points found within the country boundary for {output_name}. No shapefile will be created.")
#         return  # Exit without creating any output, as no points intersect.

#     # Save the resulting shapefile as points
#     logging.info(f"Saving the resulting point shapefile to {output_shp_path}...")
#     clipped_gdf.to_file(output_shp_path, driver='ESRI Shapefile')
#     logging.info(f"Point shapefile created successfully: {output_shp_path}")

# if __name__ == "__main__":
#     import sys
#     if len(sys.argv) != 4:
#         print("Usage: python clip_points.py <country_geojson> <input_shp> <output_name>")
#     else:
#         country_geojson_path = sys.argv[1]
#         input_shp_path = sys.argv[2]
#         output_name = sys.argv[3]

#         clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)

# v3
import geopandas as gpd
import pathlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name):
    """Clips a point shapefile derived from polygon centroids by a country's GeoJSON boundary using geopandas."""

    output_shp_path = pathlib.Path(output_name).with_suffix(".shp")

    # Read the country boundary GeoJSON
    logging.info("Reading country boundary GeoJSON...")
    country_gdf = gpd.read_file(country_geojson_path)

    # Read the input polygon shapefile and convert to centroids
    logging.info("Reading input shapefile and converting to centroids...")
    input_gdf = gpd.read_file(input_shp_path)
    input_gdf['geometry'] = input_gdf.centroid  # Convert polygons to centroids

    # Reproject the centroids to match the CRS of the country boundary
    if input_gdf.crs != country_gdf.crs:
        logging.info(f"Reprojecting centroids to match CRS of country boundary: {country_gdf.crs}")
        input_gdf = input_gdf.to_crs(country_gdf.crs)

    # Filter centroids that fall within the country boundary
    logging.info("Filtering centroids within the country boundary...")
    clipped_gdf = input_gdf[input_gdf.geometry.within(country_gdf.unary_union)]

    # Check if the resulting GeoDataFrame is empty
    if clipped_gdf.empty:
        logging.warning(f"No centroids found within the country boundary for {output_name}. No shapefile will be created.")
        return  # Exit without creating any output, as no centroids intersect.

    # Save the resulting shapefile as points
    logging.info(f"Saving the resulting point shapefile to {output_shp_path}...")
    clipped_gdf.to_file(output_shp_path, driver='ESRI Shapefile')
    logging.info(f"Point shapefile created successfully: {output_shp_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python clip_centroids.py <country_geojson> <input_shp> <output_name>")
    else:
        country_geojson_path = sys.argv[1]
        input_shp_path = sys.argv[2]
        output_name = sys.argv[3]

        clip_shapefile_by_country(country_geojson_path, input_shp_path, output_name)
