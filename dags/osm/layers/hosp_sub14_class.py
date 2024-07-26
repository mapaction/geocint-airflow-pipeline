import os
import osmnx as ox
import geopandas as gpd
import pandas as pd

class OSMHospitalDataDownloader:

    def __init__(self, geojson_path, crs_project, crs_global, country_code):
        self.geojson_path = geojson_path
        self.crs_project = crs_project
        self.crs_global = crs_global
        self.osm_tags_hospital = {'amenity': 'hospital'}
        self.attributes = ['name', 'name:en', 'name_en', 'emergency', 'operator', 'operator:type', 'beds', 'operator_type', 'operator_ty']
        ox.settings.log_console = True
        ox.settings.use_cache = True
        self.output_filename = f"osm-data/output/country_extractions/{country_code}/215_heal/{country_code}_heal_hea_pt_s3_osm_pp_hospital.shp"

    def download_and_process_data(self):
        region_gdf = gpd.read_file(self.geojson_path)
        geometry = region_gdf['geometry'].iloc[0]

        if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
            raise ValueError("Geometry type not supported. Please provide a Polygon or MultiPolygon.")

        # Download hospital data
        gdf_hospitals = ox.geometries_from_polygon(geometry, tags=self.osm_tags_hospital)

        # Ensure unique column names
        gdf_hospitals = self.ensure_unique_column_names(gdf_hospitals)

        # Process geometries to centroid points
        gdf_hospitals = self.process_geometries(gdf_hospitals)

        # Ensure required fields and create fclass2 column
        gdf_hospitals = self.ensure_required_fields(gdf_hospitals)

        # Add 'fclass' column
        if 'fclass' not in gdf_hospitals.columns:
            gdf_hospitals['fclass'] = 'hospital'

        # Reorder columns to make fclass the first column
        columns = ['fclass'] + [col for col in gdf_hospitals.columns if col != 'fclass']
        gdf_hospitals = gdf_hospitals[columns]

        # Reorder columns to make fclass2 the second column
        columns = ['fclass'] + ['fclass2'] + [col for col in gdf_hospitals.columns if col not in ['fclass', 'fclass2']]
        gdf_hospitals = gdf_hospitals[columns]

        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        if not gdf_hospitals.empty:
            gdf_hospitals.to_file(self.output_filename, driver='ESRI Shapefile')
        else:
            print("No data to save.")

    def process_geometries(self, gdf):
        # Create centroids for polygon geometries and reproject
        gdf = gdf.to_crs(epsg=self.crs_project)
        gdf['geometry'] = gdf.apply(lambda row: row['geometry'].centroid if row['geometry'].geom_type != 'Point' else row['geometry'], axis=1)
        gdf = gdf.to_crs(epsg=self.crs_global)

        # Handle list-type fields
        for col in gdf.columns:
            # Check if the column is of object type and contains lists
            if pd.api.types.is_object_dtype(gdf[col]) and gdf[col].apply(lambda x: isinstance(x, list) if x is not None else False).any():
                gdf[col] = gdf[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        actual_tags = gdf.columns.intersection(self.attributes)
        missing_tags = set(self.attributes) - set(actual_tags)
        if missing_tags:
            print(f"Warning: The following tags are missing from the data and will not be included: {missing_tags}")

        
        for tag in missing_tags:
            if tag not in gdf.columns:
                gdf[tag] = None

        
        columns_to_keep = ['geometry'] + [col for col in self.attributes if col in gdf.columns]
        gdf = gdf[columns_to_keep]

        return gdf

    def ensure_unique_column_names(self, gdf):
        truncated_columns = {}
        final_columns = {}
        unique_suffixes = {}

        # Step 1: Truncate names
        for col in gdf.columns:
            truncated = col[:10]
            if truncated not in truncated_columns:
                truncated_columns[truncated] = 1
            else:
                truncated_columns[truncated] += 1
            final_columns[col] = truncated

        # Step 2: Resolve duplicates by adding a unique suffix
        for original, truncated in final_columns.items():
            if truncated_columns[truncated] > 1:
                if truncated not in unique_suffixes:
                    unique_suffixes[truncated] = 1
                else:
                    unique_suffixes[truncated] += 1
                suffix = unique_suffixes[truncated]
                suffix_length = len(str(suffix))
                truncated_with_suffix = truncated[:10-suffix_length] + str(suffix)
                final_columns[original] = truncated_with_suffix

        gdf.rename(columns=final_columns, inplace=True)
        return gdf

    def ensure_required_fields(self, gdf):
        required_fields = ['emergency', 'operator', 'operator_type', 'beds']
        for field in required_fields:
            if field not in gdf.columns:
                gdf[field] = None e

        
        gdf['fclass2'] = gdf.apply(
            lambda row: ', '.join([field for field in required_fields if pd.notna(row[field])]) or 'hospital', axis=1
        )

        return gdf

    def save_data(self, gdf):
        
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)

        try:
            gdf.to_file(self.output_filename, driver='ESRI Shapefile')
        except Exception as e:
            print(f"An error occurred while saving the GeoDataFrame: {e}")
