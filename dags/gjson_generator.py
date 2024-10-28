from airflow import DAG
from airflow.operators.python import PythonOperator
from pipline_lib.countires_iso import COUNTRIES
import pendulum
import os
import json
import logging
import geopandas as gpd
from shapely.geometry import mapping
import re
import pandas as pd
import pycountry
from airflow.exceptions import AirflowFailException 
import re

def get_admin_columns(admin_gdf, adm_level):
    expected_admin_name_patterns = [
        re.compile(rf'ADM{adm_level}_[A-Z]{{2}}', re.IGNORECASE),  # e.g., ADM1_EN, adm2_fr
        re.compile(rf'NAME_{adm_level}_[A-Z]{{2}}', re.IGNORECASE), # e.g., NAME_1_ES 
        re.compile(rf'NAME_{adm_level}', re.IGNORECASE),          # e.g., NAME_1, name_0
        re.compile(rf'VARNAME_{adm_level}', re.IGNORECASE),       # e.g., VARNAME_1
        re.compile(rf'NL_NAME_{adm_level}', re.IGNORECASE),
        re.compile(rf'adm{adm_level}_name', re.IGNORECASE),  # e.g., adm1_name, Adm2_Name   
        re.compile(rf'admin{adm_level}Name', re.IGNORECASE),  # e.g., admin1Name, ADMIN2NAME
        re.compile(rf'NAMA{adm_level}', re.IGNORECASE),  # e.g., NAMA0, nama1, Nama2
    ]
    expected_admin_types_columns = [
        re.compile(rf'ADM{adm_level}_TYPE', re.IGNORECASE),
        re.compile(rf'ADM{adm_level}_TYPE', re.IGNORECASE),
        re.compile(rf'TYPE_{adm_level}', re.IGNORECASE),     # e.g., TYPE_1
        re.compile(rf'ENGTYPE_{adm_level}', re.IGNORECASE),  # e.g., ENGTYPE_1
        re.compile(rf'Type{adm_level}', re.IGNORECASE), 
    ]
    expected_pcode_columns = [
        re.compile(rf'ADM{adm_level}_PCODE', re.IGNORECASE),
        re.compile(rf'ADM{adm_level}_CODE', re.IGNORECASE),
        re.compile(r'P_CODE', re.IGNORECASE), 
        re.compile(r'PCODE', re.IGNORECASE), 
        re.compile(r'CODE', re.IGNORECASE), 
        re.compile(rf'admin{adm_level}Pcod', re.IGNORECASE),  # e.g., admin1Pcod, ADMIN2PCOD
        re.compile(rf'SHN{adm_level}', re.IGNORECASE),  # e.g., SHN0, shn1, Shn2
    ]

    admin_name_column = None
    for pattern in expected_admin_name_patterns:
        for col in admin_gdf.columns:
            if pattern.match(col):
                admin_name_column = col
                break  # Exit inner loop if match found
        if admin_name_column:
            logging.info(f"ADMIN NAME FOUND IS >>>>>>>>>> {admin_name_column}")
            break  # Exit outer loop if match found

    admin_type_column = None
    for pattern in expected_admin_types_columns:
        for col in admin_gdf.columns:
            if pattern.match(col):
                admin_type_column = col
                break
        if admin_type_column:
            logging.info(f"ADMIN TYPE FOUND IS >>>>>>>>>> {admin_name_column}")
            break  # Exit outer loop if match found

    pcode_column = None
    for pattern in expected_pcode_columns:
        for col in admin_gdf.columns:
            if pattern.match(col):
                pcode_column = col
                break
        if pcode_column:
            logging.info(f"PCODE FOUND IS >>>>>>>>>> {admin_name_column}")
            break  # Exit outer loop if match found

    return admin_name_column, pcode_column, admin_type_column


def generate_geojsons_per_country(country_iso, country_name, shapefiles):
    docker_worker_working_dir = "/opt/airflow"
    output_directory = os.path.join(docker_worker_working_dir, "dags", "static_data", "subnational_jsons")
    pcode_file = os.path.join(docker_worker_working_dir, "dags", "static_data", "pcodes", f"{country_iso}.json")
    country_geojson_filename = f"{docker_worker_working_dir}/dags/static_data/countries/{country_iso.lower()}.json"

    try:
        with open(pcode_file, 'w') as f:
            json.dump({}, f)
    except FileNotFoundError:
        logging.error(f"Could not find or create file: {pcode_file}")
        try:
            logging.info(f"Attempting to create the missing file: {pcode_file}")
            os.makedirs(os.path.dirname(pcode_file), exist_ok=True)  # Create parent directories if needed
            with open(pcode_file, 'w') as f:
                pass
        except OSError as e:
            logging.exception(f"Failed to create file: {pcode_file} due to {e}")
            raise AirflowFailException(f"Failed to create file: {pcode_file} due to {e}") 
        
    if not shapefiles:
        raise AirflowFailException(f"Shapefiles is empty for {country_iso}")


    if not os.path.exists(country_geojson_filename):
        logging.error(f"Country GeoJSON file not found: {country_geojson_filename}")
        raise AirflowFailException(f"Country GeoJSON file not found: {country_geojson_filename}") 

    with open(country_geojson_filename, 'r') as f:
        country_geojson = json.load(f)
    
    crs = country_geojson.get("crs", {
        "type": "name",
        "properties": {
            "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
        }
    })

    # Get country directory from shapefiles and log these
    if shapefiles:
        files = [f"{s}\n" for s in shapefiles]
        files_text = '\n'.join(files)
        logging.info(f"Batch contains the following >>> \\n {files_text}")
        
    else:
        logging.error(f"Shapefiles is empty for {country_iso}")
        raise AirflowFailException(f"Shapefiles is empty for {country_iso}") 

    # Filter shapefiles for adm levels 0-2
    adm_shapefiles = []
    for shapefile in shapefiles:
        adm_level_match = re.search(r'adm(\d+)', shapefile)
        if adm_level_match:
            adm_level = int(adm_level_match.group(1))
            if 0 <= adm_level <= 2:
                adm_shapefiles.append((shapefile, adm_level))

    logging.info(f"Admin shapefiles >>> {adm_shapefiles}")

    if len(adm_shapefiles) == 0:
        files = [f"{s}\n" for s in shapefiles]
        files_text = '\n'.join(files)
        raise AirflowFailException(f"Found a shape file but wasnt able to process check the naming convention \\n {files_text}") 

    # Process shapefiles
    for shapefile, adm_level in adm_shapefiles:
        shapefile_path = os.path.join(shapefile)
        try:
            admin_gdf = gpd.read_file(shapefile_path)

            admin_name_column, pcode_column, admin_type_column = get_admin_columns(admin_gdf, adm_level)

            if admin_name_column is None or pcode_column is None:
                # Extract relevant row information
                row_info = []
                for idx, row in admin_gdf.iterrows():
                    if idx >= 5:  # Limit to 5 rows
                        break  # Exit the loop if we have processed 5 rows
                    row_info.append({
                        'index': idx,
                        'row_info': row.to_dict()
                    })

                    raise AirflowFailException(f"""Could not find suitable admin name or pcode columns in {shapefile_path}
                                                    Available columns: {list(admin_gdf.columns)}
                                                    Found columns: {admin_name_column, pcode_column, admin_type_column}
                                                    Row information (first 5 rows): {row_info}""")

            os.makedirs(output_directory, exist_ok=True)

            for idx, row in admin_gdf.iterrows():
                pcode = row[pcode_column] if pcode_column in admin_gdf.columns else None

                if pcode is not None:
                    # Get the ISO2 code for the country (using the ISO3 code from country_iso)
                    country = pycountry.countries.get(alpha_3=country_iso)
                    if country:
                        country_name = country.name
                    else:
                        country_name = None
                        logging.warning(f"Could not find suitable country name for row id {idx}:{row}")

                    # Prioritize 'ADM{admin_level}_EN' if present, otherwise use the country-specific column
                    if admin_name_column is not None:
                        if admin_name_column in admin_gdf.columns:
                            admin_name =  row[admin_name_column]
                        elif admin_name_column.lower() in admin_gdf.columns:
                            admin_name = row[admin_name_column.lower()]
                        else:
                            logging.warning(f"Could not find suitable admin name for row id {idx}:{row}")
                    else:
                        admin_name = ""
                        logging.warning(f"Could not find suitable admin name for row id {idx}:{row}")
                        raise AirflowFailException(f"Could not find suitable admin name for row id {idx}:{row}")
                    
                    # Get admin type, checking for both uppercase and lowercase column names
                    if admin_type_column is not None:
                        if admin_type_column in admin_gdf.columns:
                            admin_type = row[admin_type_column]
                        elif admin_type_column.lower() in admin_gdf.columns:
                            admin_type = row[admin_type_column.lower()]
                    else:
                        admin_type = ""
                        logging.warning(f"Could not find suitable admin type for row id {idx}:{row}")

                    geometry = row['geometry']
                    if geometry is None or geometry.is_empty:
                        logging.warning(f"Warning: geometry is None or empty for admin unit '{admin_name}' with pcode '{pcode}' in {shapefile_path}")
                        break
                    else:
                        geojson_geometry = mapping(geometry)

                    # Prepare GeoJSON data
                    geojson_data = {
                        "type": "FeatureCollection",
                        "name": pcode,
                        "crs": crs,
                        "features": [
                            {
                                "type": "Feature",
                                "properties": {
                                    "iso": country_iso.upper(),
                                    "name_en": admin_name,
                                    "pcode": pcode,
                                    "admin_type": admin_type,
                                    "admin_level": adm_level,
                                    "country_name": country_name
                                },
                                "geometry": geojson_geometry
                            }
                        ]
                    }

                    with open(pcode_file, 'r+') as f:
                        try:
                            pcode_data = json.load(f)  # Load existing data
                        except json.JSONDecodeError:
                            pcode_data = {}  # If file is empty or invalid JSON

                        pcode_data[pcode] = {
                            'name': admin_name,
                            'admin_lvl': adm_level,
                            'country_iso': country_iso,
                            'country_name': country_name,
                            'admin_type': admin_type
                        }

                        f.seek(0)  # Go back to the beginning of the file
                        json.dump(pcode_data, f, indent=4)  # Write the updated data
                        f.truncate()  # Remove any remaining old data

                    logging.info(f"Processing complete. Pcodes saved to {pcode_file}")

                    output_path = os.path.join(output_directory, f"{pcode}.json")
                    with open(output_path, 'w') as f:
                        json.dump(geojson_data, f, indent=4)

                    logging.info(f"Generated GeoJSON for {admin_name} with pcode {pcode} -> {output_path}")

        except Exception as e:
            logging.exception(f"An error occurred while processing {shapefile_path}: {e}")
            raise AirflowFailException(f"An error occurred while processing {shapefile_path}: {e}")


with DAG(
    dag_id="generate_subnational_geojsons",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['subnational_geojsons', "pcodes"],
) as dag:
    missing_countries_file = "/opt/airflow/dags/missing_countries.txt"

    with open(missing_countries_file, "w") as missing_file:
        missing_file.write("")

    for country_name, config in COUNTRIES.items():
        country_iso = config['code']
        base_path = 'AllAdminBoundaries'
        country_directory = os.path.join(base_path, country_iso.lower())
        shapefiles = []

        if os.path.exists(country_directory):
            try:
                # Check for shapefiles in the country_iso directory
                shapefiles.extend([
                    os.path.join(country_directory, f) 
                    for f in os.listdir(country_directory) if f.endswith('.shp')
                ])

                # Check for shapefiles in subdirectories within country_iso
                for subdir in os.listdir(country_directory):
                    subdir_path = os.path.join(country_directory, subdir)
                    if os.path.isdir(subdir_path):
                        shapefiles.extend([
                            os.path.join(subdir_path, f) 
                            for f in os.listdir(subdir_path) if f.endswith('.shp')
                        ])

                generate_geojsons_task = PythonOperator(
                        task_id=f'generate_geojsons_{country_iso}',
                        python_callable=generate_geojsons_per_country,
                        op_kwargs={
                            'country_iso': country_iso,
                            'country_name': country_name,
                            'shapefiles': shapefiles,
                        }
                    )
            except OSError as e:
                logging.exception(f"An error occurred while accessing files for {country_iso}: {e}")
                raise AirflowFailException(f"An error occurred while accessing files for {country_iso}: {e}")
        else:
            logging.error(f"Country directory not found: {country_iso}")
            with open(missing_countries_file, "a") as missing_file:  # Open in append mode
                missing_file.write(f"{country_iso}\n")  # Add the missing country to the file