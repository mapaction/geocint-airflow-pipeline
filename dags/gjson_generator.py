from airflow import DAG
from airflow.operators.python import PythonOperator
from pipline_lib.countires_iso import COUNTRIES
import pendulum
import os
import json
import geopandas as gpd
from shapely.geometry import mapping
import re
import pandas as pd

def get_admin_columns(admin_gdf, adm_level):
    expected_admin_name_columns = [
        f'ADM{adm_level}_EN',
        f'ADM{adm_level}_NAME',
        f'ADM{adm_level}_DESCR',
        f'NAME_{adm_level}',
        'NAME_EN',
        'NAME',
        'ADMIN_NAME',
    ]
    expected_pcode_columns = [
        f'ADM{adm_level}_PCODE',
        f'ADM{adm_level}_CODE',
        f'P_CODE',
        'PCODE',
        'CODE',
    ]
    admin_name_column = None
    pcode_column = None

    for col in expected_admin_name_columns:
        if col in admin_gdf.columns:
            admin_name_column = col
            break

    for col in expected_pcode_columns:
        if col in admin_gdf.columns:
            pcode_column = col
            break

    return admin_name_column, pcode_column

def generate_geojsons_per_country(country_iso, country_name, config):
    base_path = 'AllAdmnBoundaries'
    country_directory = os.path.join(base_path, country_iso.lower())
    output_base_directory = os.path.join(base_path, 'Subnational_jsons', country_iso.lower())
    docker_worker_working_dir = "/opt/airflow"
    country_geojson_filename = f"{docker_worker_working_dir}/dags/static_data/countries/{country_iso.lower()}.json"

    if not os.path.exists(country_geojson_filename):
        print(f"Country GeoJSON file not found: {country_geojson_filename}")
        return

    with open(country_geojson_filename, 'r') as f:
        country_geojson = json.load(f)
    
    crs = country_geojson.get("crs", {
        "type": "name",
        "properties": {
            "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
        }
    })

    if not os.path.exists(country_directory):
        print(f"Country directory not found: {country_directory}")
        return

    shapefiles = [f for f in os.listdir(country_directory) if f.endswith('.shp')]

    for shapefile in shapefiles:
        adm_level_match = re.search(r'adm(\d+)', shapefile)
        if adm_level_match:
            adm_level = adm_level_match.group(1)
        else:
            continue

        shapefile_path = os.path.join(country_directory, shapefile)
        admin_gdf = gpd.read_file(shapefile_path)

        admin_name_column, pcode_column = get_admin_columns(admin_gdf, adm_level)

        if admin_name_column is None or pcode_column is None:
            print(f"Could not find suitable admin name or pcode columns in {shapefile_path}")
            print(f"Available columns: {list(admin_gdf.columns)}")
            continue

        output_directory = os.path.join(output_base_directory, f'adm{adm_level}')
        os.makedirs(output_directory, exist_ok=True)

        previous_pcode_str = None

        for idx, row in admin_gdf.iterrows():
            admin_name = row[admin_name_column]
            pcode = row[pcode_column]
            geometry = row['geometry']

            if admin_name is None or pd.isna(admin_name):
                print(f"Warning: admin_name is None or NaN for idx {idx} in {shapefile_path}")
                admin_name = f"unknown_admin_{idx}"
            else:
                admin_name = str(admin_name)

            if pcode is None or pd.isna(pcode):
                print(f"Warning: pcode is None or NaN for admin unit '{admin_name}' in {shapefile_path}")
                if previous_pcode_str:
                    pcode_str = f"{previous_pcode_str}_exception"
                else:
                    clean_admin_name = re.sub(r'\W+', '_', admin_name)
                    pcode_str = f"{clean_admin_name.lower()}_{idx}"
            else:
                pcode_str = str(pcode).lower()
                previous_pcode_str = pcode_str  

            
            if geometry is None or geometry.is_empty:
                print(f"Warning: geometry is None or empty for admin unit '{admin_name}' with pcode '{pcode}' in {shapefile_path}")
                continue
                # Option B: Include with null geometry (uncomment the next line if you prefer this option)
                # geojson_geometry = None
            else:
                geojson_geometry = mapping(geometry)

            # Prepare GeoJSON data
            geojson_data = {
                "type": "FeatureCollection",
                "name": pcode_str,
                "crs": crs,
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "iso": country_iso.upper(),
                            "name_en": admin_name,
                            "pcode": pcode if not (pcode is None or pd.isna(pcode)) else None
                        },
                        "geometry": geojson_geometry
                    }
                ]
            }

            output_path = os.path.join(output_directory, f"{pcode_str}.json")
            with open(output_path, 'w') as f:
                json.dump(geojson_data, f, indent=4)

            print(f"Generated GeoJSON for {admin_name} with pcode {pcode} -> {output_path}")

def process_all_countries():
    configs = COUNTRIES
    for country_name, config in configs.items():
        country_iso = config['code']
        generate_geojsons_per_country(country_iso, country_name, config)

with DAG (
    dag_id="generate_subnational_geojsons",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['subnational_geojsons'],
) as dag:
    generate_geojsons_task = PythonOperator(
        task_id='generate_geojsons_task',
        python_callable=process_all_countries
    )
