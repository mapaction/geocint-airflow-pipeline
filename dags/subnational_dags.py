import json
import os
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pipline_lib.tasks import *  # Assuming this contains your task definitions
from datetime import datetime
import pendulum
import re
from pipline_lib.countires_iso import COUNTRIES

# Load admin_types
with open("dags/static_data/admin_level_display_names.json", "r") as f:
    admin_types = json.load(f)

######################################
######## Dag Creation ################
######################################
def generate_subnational_dags(pcode, pcode_data):
    country_code = pcode_data["country_iso"]
    country_name = pcode_data["country_name"]
    admin_level = pcode_data["admin_lvl"]
    admin_type = admin_types[country_code].get(f"adm{admin_level}", "")
    subnational_name = pcode_data["name"]  # Get subnational name
    data_in_directory = f"data/input/{country_code}/{subnational_name}"
    data_out_directory = f"data/output/country_extractions/{country_code}/{subnational_name}"
    cmf_directory = f"data/cmfs/{country_code}{subnational_name}"
    docker_worker_working_dir = "/opt/airflow"
    country_geojson_filename = f"{docker_worker_working_dir}/dags/static_data/subnational_jsons/{pcode}.json"

    pattern = r"[^a-zA-Z0-9]"  # Matches any character that is NOT alphanumeric
    subnational_name = re.sub(pattern, "_", subnational_name)

    dag_id = f"Subnational_DAG_{subnational_name}"

    # message dict
    message = {
        "emoji": 'cat',
        "channel": 'datapipeline',
        "username": 'geocint',
        "text": '',
    }

    # task_args dict
    task_args = {
        "docker_worker_working_dir": docker_worker_working_dir,
        "data_in_directory": data_in_directory,
        "data_out_directory": data_out_directory,
        "country_code": country_code,
        "country_name": country_name,
        "cmf_directory": cmf_directory,
        "country_geojson_filename": country_geojson_filename
    }

    default_args = {
        'owner': 'airflow',
        'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=['Subnational', country_name, f"admn{admin_level}", admin_type, subnational_name, pcode],
        ) as dag:

        message['text'] = f"[{datetime.now()}]: {dag_id} ---- Dag has started running !!!"
        send_slack_message_task = send_slack_message(message)

        make_data_dirs_task = make_data_dirs(**task_args)

        ######################################
        ######## Task groups #################
        ######################################
        with TaskGroup(group_id=f"hdx_data_scrape_{pcode}") as hdx_data_scrape_group :
            with dag:
                download_population_with_sadd_inst = download_population_with_sadd(task_concurrency=3, **task_args)
                transform_population_with_sadd_inst = transform_population_with_sadd(task_concurrency=3, **task_args)
                download_population_with_sadd_inst >> transform_population_with_sadd_inst

            for download_task in hdx_data_scrape_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        with TaskGroup(group_id=f"hdx_data_download_{pcode}") as hdx_data_download_group:
            with dag:
                download_all_hdx_country_data_types_inst = download_all_hdx_country_data_types(task_concurrency=3, **task_args)
                download_hdx_country_data_inst = download_hdx_country_data(task_concurrency=2, **task_args)
                download_all_hdx_country_data_types_inst >> download_hdx_country_data_inst

            for download_task in hdx_data_download_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        with TaskGroup(group_id=f"roads_populated_places_{pcode}") as roads_populated_places_group:
            with dag:
                ne_10m_roads_inst = ne_10m_roads(task_concurrency=3, **task_args)
                ne_10m_populated_place_inst = ne_10m_populated_places(task_concurrency=3, **task_args)
                transform_ne_10m_roads_inst = transform_ne_10m_roads(task_concurrency=2, **task_args)
                transform_ne_10m_populated_places_inst = transform_ne_10m_populated_places(task_concurrency=2, **task_args)
                ne_10m_roads_inst >> transform_ne_10m_roads_inst
                ne_10m_populated_place_inst >> transform_ne_10m_populated_places_inst

            for download_task in roads_populated_places_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Water Features
        with TaskGroup(group_id=f"water_features_{pcode}") as water_features_group:
            with dag:
                ne_10m_rivers_lake_centerlines_inst = ne_10m_rivers_lake_centerlines(task_concurrency=3, **task_args)
                ne_10m_lakes_inst = ne_10m_lakes(task_concurrency=3, **task_args)
                transform_ne_10m_rivers_lake_centerlines_inst = transform_ne_10m_rivers_lake_centerlines(task_concurrency=2, **task_args)
                transform_ne_10m_lakes_inst = transform_ne_10m_lakes(task_concurrency=2, **task_args)
                ne_10m_rivers_lake_centerlines_inst >> transform_ne_10m_rivers_lake_centerlines_inst
                ne_10m_lakes_inst >> transform_ne_10m_lakes_inst

            for download_task in water_features_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Energy and Infrastructure
        with TaskGroup(group_id=f"energy_infrastructure_{pcode}") as energy_infrastructure_group:
            with dag:
                power_plants_inst = power_plants(task_concurrency=3, **task_args)
                worldports_inst = worldports(task_concurrency=3, **task_args)
                ourairports_inst = ourairports(task_concurrency=3, **task_args)
                transform_power_plants_inst = transform_power_plants(task_concurrency=2, **task_args)
                transform_worldports_inst = transform_worldports(task_concurrency=2, **task_args)
                transform_ourairports_inst = transform_ourairports(task_concurrency=2, **task_args)
                power_plants_inst >> transform_power_plants_inst
                worldports_inst >> transform_worldports_inst
                ourairports_inst >> transform_ourairports_inst

            for download_task in energy_infrastructure_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Elevation and Terrain
        with TaskGroup(group_id=f"elevation_terrain_{pcode}") as elevation_terrain_group:
            with dag:
                download_elevation90_dtm_inst = download_elevation90_dtm(task_concurrency=1, **task_args)
                download_gmdted250_dtm_inst = download_gmdted250_dtm(task_concurrency=1, **task_args)
                transform_elevation90_dtm_inst = transform_elevation90_dtm(task_concurrency=1, **task_args)
                transform_gmdted250_dtm_inst = transform_gmdted250_dtm(task_concurrency=1, **task_args)
                download_elevation90_hsh_inst = download_elevation90_hsh(task_concurrency=1, **task_args)
                download_gmdted250_hsh_inst = download_gmdted250_hsh(task_concurrency=1, **task_args)
                transform_elevation90_hsh_inst = transform_elevation90_hsh(task_concurrency=1, **task_args)
                transform_gmdted250_hsh_inst = transform_gmdted250_hsh(task_concurrency=1, **task_args)
                download_elevation90_dtm_inst >> transform_elevation90_dtm_inst
                download_gmdted250_dtm_inst >> transform_gmdted250_dtm_inst
                download_elevation90_hsh_inst >> transform_elevation90_hsh_inst
                download_gmdted250_hsh_inst >> transform_gmdted250_hsh_inst

            for download_task in elevation_terrain_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Administrative Boundaries
        with TaskGroup(group_id=f"admin_boundaries_{pcode}") as admin_boundaries_group:
            with dag:
                download_world_admin_boundaries_inst = download_world_admin_boundaries(task_concurrency=3, **task_args)
                ocha_admin_boundaries_inst = ocha_admin_boundaries(task_concurrency=3, **task_args)
                transform_feather_inst = create_feather_task(task_concurrency=2, **task_args)
                transform_admin_linework_inst = transform_admin_linework(task_concurrency=2, **task_args)
                transform_world_admin_boundaries_inst = transform_world_admin_boundaries(task_concurrency=2, **task_args)

                download_world_admin_boundaries_inst >> transform_world_admin_boundaries_inst
                ocha_admin_boundaries_inst >> [transform_admin_linework_inst, transform_feather_inst]
            
            for download_task in admin_boundaries_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Dams and Reservoirs (from Geodar)
        with TaskGroup(group_id=f"dams_reservoirs_{pcode}") as dams_reservoirs_group:
            with dag:
                download_geodar_data_inst = download_geodar_data(task_concurrency=3, **task_args)
                transform_dams_inst = transform_dams(task_concurrency=2, **task_args)
                transform_reservoirs_inst = transform_reservoirs(task_concurrency=2, **task_args)

                download_geodar_data_inst >> [transform_dams_inst, transform_reservoirs_inst]

            for download_task in dams_reservoirs_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Coastline
        with TaskGroup(group_id=f"coastline_{pcode}") as coastline_group:
            with dag:
                download_world_coastline_data_inst = download_world_coastline_data(task_concurrency=3, **task_args)
                transform_world_costline_data_inst = transform_world_costline_data(task_concurrency=2, **task_args)
                extract_country_national_coastline_inst = extract_country_coastline_v2(task_concurrency=2, **task_args)

                download_world_coastline_data_inst >> transform_world_costline_data_inst >> extract_country_national_coastline_inst

            for download_task in coastline_group:
                download_task.trigger_rule = TriggerRule.ALL_DONE


        with TaskGroup(group_id=f"osm_tasks_{pcode}") as osm_group:
            with dag:
                ##################################
                ######## OSM definitions #########
                ##################################
                osm_dam_inst = osm_dam(task_concurrency=1,**task_args)
                osm_school_inst = osm_school(task_concurrency=1,**task_args)
                osm_education_inst = osm_education(task_concurrency=1,**task_args)
                osm_ferry_inst = osm_ferry(task_concurrency=1,**task_args)
                osm_ferry_route_inst = osm_ferry_route(task_concurrency=1,**task_args)
                osm_port_inst = osm_port(task_concurrency=1,**task_args)
                osm_bank_inst = osm_bank(task_concurrency=1,**task_args)
                osm_atm_inst = osm_atm(task_concurrency=1,**task_args)
                osm_healthfacilities_inst = osm_healthfacilities(task_concurrency=1,**task_args)
                osm_hospital_inst = osm_hospital(task_concurrency=1,**task_args)
                osm_border_control_inst = osm_border_control(task_concurrency=1,**task_args)
                osm_settlement_inst = osm_settlement(task_concurrency=1,**task_args)
                osm_large_river_inst = osm_large_river(task_concurrency=1,**task_args)
                osm_phys_river_inst = osm_phys_river(task_concurrency=1,**task_args)
                osm_canal_inst = osm_canal(task_concurrency=1,**task_args)
                osm_railway2_inst = osm_railway2(task_concurrency=1,**task_args)
                osm_dam_inst,
                osm_school_inst,
                osm_education_inst,
                osm_ferry_inst,
                osm_ferry_route_inst,
                osm_port_inst,
                osm_bank_inst,
                osm_atm_inst,
                osm_healthfacilities_inst,
                osm_hospital_inst,
                osm_border_control_inst,
                osm_settlement_inst,
                osm_large_river_inst,
                osm_phys_river_inst,
                osm_canal_inst,
                osm_railway2_inst

                for download_task in osm_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        with TaskGroup(group_id=f"other_download_tasks_{pcode}") as other_download_group:
            with dag:
                ######################################
                ######## Variable definitions ########
                ######################################
                oceans_and_seas_inst = oceans_and_seas(**task_args)
                hyrdrorivers_inst = hyrdrorivers(**task_args)
                download_healthsites_inst = healthsites(**task_args)
                download_worldpop1km_inst = worldpop1km(**task_args)
                download_worldpop100m_inst = worldpop100m(**task_args)
                wfp_railroads_inst= wfp_railroads(**task_args)
                wfp_boarder_crossings_inst = wfp_boarder_crossings(**task_args)
                oceans_and_seas_inst,
                hyrdrorivers_inst,
                download_healthsites_inst,
                download_worldpop1km_inst,
                download_worldpop100m_inst,
                wfp_railroads_inst,
                wfp_boarder_crossings_inst
                

                for download_task in [oceans_and_seas_inst, hyrdrorivers_inst, download_healthsites_inst, download_worldpop1km_inst, download_worldpop100m_inst, wfp_railroads_inst, wfp_boarder_crossings_inst]:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Split export_tasks into two groups
        with TaskGroup(group_id=f"export_tasks_{pcode}") as export_group_1:
            with dag:
                mapaction_export_s3_task = mapaction_export_s3(**task_args)
                upload_cmf_all_task = upload_cmf_all(**task_args)
                upload_datasets_all_task = upload_datasets_all(**task_args)
                datasets_ckan_descriptions_task = datasets_ckan_descriptions(**task_args)
                create_completeness_report_task = create_completeness_report(**task_args)
                # Set dependencies for each export task
                for export_task in [mapaction_export_s3_task, upload_cmf_all_task, upload_datasets_all_task, datasets_ckan_descriptions_task, create_completeness_report_task]:
                    export_task.trigger_rule = TriggerRule.ALL_DONE

        with TaskGroup(group_id=f"export_intensive_tasks_{pcode}") as export_group_2:
            with dag:
                mapaction_export_s3_task_2 = mapaction_export_s3(**task_args)
                upload_cmf_all_task_2 = upload_cmf_all(**task_args)
                upload_datasets_all_task_2 = upload_datasets_all(**task_args)
                datasets_ckan_descriptions_task_2 = datasets_ckan_descriptions(**task_args)
                create_completeness_report_task_2 = create_completeness_report(**task_args)
                # Set dependencies for each export task
                for export_task in [mapaction_export_s3_task_2, upload_cmf_all_task_2, upload_datasets_all_task_2, datasets_ckan_descriptions_task_2, create_completeness_report_task_2]:
                    export_task.trigger_rule = TriggerRule.ALL_DONE

        # Create tasks for individual OSM functions
        osm_roads_task = osm_roads(task_concurrency=1, **task_args)
        osm_waterbodies_task = osm_waterbodies(task_concurrency=1, **task_args)
        osm_railway_task = osm_railway(task_concurrency=1, **task_args)
        # download_elevation30_inst = download_elevation30(task_concurrency=1, **task_args)
        # transform_elevation30_inst = transform_elevation30(task_concurrency=1, **task_args)
        
        # DAG Structure
        send_slack_message_task >> make_data_dirs_task >> [
            hdx_data_scrape_group,
            hdx_data_download_group,
            water_features_group, 
            dams_reservoirs_group,
            coastline_group,
            elevation_terrain_group,
            admin_boundaries_group,
            energy_infrastructure_group,
            roads_populated_places_group, 
            osm_group, 
            other_download_group
        ] >> export_group_1

        mid_message = message.copy()
        end_message = message.copy()

        mid_message['text'] = f"[{datetime.now()}]: {dag_id} ---- Dag has completed its run of less intensive datasets. Now processing SRTM30, Waterbodies, Raods and Railways datasets"
        send_slack_message_task_mid = send_slack_message(mid_message)

        # Ensure OSM tasks run after the first export
        # Removed >> download_elevation30_inst >> transform_elevation30_inst
        export_group_1 >> send_slack_message_task_mid >> osm_roads_task >> osm_waterbodies_task >> osm_railway_task 

        # Export data after Intensive tasks complete
        # Removed download_elevation30_inst, transform_elevation30_inst
        [osm_roads_task, osm_waterbodies_task, osm_railway_task] >> export_group_2

        end_message['text'] = f"[{datetime.now()}]: {dag_id} ---- Dag has completed its run of please check the Airflow UI..."
        send_slack_message_task_end = send_slack_message(end_message) 

        # Ensure notification is sent after export
        export_group_2 >> send_slack_message_task_end 
        
    return dag


def create_subnational_dags(pcode, pcode_data):
    """Creates a group of DAGs for the given pcodes."""
    dag = generate_subnational_dags(pcode, pcode_data)
    globals()[dag.dag_id] = dag

# Generate DAGs for all countries in the config
docker_worker_working_dir = "/opt/airflow" 

missing_countries_list = os.path.join(
    f"{docker_worker_working_dir}", "dags", "missing_countries.txt"
)
countries_to_process_list = os.path.join(
    f"{docker_worker_working_dir}", "dags", "countries_to_process.txt"
)

with open(missing_countries_list, "r") as f:
    missing_countries = {line.strip() for line in f}

# Define countries to process (can be from a config file)
with open(countries_to_process_list, "r") as f:
    countries_to_process = {line.strip() for line in f}

for country_name, config in COUNTRIES.items():
    country_code = config["code"]
    if country_name in countries_to_process and country_code not in missing_countries:
        pcode_file = f"{docker_worker_working_dir}/dags/static_data/pcodes/{country_code}.json"

        # Check if the file exists before opening
        if os.path.exists(pcode_file):
            with open(os.path.join(pcode_file), "r") as f:
                pcode_data_from_file = json.load(f)
                for pcode, pcode_data in pcode_data_from_file.items():
                    create_subnational_dags(pcode, pcode_data)
        else:
            logging.warning(f"Skipping {country_name} ({country_code}) - pcode file not found: {pcode_file}")
