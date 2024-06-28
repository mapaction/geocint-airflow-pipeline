import pendulum
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag
from airflow.utils.trigger_rule import TriggerRule
from pipline_lib.tasks import *
from pipline_lib.countires_iso import COUNTRIES
from datetime import datetime

# This is a way to create a different pipeline for every country, so they
# (hopefully) run in parallel. This is called "dynamic DAG generation" in the docs.
configs = COUNTRIES

######################################
######## Dag Creation ################
######################################
def create_mapaction_pipeline(country_name, config):
    dag_id = f"dynamic_generated_dag_{country_name}"
    country_code = config['code']
    data_in_directory = f"data/input/{country_code}"
    data_out_directory = f"data/output/country_extractions/{country_code}"
    cmf_directory = f"data/cmfs/{country_code}"
    docker_worker_working_dir = "/opt/airflow"
    country_geojson_filename = f"{docker_worker_working_dir}/dags/static_data/countries/{country_code}.json"
    downloaded_data_path = f"{docker_worker_working_dir}/dags/static_data/downloaded_data"

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
        "country_geojson_filename": country_geojson_filename,
        "data_in_directory": data_in_directory,
        "data_out_directory": data_out_directory,
        "country_code": country_code,
        "country_name": country_name,
        "cmf_directory": cmf_directory,
        "downloaded_data_path": downloaded_data_path
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
        tags=["mapaction"],
    ) as dag:

        message['text'] = f"[{datetime.now()}]: {dag_id} ---- Dag has started running !!!"
        send_slack_message_task = send_slack_message(message)

        make_data_dirs_task = make_data_dirs(**task_args)
        ######################################
        ######## Task groups #################
        ######################################
        with TaskGroup(group_id="hdx_data_scrape") as hdx_data_scrape_group :
            with dag:
                download_population_with_sadd_inst = download_population_with_sadd(task_concurrency=3, **task_args)
                transform_population_with_sadd_inst = transform_population_with_sadd(task_concurrency=3, **task_args)
                download_hdx_admin_boundaries_inst = download_hdx_admin_boundaries(task_concurrency=3, **task_args)
                transform_hdx_admin_boundaries_inst = transform_hdx_admin_boundaries(task_concurrency=3, **task_args)
                download_population_with_sadd_inst >> transform_population_with_sadd_inst
                download_hdx_admin_boundaries_inst >> transform_hdx_admin_boundaries_inst
                
            for download_task in hdx_data_scrape_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        with TaskGroup(group_id="hdx_data_download") as hdx_data_download_group:
            with dag:
                download_all_hdx_country_data_types_inst = download_all_hdx_country_data_types(task_concurrency=3, **task_args)
                download_hdx_country_data_inst = download_hdx_country_data(task_concurrency=2, **task_args)
                download_all_hdx_country_data_types_inst >> download_hdx_country_data_inst

            for download_task in hdx_data_download_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        with TaskGroup(group_id="roads_populated_places") as roads_populated_places_group:
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
        with TaskGroup(group_id="water_features") as water_features_group:
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
        with TaskGroup(group_id="energy_infrastructure") as energy_infrastructure_group:
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
        with TaskGroup(group_id="elevation_terrain") as elevation_terrain_group:
            with dag:
                download_elevation90_inst = download_elevation90(task_concurrency=1, **task_args)
                download_gmdted250_inst = download_gmdted250(task_concurrency=1, **task_args)
                transform_elevation90_inst = transform_elevation90(task_concurrency=1, **task_args)
                transform_gmdted250_inst = transform_gmdted250(task_concurrency=1, **task_args)
                download_elevation90_inst >> transform_elevation90_inst
                download_gmdted250_inst >> transform_gmdted250_inst

            for download_task in elevation_terrain_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Administrative Boundaries
        with TaskGroup(group_id="admin_boundaries") as admin_boundaries_group:
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
        with TaskGroup(group_id="dams_reservoirs") as dams_reservoirs_group:
            with dag:
                # download_geodar_data_inst = download_geodar_data(task_concurrency=3, **task_args)
                # transform_dams_inst = transform_dams(task_concurrency=2, **task_args)
                # transform_reservoirs_inst = transform_reservoirs(task_concurrency=2, **task_args)

                # download_geodar_data_inst >> [transform_dams_inst, transform_reservoirs_inst]

                transform_geodar_catchment_data_inst = transform_geodar_catchment_data(task_concurrency=3, **task_args)
                transform_geodar_dam_data_inst = transform_geodar_dam_data(task_concurrency=3, **task_args)
                transform_geodar_reservoir_data_inst = transform_geodar_reservoir_data(task_concurrency=3, **task_args)

                transform_geodar_catchment_data_inst,
                transform_geodar_dam_data_inst,
                transform_geodar_reservoir_data_inst

            for download_task in dams_reservoirs_group:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Coastline
        with TaskGroup(group_id="coastline") as coastline_group:
            with dag:
                download_world_coastline_data_inst = download_world_coastline_data(task_concurrency=3, **task_args)
                transform_world_costline_data_inst = transform_world_costline_data(task_concurrency=2, **task_args)
                extract_country_national_coastline_inst = extract_country_national_coastline(task_concurrency=2, **task_args)

                download_world_coastline_data_inst >> transform_world_costline_data_inst >> extract_country_national_coastline_inst

            for download_task in coastline_group:
                download_task.trigger_rule = TriggerRule.ALL_DONE


        with TaskGroup(group_id=f"osm_tasks") as osm_group:
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

        with TaskGroup(group_id=f"other_download_tasks") as other_download_group:
            with dag:
                ######################################
                ######## Variable definitions ########
                ######################################
                oceans_and_seas_inst = oceans_and_seas(**task_args)
                hyrdrorivers_inst = hyrdrorivers(**task_args)
                download_healthsites_inst = healthsites(**task_args)
                download_worldpop1km_inst = worldpop1km(**task_args)
                download_worldpop100m_inst = worldpop100m(**task_args)
                transform_elevation90_inst = transform_elevation90(task_concurrency=1, **task_args)
                transform_gmdted250_inst = transform_gmdted250(task_concurrency=1, **task_args)
                oceans_and_seas_inst,
                hyrdrorivers_inst,
                download_healthsites_inst,
                download_worldpop1km_inst,
                download_worldpop100m_inst,
                transform_elevation90_inst,
                transform_gmdted250_inst

                for download_task in [oceans_and_seas_inst, hyrdrorivers_inst, download_healthsites_inst, download_worldpop1km_inst, download_worldpop100m_inst]:
                    download_task.trigger_rule = TriggerRule.ALL_DONE

        # Split export_tasks into two groups
        with TaskGroup(group_id=f"export_tasks") as export_group_1:
            with dag:
                mapaction_export_s3_task = mapaction_export_s3(**task_args)
                upload_cmf_all_task = upload_cmf_all(**task_args)
                upload_datasets_all_task = upload_datasets_all(**task_args)
                datasets_ckan_descriptions_task = datasets_ckan_descriptions(**task_args)
                create_completeness_report_task = create_completeness_report(**task_args)
                # Set dependencies for each export task
                for export_task in [mapaction_export_s3_task, upload_cmf_all_task, upload_datasets_all_task, datasets_ckan_descriptions_task, create_completeness_report_task]:
                    export_task.trigger_rule = TriggerRule.ALL_DONE

        with TaskGroup(group_id=f"export_intensive_tasks") as export_group_2:
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
        send_slack_message_task >> make_data_dirs_task >> elevation_terrain_group >> [
            hdx_data_download_group,
            water_features_group, 
            dams_reservoirs_group,
            coastline_group,
            hdx_data_scrape_group,
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
# Generate DAGs for all countries in the config
for country_name, config in configs.items():
    dag = create_mapaction_pipeline(country_name, config)
    globals()[dag.dag_id] = dag
