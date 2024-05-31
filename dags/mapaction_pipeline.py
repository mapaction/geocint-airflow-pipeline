import pendulum
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag
from airflow.utils.trigger_rule import TriggerRule
from pipline_lib.tasks import *
from pipline_lib.countires_iso import COUNTRIES
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
    # bash_script_path = f"{docker_worker_working_dir}/dags/scripts/bash"
    country_geojson_filename = f"{docker_worker_working_dir}/dags/static_data/countries/{country_code}.json"

    task_args = {
        "docker_worker_working_dir": docker_worker_working_dir,
        "country_geojson_filename": country_geojson_filename,
        "data_in_directory": data_in_directory,
        "data_out_directory": data_out_directory,
        "country_code": country_code,
        "cmf_directory": cmf_directory,
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
        
        make_data_dirs_task = make_data_dirs(**task_args)
        
        ######################################
        ######## Task groups #################
        ######################################
        with TaskGroup(group_id=f"download_and_transform_tasks") as download_and_transform_group:
            with dag:
                ######################################
                ######## Variable definitions ########
                ######################################
                # It's often easier to init a task here so you can
                # use it multiple times in the pipeline def below. If you only need to use it
                # once you can just call it directly.
                ne_10m_roads_inst = ne_10m_roads(**task_args)
                ne_10m_populated_place_inst = ne_10m_populated_places(**task_args)
                ne_10m_rivers_lake_centerlines_inst = ne_10m_rivers_lake_centerlines(**task_args)
                power_plants_inst = power_plants(**task_args)
                worldports_inst = worldports(**task_args)
                ourairports_inst = ourairports(**task_args)
                ne_10m_lakes_inst = ne_10m_lakes(**task_args)
                download_geodar_data_inst = download_geodar_data(**task_args)
                oceans_and_seas_inst = oceans_and_seas(**task_args)
                hyrdrorivers_inst = hyrdrorivers(**task_args)
                download_world_admin_boundaries_inst = download_world_admin_boundaries(**task_args)
                download_world_coastline_data_inst = download_world_coastline_data(**task_args)
                ocha_admin_boundaries_inst = ocha_admin_boundaries(**task_args)
                download_elevation90_inst = download_elevation90(**task_args)
                download_elevation30_inst = download_elevation30(**task_args)
                download_gmdted250_inst = download_gmdted250(**task_args)
                download_healthsites_inst = healthsites(**task_args)
                download_worldpop1km_inst = worldpop1km(**task_args)
                download_worldpop100m_inst = worldpop100m(**task_args)

                ##################################
                ######## OSM definitions #########
                ##################################
                osm_roads_inst = osm_roads(**task_args)
                osm_railway_inst = osm_railway(**task_args)
                osm_dam_inst = osm_dam(**task_args)
                osm_school_inst = osm_school(**task_args)
                osm_education_inst = osm_education(**task_args)
                osm_ferry_inst = osm_ferry(**task_args)
                osm_ferry_route_inst = osm_ferry_route(**task_args)
                osm_port_inst = osm_port(**task_args)
                osm_bank_inst = osm_bank(**task_args)
                osm_atm_inst = osm_atm(**task_args)
                osm_healthfacilities_inst = osm_healthfacilities(**task_args)
                osm_hospital_inst = osm_hospital(**task_args)
                osm_border_control_inst = osm_border_control(**task_args)
                osm_settlement_inst = osm_settlement(**task_args)
                osm_waterbodies_inst = osm_waterbodies(**task_args)
                osm_large_river_inst = osm_large_river(**task_args)
                osm_phys_river_inst = osm_phys_river(**task_args)
                osm_canal_inst = osm_canal(**task_args)
                osm_railway2_inst = osm_railway2(**task_args)

                ######################################
                ######## Tranform definitions ########
                ######################################
                transform_ne_10m_roads_inst = transform_ne_10m_roads(**task_args)
                transform_ne_10m_populated_places_inst = transform_ne_10m_populated_places(**task_args)
                transform_ne_10m_rivers_lake_centerlines_inst = transform_ne_10m_rivers_lake_centerlines(**task_args)
                transform_power_plants_inst = transform_power_plants(**task_args)
                transform_worldports_inst = transform_worldports(**task_args)
                transform_ourairports_inst = transform_ourairports(**task_args)
                transform_gmdted250_inst = transform_gmdted250(**task_args)
                transform_feather_inst = create_feather_task(**task_args)
                transform_elevation30_inst = transform_elevation30(**task_args)
                transform_elevation90_inst = transform_elevation90(**task_args)
                transform_world_costline_data_inst = transform_world_costline_data(**task_args)
                extract_country_national_coastline_inst = extract_country_national_coastline(**task_args)
                transform_admin_linework_inst = transform_admin_linework(**task_args)
                transform_world_admin_boundaries_inst = transform_world_admin_boundaries(**task_args)
                transform_dams_inst = transform_dams(**task_args)
                transform_reservoirs_inst = transform_reservoirs(**task_args)
                transform_ne_10m_lakes_inst = transform_ne_10m_lakes(**task_args)

                oceans_and_seas_inst,
                hyrdrorivers_inst,
                download_healthsites_inst,
                download_worldpop1km_inst,
                download_worldpop100m_inst,
                osm_roads_inst,
                osm_railway_inst,
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
                osm_waterbodies_inst,
                osm_large_river_inst,
                osm_phys_river_inst,
                osm_canal_inst,
                osm_railway2_inst

                # Direct Download-Transform dependencies
                ne_10m_lakes_inst >> transform_ne_10m_lakes_inst
                ourairports_inst >> transform_ourairports_inst
                ne_10m_roads_inst >> transform_ne_10m_roads_inst
                ne_10m_populated_place_inst >> transform_ne_10m_populated_places_inst
                ne_10m_rivers_lake_centerlines_inst >> transform_ne_10m_rivers_lake_centerlines_inst
                power_plants_inst >> transform_power_plants_inst
                worldports_inst >> transform_worldports_inst
                download_world_admin_boundaries_inst >> transform_world_admin_boundaries_inst
                download_gmdted250_inst >> transform_gmdted250_inst
                download_elevation90_inst >> transform_elevation90_inst
                download_elevation30_inst >> transform_elevation30_inst

                # Download with Multiple Transform Outputs
                download_geodar_data_inst >> [transform_dams_inst, transform_reservoirs_inst]

                # Chained Transform Dependency
                download_world_coastline_data_inst >> transform_world_costline_data_inst >> extract_country_national_coastline_inst

                # Multiple Download Inputs to One Transform
                ocha_admin_boundaries_inst >> [transform_admin_linework_inst, transform_feather_inst]       

        with TaskGroup(group_id=f"export_tasks") as export_group:
            with dag:
                mapaction_export_s3_task = mapaction_export_s3(**task_args)
                upload_cmf_all_task = upload_cmf_all(**task_args)
                upload_datasets_all_task = upload_datasets_all(**task_args)
                datasets_ckan_descriptions_task = datasets_ckan_descriptions(**task_args)
                create_completeness_report_task = create_completeness_report(**task_args)
                send_slack_message_task = send_slack_message(**task_args)

                # Set dependencies for each export task
                for export_task in [mapaction_export_s3_task, upload_cmf_all_task, upload_datasets_all_task, datasets_ckan_descriptions_task, create_completeness_report_task, send_slack_message_task]:
                    download_and_transform_group >> export_task
                    export_task.trigger_rule = TriggerRule.ALL_DONE

         # Linking task groups
        make_data_dirs_task >> download_and_transform_group >> export_group

    return dag

# Generate DAGs for all countries in the config
for country_name, config in configs.items():
    dag = create_mapaction_pipeline(country_name, config)
    globals()[dag.dag_id] = dag
  