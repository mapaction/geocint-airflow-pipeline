import os

import pandas
import geopandas
import pendulum
from airflow import DAG
from airflow.decorators import dag, task
from dotenv import load_dotenv
from pipline_lib.countires_iso import COUNTRIES

load_dotenv()  # your .env should be in the /dags dir, not the project root.

# This is a way to create a different pipeline for every country, so they
# (hopefully) run in parallel. This is called "dynamic DAG generation" in the docs.
configs = COUNTRIES
# TODO: there's a bug in this config / the below for loop that isn't passing the 'code'
#  around correctly (is passing "moz" to both Afgan and Moz pipelines?)

S3_BUCKET = os.environ.get("S3_BUCKET")

def create_mapaction_pipeline(country_name, config):

    dag_id = f"dynamic_generated_dag_{country_name}"
    country_code = config['code']
    data_in_directory = f"data/input/{country_code}"
    data_out_directory = f"data/output/country_extractions/{country_code}"
    cmf_directory = f"data/cmfs/{country_code}"
    docker_worker_working_dir = "/opt/airflow"
    bash_script_path = f"{docker_worker_working_dir}/dags/scripts/bash"
    country_geojson_filename = f"{docker_worker_working_dir}/dags/static_data/countries/{country_code}.json"

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
        
        @task()
        def make_data_dirs():
            """ Development complete """
            from pipline_lib.make_data_dirs import make_data_dirs as make_dirs
            print("////", data_in_directory, data_out_directory, cmf_directory, dag_id)
            make_dirs(data_in_directory, data_out_directory, cmf_directory)

        @task()
        def download_hdx_admin_pop():
            """ Development complete """
            from pipline_lib.download_hdx_admin_pop import \
                download_hdx_admin_pop as download_pop

            print("////", data_in_directory, data_out_directory, cmf_directory)
            download_pop(country_code, data_in_directory)

        @task()
        def download_geodar_data():
            """ Development complete """
            from pipline_lib.download_geodar_data import download_shapefile_zip
            print("//// downloading geodar data in data/input/geodar")
            download_shapefile_zip()

        @task.bash()
        def transform_dams() -> str:
            """ Development complete """
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/geodar/dams/GeoDAR_v11_dams.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_dam_pt_s1_geodar_pp_dam"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"
        
        @task.bash()
        def transform_reservoirs() -> str:
            """ Development complete """
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/geodar/reservoirs/GeoDAR_v11_reservoirs.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_lak_py_s3_geodar_pp_reservoir"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"
        
        @task()
        def oceans_and_seas():
            from pipline_lib.extraction import extract_data
            extract_data("dags/static_data/oceans_and_seas.zip", 'data/output/world', 'wrl_phys_ocn_py_s0_marineregions_pp_oceans.shp')

        @task()
        def hyrdrorivers():
            """ Development complete """
            from pipline_lib.extraction import extract_data
            extract_data("dags/static_data/hydrorivers.zip", f'data/output/{country_code}', f'221_phys/{country_code}_phys_riv_ln_s1_hydrosheds_pp_rivers')
        
        @task()
        def download_world_admin_boundaries():
            """ Development complete """
            from pipline_lib.download_from_url import download_file
            download_file("https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/world-administrative-boundaries/exports/shp?lang=en&timezone=Europe%2FLondon", 'data/input/world_admin_boundaries')

        @task()
        def transform_world_admin_boundaries():
            from pipline_lib.extraction import extract_data
            extract_data("data/input/world_admin_boundaries", 'data/output/world', 'wrl_admn_ad0_ln_s0_wfp_pp_worldcountries')

        @task()
        def download_world_coastline_data():
            from pipline_lib.download_from_url import download_file
            download_file("https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/latest/gshhg-shp-2.3.7.zip", 'data/input/world_coastline')
        
        @task()
        def transform_world_costline_data():
            from pipline_lib.extraction import extract_data
            extract_data("data/input/world_coastline", 'data/output/world', 'wrl_elev_cst_ln_s0_un_pp_coastline')

        @task()
        def elevation():
            from pipline_lib.srtm_30m import download_srtm_30
            # download_srtm_30("Mozambique")  # Commented for development as slow to run
            # TODO: this downloads ?30m SRTM, but also need to download 90 and 250m
            # TODO: not currently doing any processing here. May need to grid and output

        @task()
        def gmted2010():
            """ https://www.usgs.gov/coastal-changes-and-impacts/gmted2010 """
            from pipline_lib.gmted2010 import gmted2010 as _gmted2010
            _gmted2010(data_in_directory)

        @task()
        def transform_gmted2010():
            """ https://www.usgs.gov/coastal-changes-and-impacts/gmted2010 """
            from pipline_lib.gmted2010 import transform_gmted2010 as _transform_gmted2010
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/232_tran/{country_code}_tran_rds_ln_s0_naturalearth_pp_roads"
            _transform_gmted2010(data_in_directory, country_geojson_filename, output_name)

        @task()
        def worldpop1km():
            """ Development complete """
            from pipline_lib.worldpop1km import worldpop1km as _worldpop1km
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _worldpop1km(country_code)

        @task()
        def worldpop100m():
            """ Development complete """
            from pipline_lib.worldpop100m import worldpop100m as _worldpop100m
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _worldpop100m(country_code)

        @task()
        def mapaction_export():
            # from pipline_lib.s3 import upload_to_s3, create_file
            # print("////", data_in_directory, data_out_directory, cmf_directory)
            # create_file()
            # upload_to_s3(s3_bucket=S3_BUCKET)
            pass

        @task()
        def ocha_admin_boundaries():
            """ Development complete """
            from pipline_lib.ocha_admin_boundaries import \
                ocha_admin_boundaries as _ocha_admin_boundaries

            print("////", data_in_directory, data_out_directory, cmf_directory)
            _ocha_admin_boundaries(country_code, data_in_directory, data_out_directory)

        @task()
        def healthsites():
            """ Development complete (extraction already done as API is by country) """
            from pipline_lib.process_healthsites import extract_and_download as _extract_and_download
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _extract_and_download(os.environ.get("HS_API_KEY"), country_geojson_filename)

        @task()
        def ne_10m_roads():
            """ Development complete """
            from pipline_lib.ne_10m_roads import ne_10m_roads as _ne_10m_roads
            print("////", data_in_directory, data_out_directory, cmf_directory)
            _ne_10m_roads(data_in_directory)

        @task.bash()
        def transform_ne_10m_roads() -> str:
            """ Usure if dev complete - outputs empty for Moz. """
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_roads/ne_10m_roads.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/232_tran/{country_code}_tran_rds_ln_s0_naturalearth_pp_roads"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"

        @task()
        def ne_10m_populated_places():
            """ Development complete """
            from pipline_lib.ne_10m_populated_places import ne_10m_populated_places as \
                _ne_10m_populated_places
            _ne_10m_populated_places(data_in_directory)
            # TODO: extract from shapefile

        @task.bash()
        def transform_ne_10m_populated_places() -> str:
            """ Development complete, but no output, so possible bugs """
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_populated_places/ne_10m_populated_places.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/229_stle/{country_code}_stle_stl_pt_s0_naturalearth_pp_maincities"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"

        @task()
        def ne_10m_rivers_lake_centerlines():
            """ Development complete """
            from pipline_lib.ne_10m_rivers_lake_centerlines import \
                ne_10m_rivers_lake_centerlines as _ne_10m_rivers_lake_centerlines
            _ne_10m_rivers_lake_centerlines(country_code, data_in_directory,
                                            data_out_directory)

        @task.bash()
        def transform_ne_10m_rivers_lake_centerlines() -> str:
            """ Development complete, but no features, so bgs?"""
            input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_lakes/ne_10m_lakes.shp"
            output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_riv_ln_s0_naturalearth_pp_rivers"
            return f"{bash_script_path}/mapaction_extract_country_from_shp.sh {country_geojson_filename} {input_shp_name} {output_name}"


        @task()
        def power_plants():
            """ Development complete """
            from pipline_lib.power_plants import power_plants as _power_plants
            _power_plants(data_in_directory, data_out_directory)

        @task()
        def transform_power_plants():
            """ Development complete """
            csv_filename = f"{data_in_directory}/power_plants/global_power_plant_database.csv"
            df = pandas.read_csv(csv_filename, low_memory=False)
            country_df = df[df["country"] == country_code.upper()]
            gdf = geopandas.GeoDataFrame(
                country_df, geometry=geopandas.points_from_xy(country_df.longitude, country_df.latitude)
            )
            output_dir = f"{docker_worker_working_dir}/{data_out_directory}/233_util"
            output_name_csv = f"{output_dir}/{country_code}_util_pst_pt_s0_gppd_pp_powerplants.csv"
            output_name_shp = f"{output_dir}/{country_code}_util_pst_pt_s0_gppd_pp_powerplants.shp"
            os.makedirs(output_dir, exist_ok=True)
            country_df.to_csv(output_name_csv)
            gdf.to_file(output_name_shp)


        @task()
        def wfp_railroads():
            from pipline_lib.wfp_railroads import wfp_railroads as _wfp_railroads
            _wfp_railroads(data_in_directory, data_out_directory)
            # TODO: haven't found any source for this file yet 🤷

        @task()
        def worldports():
            """ Development complete """
            from pipline_lib.worldports import worldports as _world_ports
            _world_ports(data_in_directory)


        @task()
        def transform_worldports():
            """ Development complete """
            csv_filename = f"{data_in_directory}/worldports/worldports.csv"
            df = pandas.read_csv(csv_filename, low_memory=False)
            country_df = df[df["Country Code"] == country_name.capitalize()]
            gdf = geopandas.GeoDataFrame(
                country_df, geometry=geopandas.points_from_xy(country_df.Longitude, country_df.Latitude)
            )
            print(gdf.head())
            output_dir = f"{docker_worker_working_dir}/{data_out_directory}/232_tran"
            output_name_csv = f"{output_dir}/{country_code}_tran_por_pt_s0_worldports_pp_ports.csv"
            output_name_shp = f"{output_dir}/{country_code}_tran_por_pt_s0_worldports_pp_ports.shp"
            os.makedirs(output_dir, exist_ok=True)
            country_df.to_csv(output_name_csv)
            gdf.to_file(output_name_shp)


        @task()
        def ourairports():
            """ Development complete """
            from pipline_lib.ourairports import ourairports as _ourairports
            _ourairports(data_in_directory, data_out_directory)

        @task()
        def transform_ourairports():
            """ Development complete """
            csv_filename = f"{data_in_directory}/ourairports/ourairports.csv"
            df = pandas.read_csv(csv_filename, low_memory=False)
            gdf = geopandas.GeoDataFrame(
                df, geometry=geopandas.points_from_xy(df.longitude_deg, df.latitude_deg)
            )
            # Use point inside polygon to select relevant rows
            country_poly = geopandas.read_file(country_geojson_filename)
            country_data = gdf[gdf.geometry.within(country_poly.geometry.iloc[0])]
            output_dir = f"{docker_worker_working_dir}/{data_out_directory}/232_tran"
            output_name_csv = f"{output_dir}/{country_code}_tran_air_pt_s0_ourairports_pp_airports.csv"
            output_name_shp = f"{output_dir}/{country_code}_tran_air_pt_s0_ourairports_pp_airports.shp"
            os.makedirs(output_dir, exist_ok=True)
            country_data.to_csv(output_name_csv)
            country_data.to_file(output_name_shp)

        @task()
        def ne_10m_lakes():
            from pipline_lib.ne_10m_lakes import ne_10m_lakes as _ne_10m_lakes
            _ne_10m_lakes(data_in_directory, data_out_directory)


        @task()
        def transform_ne_10m_lakes():
            shp_filename = data_in_directory + "/ne_10m_lakes/ne_10m_lakes.shp"
            print(shp_filename)
            gdf = geopandas.read_file(shp_filename, encoding='utf-8')
            print(gdf)
            country_poly = geopandas.read_file(country_geojson_filename)
            country_data = gdf[gdf.geometry.within(country_poly.geometry.iloc[0])]
            print("country data::")
            print(country_data)
            output_dir = f"{docker_worker_working_dir}/{data_out_directory}/221_phys"
            output_name_shp = f"{output_dir}/{country_code}_phys_lak_py_s0_naturalearth_pp_waterbodies"
            os.makedirs(output_dir, exist_ok=True)
            country_data.to_file(output_name_shp)
            # TODO: needs more testing - no features in output shapefile

        # osm layer targets
        @task()
        def osm_roads():
            from osm.layers.road_sub1_class import OSMRoadDataDownloader
            downloader = OSMRoadDataDownloader(country_geojson_filename, country_code)
            downloader.download_and_process_data()


        @task()
        def osm_railway():
            from osm.layers.railway_sub3_class import OSMRailwayDataDownloader
            downloader = OSMRailwayDataDownloader(country_geojson_filename, country_code)
            downloader.download_and_process_data()

        @task()
        def osm_dam():
            from osm.layers.dam_sub5_class import OSMDamDataDownloader
            downloader = OSMDamDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_school():
            from osm.layers.school_sub6_class import OSMSchoolDataDownloader
            downloader = OSMSchoolDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_education():
            from osm.layers.uni_sub7_class import OSMEducationDataDownloader
            downloader = OSMEducationDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_ferry():
            from osm.layers.ferry_sub8_class import OSMFerryTerminalDataDownloader
            downloader = OSMFerryTerminalDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_ferry_route():
            from osm.layers.ferry_sub9_class import OSMFerryRouteDataDownloader
            downloader = OSMFerryRouteDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_port():
            from osm.layers.port_sub10_class import OSMPortDataDownloader
            downloader = OSMPortDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_bank():
            from osm.layers.bank_sub11_class import OSMBankDataDownloader
            downloader = OSMBankDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_atm():
            from osm.layers.atm_sub12_class import OSMATMDataDownloader
            downloader = OSMATMDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_healthfacilities():
            from osm.layers.health_fac_sub13_class import OSMHealthDataDownloader
            downloader = OSMHealthDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_hospital():
            from osm.layers.hosp_sub14_class import OSMHospitalDataDownloader
            downloader = OSMHospitalDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_border_control():
            from osm.layers.border_control_sub18_class import OSMBorderControlDataDownloader
            downloader = OSMBorderControlDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_settlement():
            from osm.layers.settlement_sub19_class import OSMSettlementsDataDownloader
            downloader = OSMSettlementsDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_waterbodies():
            from osm.layers.waterbodies_sub27_class import OSMLakeDataDownloader
            downloader = OSMLakeDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_large_river():
            from osm.layers.large_river_sub28_class import OSMLargeRiverDataDownloader
            downloader = OSMLargeRiverDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_phys_river():
            from osm.layers.phy_river_sub29_class import OSMRiverDataDownloader
            downloader = OSMRiverDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_canal():
            from osm.layers.canal_sub30_class import OSMCanalDataDownloader
            downloader = OSMCanalDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()

        @task()
        def osm_railway2():
            from osm.layers.rail2_sub31_class import OSMRailwayStationDataDownloader
            downloader = OSMRailwayStationDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
            downloader.download_and_process_data()
            
        @task()
        def datasets_ckan_descriptions():
            pass

        @task()
        def cmf_metadata_list_all():
            pass

        @task()
        def upload_cmf_all():
            pass

        @task()
        def upload_datasets_all():
            pass

        @task()
        def create_completeness_report():
            pass

        @task()
        def send_slack_message():
            pass

        ######################################
        ######## Variable definitions ########
        ######################################
        # It's often easier to init a task here so you can
        # use it multiple times in the pipeline def below. If you only need to use it
        # once you can just call it directly.
        ne_10m_roads_inst = ne_10m_roads()
        ne_10m_populated_place_inst = ne_10m_populated_places()
        transform_ne_10m_roads_inst = transform_ne_10m_roads()
        transform_ne_10m_populated_places_inst = transform_ne_10m_populated_places()
        datasets_ckan_descriptions_inst = datasets_ckan_descriptions()
        ne_10m_rivers_lake_centerlines_inst = ne_10m_rivers_lake_centerlines()
        transform_ne_10m_rivers_lake_centerlines_inst = transform_ne_10m_rivers_lake_centerlines()
        power_plants_inst = power_plants()
        transform_power_plants_inst = transform_power_plants()
        worldports_inst = worldports()
        transform_worldports_inst = transform_worldports()
        ourairports_inst = ourairports()
        transform_ourairports_inst = transform_ourairports()
        ne_10m_lakes_inst = ne_10m_lakes()
        transform_ne_10m_lakes_inst = transform_ne_10m_lakes()
        download_geodar_data_inst = download_geodar_data()
        transform_dams_inst = transform_dams()
        transform_reservoirs_inst = transform_reservoirs()
        oceans_and_seas_inst = oceans_and_seas()
        hyrdrorivers_inst = hyrdrorivers()
        download_world_admin_boundaries_inst = download_world_admin_boundaries()
        transform_world_admin_boundaries_inst = transform_world_admin_boundaries()
        download_world_coastline_data_inst = download_world_coastline_data()
        transform_world_costline_data_inst = transform_world_costline_data()
        # gmted2010_inst = gmted2010()
        # transform_gmted2010_inst = transform_gmted2010()

        # OSM definitions
        osm_roads_inst = osm_roads()
        osm_railway_inst = osm_railway()
        osm_dam_inst = osm_dam()
        osm_school_inst = osm_school()
        osm_education_inst = osm_education()
        osm_ferry_inst = osm_ferry()
        osm_ferry_route_inst = osm_ferry_route()
        osm_port_inst = osm_port()
        osm_bank_inst = osm_bank()
        osm_atm_inst = osm_atm()
        osm_healthfacilities_inst = osm_healthfacilities()
        osm_hospital_inst = osm_hospital()
        osm_border_control_inst = osm_border_control()
        osm_settlement_inst = osm_settlement()
        osm_waterbodies_inst = osm_waterbodies()
        osm_large_river_inst = osm_large_river()
        osm_phys_river_inst = osm_phys_river()
        osm_canal_inst = osm_canal()
        osm_railway2_inst = osm_railway2()

        #####################################
        ######## Pipeline definition ########
        #####################################
        (
                make_data_dirs()  # Example of calling task directly in pipeline definition

                >>

                [
                 ne_10m_lakes_inst,  # Example of using an instance multiple times in the pipeline definition
                 ourairports_inst,
                 worldports_inst,
                 wfp_railroads(),
                 power_plants_inst,
                 ne_10m_rivers_lake_centerlines_inst,
                 ne_10m_populated_place_inst,
                 ne_10m_roads_inst,
                 healthsites(),
                 ocha_admin_boundaries(),
                 download_hdx_admin_pop(),
                 mapaction_export(),
                 worldpop1km(),
                 worldpop100m(),
                 elevation(),
                 download_geodar_data_inst,
                 oceans_and_seas_inst,
                 hyrdrorivers_inst,
                 download_world_admin_boundaries_inst,
                 download_world_coastline_data_inst,
                 # gmted2010_inst,


                # osm inst
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
                osm_railway2_inst,
                ]

                >>

                datasets_ckan_descriptions_inst

                >>

                [upload_datasets_all(), upload_cmf_all(), create_completeness_report()]

                >>

                send_slack_message()
        )

        # gmted2010_inst >> transform_gmted2010_inst
        ne_10m_lakes_inst >> transform_ne_10m_lakes_inst
        ourairports_inst >> transform_ourairports_inst
        ne_10m_roads_inst >> transform_ne_10m_roads_inst
        ne_10m_populated_place_inst >> transform_ne_10m_populated_places_inst
        ne_10m_rivers_lake_centerlines_inst >> transform_ne_10m_rivers_lake_centerlines_inst
        power_plants_inst >> transform_power_plants_inst
        worldports_inst >> transform_worldports_inst
        download_geodar_data_inst >> transform_dams_inst
        download_geodar_data_inst >> transform_reservoirs_inst
        download_world_admin_boundaries_inst >> transform_world_admin_boundaries_inst
        download_world_coastline_data_inst >> transform_world_costline_data_inst

        [#transform_gmted2010_inst,
         transform_ne_10m_lakes_inst,
         transform_ne_10m_roads_inst,
         transform_ourairports_inst,
         transform_ne_10m_populated_places_inst,
         transform_ne_10m_rivers_lake_centerlines_inst,
         transform_power_plants_inst,
         transform_worldports_inst,
         transform_dams_inst,
         transform_reservoirs_inst,
         transform_world_admin_boundaries_inst,
         transform_world_costline_data_inst] >> datasets_ckan_descriptions_inst


    return dag  # Required call to wrap up pipeline definition.


for country_name, config in configs.items():
    dag = create_mapaction_pipeline(country_name, config)
    globals()[dag.dag_id] = dag
  