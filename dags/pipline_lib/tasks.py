import os
import pandas
import geopandas
from airflow.decorators import task
from dotenv import load_dotenv
from webdav3.client import Client

load_dotenv("/opt/airflow/dags/.env")  # your .env should be in the /dags dir, not the project root.

S3_BUCKET = os.environ.get("S3_BUCKET")
HS_API_KEY = os.environ.get("HS_API_KEY")
SLACK_TOKEN = os.environ.get("SLACK_API_KEY")

###################################
######## Task Definitions #########
###################################
@task()
def make_data_dirs(**kwargs):
    """ Development complete """
    from pipline_lib.make_data_dirs import make_data_dirs as make_dirs
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    cmf_directory = kwargs['cmf_directory']
    print("////", data_in_directory, data_out_directory, cmf_directory,)
    make_dirs(data_in_directory, data_out_directory, cmf_directory)

@task()
def download_hdx_admin_pop(**kwargs):
    """ Development complete """
    from pipline_lib.download_hdx_admin_pop import \
        download_hdx_admin_pop as download_pop
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    cmf_directory = kwargs['cmf_directory']
    print("////", data_in_directory, data_out_directory, cmf_directory)
    download_pop(country_code, data_out_directory)


@task()
def download_geodar_data(**kwargs):
    from pipline_lib.download_geodar_data import download_shapefile_zip
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    doi = "https://doi.org/10.5281/zenodo.6163413"
    
    print("//// downloading geodar data in data/input/geodar")
    download_shapefile_zip("https://zenodo.org/records/6163413/files/GeoDAR_v10_v11.zip?download=1", "dams", "reservoirs", doi)



@task()
def download_railway_data(**kwargs):
    # """ Development complete """
    # TODO: need to find a suitable source for this 
    # from pipline_lib.download_from_url import download_file
    # download_file("http://geonode.wfp.org/geoserver/wfs?format_options=charset%3AUTF-8&typename=geonode%3Awld_trs_railways_wfp&outputFormat=SHAPE-ZIP&version=1.0.0&service=WFS&request=GetFeature&hdx=hdx", 'data/input/railway')
    pass

@task()
def download_boarder_crossings_data(**kwargs):
    # """ Development complete """
    # TODO: need to find a suitable source for this 
    # from pipline_lib.download_from_url import download_file
    # download_file("http://geonode.wfp.org/geoserver/wfs?format_options=charset%3AUTF-8&typename=geonode%3Awld_trs_railways_wfp&outputFormat=SHAPE-ZIP&version=1.0.0&service=WFS&request=GetFeature&hdx=hdx", 'data/input/railway')
    pass

@task()
def wfp_railroads(**kwargs):
    """ Development complete """
    #from pipline_lib.mapaction_extract_from_shp3 import clip_shapefile_by_country as _clip_by_country
    from pipline_lib.ma_extract_from_shp_utils import clip_shapefile_by_country_util as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    input_shp_name = f"{docker_worker_working_dir}/dags/static_data/railways/wrl_tran_rrd_ln_s2_wfp_pp_railways.shp"
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/232_tran/{country_code}_tran_rrd_ln_s2_wfp_pp_railways"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name, geometry_type="railway")

@task()
def wfp_boarder_crossings(**kwargs):
    """ Development complete """
    from pipline_lib.mapaction_extract_from_shp4 import clip_shapefile_by_country as _clip_by_country
    
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    input_shp_name = f"{docker_worker_working_dir}/dags/static_data/border_crossing/wrl_pois_bor_pt_s1_wfp_pp_bordercrossing.shp"
    os.mkdir(f"{docker_worker_working_dir}/{data_out_directory}/222_pois")
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/222_pois/{country_code}_pois_bor_pt_s1_wfp_pp_bordercrossing.shp"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name)

@task()
def download_population_with_sadd(**kwargs):
    """Development complete"""
    from pipline_lib.hdx_csv_and_zip_scraper import download_csv_and_zip_from_hdx as _scrape_hdx_data
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    _scrape_hdx_data(country_code, data_in_directory)

@task()
def transform_population_with_sadd(**kwargs):
    """ Development complete !!!!!! """
    from pipline_lib.process_pop_sadd import process_pop_sadd as _pop_sadd
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    out_dir = f"{data_out_directory}/223_popu"
    pop_input_dir = f"{data_in_directory}/population_with_sadd"
    metadata_file = f"{pop_input_dir}/{country_code}_metadata.json"
    _pop_sadd(country_code, pop_input_dir, metadata_file, out_dir)

@task()
def download_all_hdx_country_data_types(**kwargs):
    """ Development complete """
    from pipline_lib.hdx_list_country_datasets import list_and_save_data_types_with_metadata as _list_and_save
    # country_code = kwargs['country_code']
    country_name = kwargs['country_name']
    data_out_directory = kwargs["data_out_directory"]
    out_dir = f"{data_out_directory}/hdx"
    _list_and_save(country_name, out_dir)  

@task()
def download_hdx_country_data(**kwargs):
    """ Development complete """
    from pipline_lib.hdx_run_all_dataypes import download_all_datatypes as _download_all_types
    country_code = kwargs['country_code']
    country_name = kwargs['country_name']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    _download_all_types(country_name, country_code, "hdx_datatypes.txt", data_in_directory, data_out_directory)     

@task()
def transform_dams(**kwargs) -> str:
    from pipline_lib.mapaction_exctract_from_shp import clip_shapefile_by_country as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    input_shp_name = f"{docker_worker_working_dir}/data/input/geodar/dams/GeoDAR_v11_dams.shp"
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_dam_pt_s1_geodar_pp_dam"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name)


@task()
def transform_reservoirs(**kwargs) -> str:
    """ Development complete """
    from pipline_lib.mapaction_extract_from_shp2 import clip_shapefile_by_country as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    input_shp_name = f"{docker_worker_working_dir}/data/input/geodar/reservoirs/GeoDAR_v11_reservoirs.shp"
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_lak_py_s3_geodar_pp_reservoir"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name)

@task()
def oceans_and_seas(**kwargs):
    """ Development complete """
    from pipline_lib.extraction import extract_data
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    extract_data("dags/static_data/downloaded_data/oceans_and_seas.zip", 'data/output/world', 'wrl_phys_ocn_py_s0_marineregions_pp_oceans')

@task()
def hyrdrorivers(**kwargs) -> str:
    """ Development complete """
    from pipline_lib.extraction_hydrorivers import clip_shapefile_by_country as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    #input_shp_name = f"{docker_worker_working_dir}/data/hydrorivers/HydroRIVERS_v10.shp"
    input_shp_name = f"{docker_worker_working_dir}/data/hydro_america/america_clipped.shp"
    
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_riv_ln_s1_hydrosheds_pp_rivers"

    logging.info(f"Clipping HydroRIVERS line shapefile to AOI from {country_geojson_filename} for {country_code}...")

    _clip_by_country(country_geojson_filename, input_shp_name, output_name)
    logging.info(f"Clipped line shapefile saved to: {output_name}")
    return output_name


@task()
def download_world_admin_boundaries(**kwargs):
    """ Downloads the world admin boundaries data from the ArcGIS REST service and saves it as shapefiles. """
    import os
    import requests
    import geopandas as gpd
    from io import BytesIO
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    # Query the admin boundaries as GeoJSON from the ArcGIS REST service
    url = 'https://services3.arcgis.com/7J7WB6yJX0pYke9q/ArcGIS/rest/services/Admin_boundaries/FeatureServer/2/query'
    params = {
        'where': '1=1',  
        'outFields': '*',  
        'f': 'geojson'  
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        gdf = gpd.read_file(BytesIO(response.content))
        polygon_directory = os.path.join(docker_worker_working_dir, 'data/output/world')
        if not os.path.exists(polygon_directory):
            os.makedirs(polygon_directory)
        polygon_shp = os.path.join(polygon_directory, 'wrl_admn_ad0_py_s0_un_pp_worldcountries.shp')
        gdf.to_file(polygon_shp)
        print("Admin boundaries shapefile (polygons) saved successfully.")
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")

@task()
def transform_world_admin_boundaries(**kwargs):
    """ Transforms the downloaded world admin boundaries polygons into lines and saves them. """
    import os
    import geopandas as gpd
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    polygon_shp = os.path.join(docker_worker_working_dir, 'data/output/world', 'wrl_admn_ad0_py_s0_un_pp_worldcountries.shp')
    if not os.path.exists(polygon_shp):
        raise FileNotFoundError(f"Shapefile not found: {polygon_shp}")
    polygon_gdf = gpd.read_file(polygon_shp)
    line_gdf = polygon_gdf.copy()
    line_gdf['geometry'] = line_gdf.boundary  
    line_directory = os.path.join(docker_worker_working_dir, 'data/output/world')
    if not os.path.exists(line_directory):
        os.makedirs(line_directory)
    line_shp = os.path.join(line_directory, 'wrl_admn_ad0_ln_s0_un_pp_worldcountries.shp')
    line_gdf.to_file(line_shp)
    print("Admin boundaries shapefile (lines) saved successfully.")

@task()
def download_world_coastline_data(**kwargs):
    """ Development complete """
    from pipline_lib.download_from_url import download_file
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    #download_file("https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/latest/gshhg-shp-2.3.7.zip", 'data/input/world_coastline')

@task()
def transform_world_costline_data(**kwargs):
    """ Development complete """
    from pipline_lib.extraction import extract_data
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    #extract_data("data/input/world_coastline", 'data/output/world', 'wrl_elev_cst_ln_s0_un_pp_coastline')

@task()
def extract_country_national_coastline(**kwargs):
    """ Development complete """
    from pipline_lib.mapaction_exctract_from_shp import clip_shapefile_by_country as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    cmf_directory = kwargs['cmf_directory']
    input_shp_name = f"{docker_worker_working_dir}//data/output/world/wrl_elev_cst_ln_s0_un_pp_coastline.shp"
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/211_elev/{country_code}_elev_cst_ln_s0_un_pp_coastline"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name)

import logging

@task()
def extract_country_coastline_v2(**kwargs):
    """ Development complete """
    from geo_admin_tools.src.runner import main_method

    logging.info("Starting extract_country_coastline_v2 task")
    country_code = kwargs['country_code']
    country_name = kwargs.get('country_name', 'Unknown Country')
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    
    logging.info(f"Country code: {country_code}, Country name: {country_name}")
    logging.info(f"Docker working dir: {docker_worker_working_dir}, Data in directory: {data_in_directory}, Data out directory: {data_out_directory}")

    country_codes = [(country_code, country_name)]
    
    # Construct paths without unnecessary repetition
    data_in_path = os.path.join(docker_worker_working_dir, data_in_directory, "in")
    data_mid_path = os.path.join(docker_worker_working_dir, data_in_directory, "mid")
    data_out_path = os.path.join(docker_worker_working_dir, data_out_directory)

    
    logging.info(f"Data in path: {data_in_path}")
    logging.info(f"Data mid path: {data_mid_path}")
    logging.info(f"Data out path: {data_out_path}")

    # Pass paths correctly
    main_method(country_codes, data_in_path, data_mid_path, data_out_path)
    logging.info("Completed extract_country_coastline_v2 task")

@task()
def download_elevation90_hsh(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_90' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=False)
    if not os.path.exists(data_out_directory):
        os.makedirs(data_out_directory)
    downloader.download_srtm()

@task()
def transform_elevation90_hsh(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_90' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=False)
    downloader.process_files(data_in_directory)

@task()
def download_elevation90_dtm(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_90' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=False, is_hsh=False)
    downloader.download_srtm()

@task()
def transform_elevation90_dtm(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_90' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=False, is_hsh=False)
    downloader.process_files(data_in_directory)

@task(trigger_rule="all_done")
def download_elevation30_hsh(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_30' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=True)
    downloader.download_srtm()

@task()
def transform_elevation30_hsh(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_30' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=True)
    downloader.process_files(data_in_directory)

@task(trigger_rule="all_done")
def download_elevation30_dtm(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_30' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=True, is_hsh=False)
    downloader.download_srtm()

@task()
def transform_elevation30_dtm(**kwargs):
    """ Development complete """
    from pipline_lib.SRTMClass import SRTMDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'srtm_30' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = SRTMDownloader(country_geojson_filename, data_in_directory, data_out_directory, use_30m=True, is_hsh=False)
    downloader.process_files(data_in_directory)

@task()
def download_gmdted250_hsh(**kwargs):
    """ Development complete """
    from pipline_lib.GMDTEDClass import GMTEDDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'gmted_250' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = GMTEDDownloader(country_geojson_filename, data_in_directory, data_out_directory)
    downloader.download_gmted_full()

@task()
def download_gmdted250_dtm(**kwargs):
    """ Development complete """
    from pipline_lib.GMDTEDClass import GMTEDDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'gmted_250' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = GMTEDDownloader(country_geojson_filename, data_in_directory, data_out_directory, is_hsh=False)
    downloader.download_gmted_full()

@task()
def transform_gmdted250_hsh(**kwargs):
    """ Development complete """
    from pipline_lib.GMDTEDClass import GMTEDDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'gmted_250' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = GMTEDDownloader(country_geojson_filename, data_in_directory, data_out_directory)
    downloader.process_files(data_in_directory)

@task()
def transform_gmdted250_dtm(**kwargs):
    """ Development complete """
    from pipline_lib.GMDTEDClass import GMTEDDownloader
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = os.path.join(kwargs["data_in_directory"], 'gmted_250' )
    data_out_directory = os.path.join(kwargs["data_out_directory"], '211_elev') 
    downloader = GMTEDDownloader(country_geojson_filename, data_in_directory, data_out_directory, is_hsh=False)
    downloader.process_files(data_in_directory)

@task()
def create_feather_task(**kwargs):
    """ Development complete """
    from pipline_lib.FeatherClass import FeatherCreator
    data_out_directory = kwargs["data_out_directory"]
    feather_creator = FeatherCreator(data_out_directory)
    feather_creator.create_feathers()

@task()
def worldpop1km(**kwargs):
    """ Development complete """
    from pipline_lib.worldpop1km import worldpop1km as _worldpop1km
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    cmf_directory = kwargs['cmf_directory']
    print("////", data_in_directory, data_out_directory, cmf_directory)
    _worldpop1km(country_code)

@task()
def worldpop100m(**kwargs):
    """ Development complete """
    from pipline_lib.worldpop100m import worldpop100m as _worldpop100m
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    cmf_directory = kwargs['cmf_directory']
    print("////", data_in_directory, data_out_directory, cmf_directory)
    _worldpop100m(country_code)

@task()
def ocha_admin_boundaries(**kwargs):
    """ Development complete """
    from pipline_lib.ocha_admin_boundaries import \
        process_country_file as _ocha_admin_boundaries
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    cmf_directory = kwargs['cmf_directory']

    print("////", data_in_directory, data_out_directory, cmf_directory)

    _ocha_admin_boundaries(country_geojson_filename)

@task()
def transform_admin_linework(**kwargs):
    """ Development complete """
    from pipline_lib.admin_linework import process_admin_boundaries as _process_admin_boundaries
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    poly_dir = f"{docker_worker_working_dir}/{data_in_directory}/ocha_admin_boundaries/Shapefiles"
    output_dir = f"{docker_worker_working_dir}/{data_out_directory}"
    print("////", poly_dir, output_dir)
   # _process_admin_boundaries(country_code, poly_dir, output_dir)

@task()
def healthsites(**kwargs):
    """ Development complete """
    from pipline_lib.healthsities import download_shapefiles_from_page as _extract_and_download
    country_geojson_filename = kwargs['country_geojson_filename']
    data_out_directory = kwargs["data_out_directory"]
    country_name = kwargs["country_name"]
    country_code = kwargs['country_code']
    file_name = f"{country_code}_heal_hea_pt_s3_osm_pp_healthsites"
    out_dir = f"{data_out_directory}/215_heal"
    print("////", data_out_directory, country_name, file_name)
    _extract_and_download(country_name, out_dir, file_name)

@task()
def ne_10m_roads(**kwargs):
    """ Development complete """
    from pipline_lib.ne_10m_roads import ne_10m_roads as _ne_10m_roads
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    cmf_directory = kwargs['cmf_directory']
    print("////", data_in_directory, data_out_directory, cmf_directory)
    _ne_10m_roads(data_in_directory)

@task()
def transform_ne_10m_roads(**kwargs) -> str:
    """ Usure if dev complete"""
    from pipline_lib.mapaction_exctract_from_shp import clip_shapefile_by_country as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_roads/ne_10m_roads.shp"
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/232_tran/{country_code}_tran_rds_ln_s0_naturalearth_pp_roads"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name)

@task()
def ne_10m_populated_places(**kwargs):
    """ Development complete """
    from pipline_lib.ne_10m_populated_places import ne_10m_populated_places as \
        _ne_10m_populated_places
    data_in_directory = kwargs["data_in_directory"]
    _ne_10m_populated_places(data_in_directory)

@task()
def transform_ne_10m_populated_places(**kwargs) -> str:
    """ Development complete"""
    from pipline_lib.mapaction_exctract_from_shp import clip_shapefile_by_country as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_populated_places/ne_10m_populated_places.shp"
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/229_stle/{country_code}_stle_stl_pt_s1_naturalearth_pp_maincities"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name)

@task()
def ne_10m_rivers_lake_centerlines(**kwargs):
    """ Development complete """
    from pipline_lib.ne_10m_rivers_lake_centerlines import \
        ne_10m_rivers_lake_centerlines as _ne_10m_rivers_lake_centerlines
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    _ne_10m_rivers_lake_centerlines(country_code, data_in_directory,
                                    data_out_directory)

@task
def transform_ne_10m_rivers_lake_centerlines(**kwargs) -> str:
    """ Development complete"""
    from pipline_lib.mapaction_exctract_from_shp import clip_shapefile_by_country as _clip_by_country
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    input_shp_name = f"{docker_worker_working_dir}/{data_in_directory}/ne_10m_lakes/ne_10m_lakes.shp"
    output_name = f"{docker_worker_working_dir}/{data_out_directory}/221_phys/{country_code}_phys_riv_ln_s0_naturalearth_pp_rivers"
    _clip_by_country(country_geojson_filename, input_shp_name, output_name)
    
@task()
def power_plants(**kwargs):
    """ Development complete """
    from pipline_lib.power_plants import power_plants as _power_plants
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    _power_plants(data_in_directory, data_out_directory)

@task()
def transform_power_plants(**kwargs):
    country_code_lower = kwargs['country_code']
    country_code = kwargs['country_code'].upper()
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    csv_filename = f"{data_in_directory}/power_plants/global_power_plant_database.csv"

    try:
        df = pandas.read_csv(csv_filename, low_memory=False)
    except FileNotFoundError:
        print(f"Error: The file {csv_filename} was not found.")
        return
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        return

    country_df = df[df["country"].str.upper() == country_code]

    if country_df.empty:
        print(f"No data available for the specified country code ({country_code}) after filtering.")
        return

    country_df['longitude'] = pandas.to_numeric(country_df['longitude'], errors='coerce')
    country_df['latitude'] = pandas.to_numeric(country_df['latitude'], errors='coerce')
    country_df = country_df.dropna(subset=['longitude', 'latitude'])

    if country_df.empty:
        print("No valid geographic data available for the specified country after dropping NaNs.")
        return

    gdf = geopandas.GeoDataFrame(
        country_df,
        geometry=geopandas.points_from_xy(country_df.longitude, country_df.latitude),
        crs="EPSG:4326"
    )

    if not gdf.geometry.geom_type.eq('Point').all():
        print("Error: Not all geometries are points.")
        return

    output_dir = f"{docker_worker_working_dir}/{data_out_directory}/233_util"
    output_name_shp = f"{output_dir}/{country_code_lower}_util_pst_pt_s0_gppd_pp_powerplants.shp"

    os.makedirs(output_dir, exist_ok=True)

    gdf.to_file(output_name_shp, driver='ESRI Shapefile')
    print(f"Shapefile saved successfully to {output_name_shp}")


@task()
def worldports(**kwargs):
    """ Development complete """
    from pipline_lib.worldports import worldports as _world_ports
    data_in_directory = kwargs["data_in_directory"]
    _world_ports(data_in_directory)

@task()
def transform_worldports(**kwargs):
    country_name = kwargs['country_name']
    country_code = kwargs['country_code']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    csv_filename = f"{data_in_directory}/worldports/worldports.csv"
    df = pandas.read_csv(csv_filename, low_memory=False)
    country_df = df[df["Country Code"] == country_name.capitalize()]
    country_df['Longitude'] = pandas.to_numeric(country_df['Longitude'], errors='coerce')
    country_df['Latitude'] = pandas.to_numeric(country_df['Latitude'], errors='coerce')
    country_df = country_df.dropna(subset=['Longitude', 'Latitude'])
    if country_df.empty:
        print("No data available for the specified country after filtering. Exiting task.")
        return
    geometry = geopandas.points_from_xy(country_df.Longitude, country_df.Latitude)
    gdf = geopandas.GeoDataFrame(country_df, geometry=geometry, crs="EPSG:4326")
    if not gdf.geometry.geom_type.eq('Point').all():
        print("Error: Not all geometries are points.")
        return
    print(gdf.head())
    output_dir = f"{docker_worker_working_dir}/{data_out_directory}/232_tran"
    output_name_shp = f"{output_dir}/{country_code}_tran_por_pt_s0_worldports_pp_ports.shp"
    os.makedirs(output_dir, exist_ok=True)
    gdf.to_file(output_name_shp, driver='ESRI Shapefile')


@task()
def ourairports(**kwargs):
    """ Development complete """
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    from pipline_lib.ourairports import ourairports as _ourairports
    _ourairports(data_in_directory, data_out_directory)

@task()
def transform_ourairports(**kwargs):
    """ Development complete """
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    csv_filename = f"{data_in_directory}/ourairports/ourairports.csv"
    df = pandas.read_csv(csv_filename, low_memory=False)
    # Create a GeoDataFrame and set the CRS to WGS 84 (EPSG:4326)
    gdf = geopandas.GeoDataFrame(
        df,
        geometry=geopandas.points_from_xy(df.longitude_deg, df.latitude_deg),
        crs="EPSG:4326"  # Define the CRS as WGS 84
    )
    country_poly = geopandas.read_file(country_geojson_filename)
    country_data = gdf[gdf.geometry.within(country_poly.geometry.iloc[0])]
    output_dir = f"{docker_worker_working_dir}/{data_out_directory}/232_tran"
    output_name_shp = f"{output_dir}/{country_code}_tran_air_pt_s0_ourairports_pp_airports.shp"
    os.makedirs(output_dir, exist_ok=True)
    country_data.to_file(output_name_shp)

@task()
def ne_10m_lakes(**kwargs):
    """ Development complete """
    from pipline_lib.ne_10m_lakes import ne_10m_lakes as _ne_10m_lakes
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    _ne_10m_lakes(data_in_directory, data_out_directory)


@task()
def transform_ne_10m_lakes(**kwargs):
    """ Development complete """
    import geopandas as gpd
    import os
    import logging

    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    data_in_directory = kwargs["data_in_directory"]
    data_out_directory = kwargs["data_out_directory"]
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    shp_filename = data_in_directory + "/ne_10m_lakes/ne_10m_lakes.shp"

    # Define the function to check for intersecting features
    def check_intersecting_features(country_geojson_path, input_shp_path):
        try:
            country_gdf = gpd.read_file(country_geojson_path)
            input_gdf = gpd.read_file(input_shp_path)
            input_gdf = input_gdf.to_crs(country_gdf.crs)
            intersecting_gdf = gpd.sjoin(input_gdf, country_gdf, op='intersects')
            return not intersecting_gdf.empty
        except Exception as e:
            logging.error(f"Error during spatial intersection check: {e}")
            return False

    # Check for intersecting features
    if check_intersecting_features(country_geojson_filename, shp_filename):
        # Proceed if there are intersecting features
        gdf = gpd.read_file(shp_filename, encoding='utf-8')
        country_poly = gpd.read_file(country_geojson_filename)
        country_data = gdf[gdf.geometry.within(country_poly.geometry.iloc[0])]

        print("country data::")
        print(country_data)

        # Prepare output directory and filename
        output_dir = f"{docker_worker_working_dir}/{data_out_directory}/221_phys"
        output_name_shp = f"{output_dir}/{country_code}_phys_lak_py_s0_naturalearth_pp_waterbodies.shp"
        os.makedirs(output_dir, exist_ok=True)

        # Save the intersecting data to a new shapefile
        country_data.to_file(output_name_shp)
    else:
        # Skip output if no intersecting features are found
        print(f"No intersecting features found for country: {country_code}. Skipping file creation.")


# osm layer targets
@task()
def osm_roads(**kwargs):
    """ Development complete """
    from osm.layers.road_sub1_class import OSMRoadDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMRoadDataDownloader(country_geojson_filename, country_code)
    downloader.download_and_process_data()


@task()
def osm_railway(**kwargs):
    """ Development complete """
    from osm.layers.railway_sub3_class import OSMRailwayDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMRailwayDataDownloader(country_geojson_filename, country_code)
    downloader.download_and_process_data()

@task()
def osm_dam(**kwargs):
    """ Development complete """
    from osm.layers.dam_sub5_class import OSMDamDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMDamDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_school(**kwargs):
    """ Development complete """
    from osm.layers.school_sub6_class import OSMSchoolDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMSchoolDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_education(**kwargs):
    """ Development complete """
    from osm.layers.uni_sub7_class import OSMEducationDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMEducationDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_ferry(**kwargs):
    """ Development complete """
    from osm.layers.ferry_sub8_class import OSMFerryTerminalDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMFerryTerminalDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_ferry_route(**kwargs):
    """ Development complete """
    from osm.layers.ferry_sub9_class import OSMFerryRouteDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMFerryRouteDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_port(**kwargs):
    """ Development complete """
    from osm.layers.port_sub10_class import OSMPortDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMPortDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_bank(**kwargs):
    """ Development complete """
    from osm.layers.bank_sub11_class import OSMBankDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMBankDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_atm(**kwargs):
    """ Development complete """
    from osm.layers.atm_sub12_class import OSMATMDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMATMDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_healthfacilities(**kwargs):
    """ Development complete """
    from osm.layers.health_fac_sub13_class import OSMHealthDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    #downloader = OSMHealthDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    #downloader.download_and_process_data()

@task()
def osm_hospital(**kwargs):
    """ Development complete """
    from osm.layers.hosp_sub14_class import OSMHospitalDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMHospitalDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_border_control(**kwargs):
    """ Development complete """
    from osm.layers.border_control_sub18_class import OSMBorderControlDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMBorderControlDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    #downloader.download_and_process_data()

@task()
def osm_settlement(**kwargs):
    """ Development complete """
    from osm.layers.settlement_sub19_class import OSMSettlementsDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMSettlementsDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_waterbodies(**kwargs):
    """ Development complete """
    from osm.layers.waterbodies_sub27_class import OSMLakeDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMLakeDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_large_river(**kwargs):
    """ Development complete """
    from osm.layers.large_river_sub28_class import OSMLargeRiverDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMLargeRiverDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_phys_river(**kwargs):
    """ Development complete """
    from osm.layers.phy_river_sub29_class import OSMRiverDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMRiverDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_canal(**kwargs):
    """ Development complete """
    from osm.layers.canal_sub30_class import OSMCanalDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMCanalDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_railway2(**kwargs):
    """ Development complete """
    from osm.layers.rail2_sub31_class import OSMRailwayStationDataDownloader
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    downloader = OSMRailwayStationDataDownloader(country_geojson_filename,crs_project=4326,crs_global=4326, country_code=country_code)
    downloader.download_and_process_data()

@task()
def osm_pois(**kwargs):
    """Task for downloading and processing Points of Interest (POIs) data"""
    from osm.layers.pois_class import OSMPOIDataDownloader
    
    country_code = kwargs['country_code']
    country_geojson_filename = kwargs['country_geojson_filename']
    docker_worker_working_dir = kwargs['docker_worker_working_dir']
    
    downloader = OSMPOIDataDownloader(
        geojson_path=country_geojson_filename,
        crs_project=4326,
        crs_global=4326,
        country_code=country_code,
        docker_worker_working_dir=docker_worker_working_dir
    )
    
    downloader.download_and_process_data()

@task(trigger_rule="all_done")
def mapaction_export_s3(**kwargs):
    # from pipline_lib.s3 import upload_to_s3, create_file
    # print("////", data_in_directory, data_out_directory, cmf_directory)
    # create_file()
    # upload_to_s3(s3_bucket=S3_BUCKET)
    pass
    
@task(trigger_rule="all_done")
def datasets_ckan_descriptions(**kwargs):
    pass

@task(trigger_rule="all_done")
def cmf_metadata_list_all(**kwargs):
    pass

@task(trigger_rule="all_done")
def upload_cmf_all(**kwargs):
    pass

@task(trigger_rule='all_done')
def upload_datasets_all(**kwargs):
    # Define Nextcloud WebDAV options using environment variables
    options = {
        'webdav_hostname': os.getenv('WEBDAV_HOSTNAME'),
        'webdav_login': os.getenv('WEBDAV_LOGIN'),
        'webdav_password': os.getenv('WEBDAV_PASSWORD')
    }

    # Create a client instance
    client = Client(options)

    # Define source folder and target folder in Nextcloud
    source_folder = kwargs['data_out_directory']  # Local source folder
    country_code = kwargs['country_code']
    target_folder = f"DataPipeline/{country_code}"  # Remote target folder on Nextcloud

    def upload_directory(local_dir, remote_dir):
        # Ensure the remote directory exists
        if not client.check(remote_dir):
            client.mkdir(remote_dir)

        for item in os.listdir(local_dir):
            local_path = os.path.join(local_dir, item)
            remote_path = f"{remote_dir}/{item}"

            if os.path.isdir(local_path):
                # If the item is a directory, upload it recursively
                upload_directory(local_path, remote_path)
            else:
                # If the item is a file, upload it
                client.upload_sync(remote_path=remote_path, local_path=local_path)

    # Start the upload process
    upload_directory(source_folder, target_folder)

@task(trigger_rule="all_done")
def create_completeness_report(**kwargs):
    pass

@task(trigger_rule="all_done")
def send_slack_message(message):
    from pipline_lib.slack_message import send_slack_message as _ssm
    _ssm(SLACK_TOKEN, message)
