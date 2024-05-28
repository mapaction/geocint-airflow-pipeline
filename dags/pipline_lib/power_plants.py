from .utils import make_dir_download_zip


def power_plants(data_in_dir: str, data_out_dir: str):
    download_url = "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"
    download_location = f"{data_in_dir}/power_plants"
    make_dir_download_zip(download_url, download_location)
