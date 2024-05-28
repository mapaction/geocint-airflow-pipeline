from .utils import make_dir_download_zip


def ne_10m_rivers_lake_centerlines(country_code: str, data_in_dir: str,
                                   data_out_dir: str):
    download_location = f"{data_in_dir}/ne_10m_rivers"
    url = "https://naciscdn.org/naturalearth/10m/physical/ne_10m_rivers_lake_centerlines.zip"
    make_dir_download_zip(url, download_location)
