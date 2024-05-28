from .utils import make_dir_download_zip


def wfp_railroads(data_in_dir: str, data_out_dir: str):
    # TODO: This isn't working becuase the data link is depreciated. This might be the same dataset - https://data.humdata.org/dataset/global-railways
    download_url = "https://geonode.wfp.org/geoserver/wfs?format_options=charset%3AUTF-8&typename=geonode%3Awld_trs_railways_wfp&outputFormat=SHAPE-ZIP&version=1.0.0&service=WFS&request=GetFeature"
    download_location = f"{data_in_dir}/wfp_railroads"
    extract_location = f"{data_out_dir}/wfp_railroads"
    # make_dir_download_zip(download_url, download_location, extract_location)
    # TODO: The download link isn't working. I've asked on Slack if anyone has a copy.
    # TODO: extract via country code from shp here.
