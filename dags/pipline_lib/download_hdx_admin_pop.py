import json

from .utils import make_dir_download_file


def download_hdx_admin_pop(country_code: str, data_out_dir: str) -> None:
    """ Downloads population data from hdx. """
    with open("dags/static_data/hdx_admin_pop_urls.json") as file:
        country_data_all_countries = json.load(file)
        data = [x for x in country_data_all_countries
                if x['country_code'] == country_code][0]
        url = data['val'][0]['download_url']
        filename = data['val'][0]['name']
        make_dir_download_file(url, data_out_dir + "/hdx_admin_pop", filename)
