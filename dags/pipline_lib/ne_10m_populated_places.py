from .utils import make_dir_download_zip


def ne_10m_populated_places(data_in_dir: str):
    url = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_populated_places.zip"
    make_dir_download_zip(url, data_in_dir + "/ne_10m_populated_places")
    # TODO: extract by country shape (in next BashOperator)
