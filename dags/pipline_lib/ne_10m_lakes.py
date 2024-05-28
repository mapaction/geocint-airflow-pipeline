from .utils import make_dir_download_zip


def ne_10m_lakes(data_in_directory, data_out_directory):
    url = "https://naciscdn.org/naturalearth/10m/physical/ne_10m_lakes.zip"
    make_dir_download_zip(url, data_in_directory + "/ne_10m_lakes")
