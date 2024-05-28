from .utils import make_dir_download_file


def ourairports(data_in_directory, data_out_directory):
    url = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    make_dir_download_file(url, data_in_directory + "/ourairports", "ourairports.csv")
