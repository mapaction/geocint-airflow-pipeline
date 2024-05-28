from .utils import make_dir_download_file


def worldports(data_in_dir: str):
    url = "https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv"
    download_location = f"{data_in_dir}/worldports"
    filename = "worldports.csv"
    make_dir_download_file(url, download_location, filename)
