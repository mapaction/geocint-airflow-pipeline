from .utils import make_dir_download_zip


def ne_10m_roads(data_in_dir: str) -> str:
    save_dir = f"{data_in_dir}/ne_10m_roads"
    download_url = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_roads.zip"
    make_dir_download_zip(download_url, save_dir)

    return save_dir + "/ne_10m_roads.shp"  # Might be able to pass this to next task
    # TODO: extract from .shp done in next task
