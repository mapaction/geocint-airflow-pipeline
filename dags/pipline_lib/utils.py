import io
import json
from logging import getLogger
import os
import zipfile

import requests
import shapefile

logger = getLogger(__name__)


def add_string_to_filename(filename: str, string_to_add: str) -> str:
    """Adds a string to the end of a filename string before the extension."""

    base, ext = os.path.splitext(filename)
    new_filename = base + string_to_add + ext
    return new_filename


def shepefile_to_geojson(shapefile_path: str, geojson_path: str) -> None:
    with shapefile.Reader(shapefile_path) as shp:
        fields = shp.fields[1:]
        field_names = [field[0] for field in fields]
        buffer = []
        for sr in shp.shapeRecords():
            atr = dict(zip(field_names, sr.record))
            geom = sr.shape.__geo_interface__
            buffer.append(dict(type="Feature", geometry=geom, properties=atr))

        print(buffer)
        # write the GeoJSON file
        geojson = open(geojson_path, "w")
        geojson.write(json.dumps({"type": "FeatureCollection", "features": buffer},
                                 indent=2, default=str) + "\n")
        geojson.close()


def make_dir_download_zip(download_url: str, download_location: str):
    """ Helper to download and extract a zip file. """
    logger.info(f"Downloading data to {download_location}")
    os.makedirs(download_location, exist_ok=True)
    response = requests.get(download_url)
    response.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall(download_location)
    print("////", os.listdir(download_location))
    logger.info(f"Download to {download_location} complete.")


def make_dir_download_file(download_url: str, download_location: str, filename: str):
    """ Helper to download a file. """
    logger.info(f"Downloading file data to {download_location}")
    os.makedirs(download_location, exist_ok=True)
    logger.info("Starting download...")
    response = requests.get(download_url)
    response.raise_for_status()
    with open(download_location + f"/{filename}", "wb") as f:
        f.write(response.content)
    logger.info(f"Download to {download_location} complete.")