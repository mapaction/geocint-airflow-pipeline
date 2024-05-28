from dataclasses import dataclass
from logging import getLogger
import os
import requests

from geopy.geocoders import Nominatim

logger = getLogger(__name__)


@dataclass
class CountryBBox:
    name: str
    max_lat: float
    min_lat: float
    lat_hemisphere: str
    max_lon: float
    min_lon: float
    lon_hemisphere: str


def download_srtm_tile(latitude: float, longitude: float, latitude_hemisphere: str,
                       longitude_hemisphere: str, output_dir: str):
    """ Downloads a specific srtm tile. """
    tile_url = f"https://e4ftl01.cr.usgs.gov/MEASURES/SRTMGL1.003/2000.02.11/{latitude_hemisphere}{str(latitude).zfill(2)}{longitude_hemisphere}{str(abs(longitude)).zfill(3)}.SRTMGL1.hgt.zip"
    print(tile_url)
    response = requests.get(tile_url, headers={
        "Authorization": f"Bearer {os.environ['NASA_TOKEN']}"})
    if response.status_code == 200:
        zip_file_path = os.path.join(output_dir,
                                     f"N{str(latitude).zfill(2)}E{str(abs(longitude)).zfill(3)}.zip")
        with open(zip_file_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded tile N{latitude} E{longitude}")
    else:
        print(f"Failed to download tile N{latitude} E{longitude}")
        print(response.content)


def download_country_srtm(bounding_box: CountryBBox):
    """ Takes a country bounding box, and loops over tiles to download them. """
    output_directory = f"srtm_tiles_{bounding_box.name.lower()}"
    os.makedirs(output_directory, exist_ok=True)

    # Loop over tiles and download
    smallest_lat = int(min(bounding_box.min_lat, bounding_box.max_lat))
    greatest_lat = int(max(bounding_box.min_lat, bounding_box.max_lat))
    smallest_lon = int(min(bounding_box.min_lon, bounding_box.max_lon))
    greatest_lon = int(min(bounding_box.min_lon, bounding_box.max_lon))
    for lat in range(smallest_lat, greatest_lat + 1):
        for lon in range(smallest_lon, greatest_lon + 1):
            download_srtm_tile(lat, lon, bounding_box.lat_hemisphere,
                               bounding_box.lon_hemisphere, output_directory)


def get_countrybbox(country_name) -> CountryBBox:
    """ Uses Nominatim geolocator to get a country's bounding box """
    geolocator = Nominatim(user_agent="bounding_box_finder")
    location = geolocator.geocode(country_name, timeout=10, exactly_one=True)

    if location:
        min_lat = float(location.raw['boundingbox'][0])
        max_lat = float(location.raw['boundingbox'][1])
        min_lon = float(location.raw['boundingbox'][2])
        max_lon = float(location.raw['boundingbox'][3])

        lat_direction = 'N' if max_lat > 0 else 'S'
        lon_direction = 'E' if max_lon > 0 else 'W'

        return CountryBBox(
            name=country_name,
            min_lat=abs(min_lat),
            max_lat=abs(max_lat),
            min_lon=abs(min_lon),
            max_lon=abs(max_lon),
            lat_hemisphere=lat_direction,
            lon_hemisphere=lon_direction
        )
    else:
        print("Country not found or geolocation service not available.")


def download_srtm_30(country_name: str):
    """ Main entrypoint of the file"""
    print("In entrypoint")
    # get dataclass of country bounding box
    bounding_box = get_countrybbox(country_name)
    # Download tiles
    download_country_srtm(bounding_box)
