import rasterio
import geopandas as gpd
from rasterio.mask import mask

from .utils import make_dir_download_file


def gmted2010(data_in_dir: str):
    """ Downloads the gmted2010 grid file and unzips it. """
    url = "https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/topo/downloads/GMTED/Grid_ZipFiles/be75_grd.zip"
    # make_dir_download_zip(url, f"{data_in_dir}/gmted2010.be75_grd.zip")
    make_dir_download_file(url, data_in_dir, "/gmted2010.be75_grd.zip")
    # TODO: currently broken - runs out of memory (exit code 9). Possibly because
    #  downloading large file (4.5gb) to inside container rather than toa volume?
    #  Otherwise, might just need to boost memory in Docker preferences.



def transform_gmted2010(data_in_dir: str, country_shapefile_name: str, data_out_loc: str):
    """ Extracts the gmted2010 dataset from the country outline polygon """
    # Define input and output paths
    arcgrid_path = f"{data_in_dir}/gmted2010.be75_grd.zip"
    polygon_path = country_shapefile_name
    output_path = data_out_loc

    # Read the ArcGrid data
    data = rasterio.open(arcgrid_path)
    profile = data.profile

    # Read the polygon data as a GeoDataFrame
    polygon_gdf = gpd.read_file(polygon_path)

    # Clip the data using polygon geometries
    clipped_data, clipped_transform = mask(data, polygon_gdf.geometry.values, crop=True)

    profile.update(
        driver="GTiff",
        height=clipped_data.shape[1],
        width=clipped_data.shape[2],
        count=1,
        dtype=clipped_data.dtype,
        transform=clipped_transform,
    )

    # Save the clipped data as a GeoTIFF
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(clipped_data)
