import ee
import os
import geopandas as gpd
import geemap
from shapely.geometry import box
from osgeo import gdal

class GMTEDDownloader:
    def __init__(self, country_geojson_filename, data_in_directory, data_out_directory):
        self.project_id = 'ma-ediakatos'
        self.country_geojson_filename = country_geojson_filename
        self.data_out_directory = data_out_directory
        self.data_in_directory = data_in_directory
        ee.Authenticate()
        ee.Initialize(project=self.project_id)

    def download_gmted_full(self):
        gdf = gpd.read_file(self.country_geojson_filename)
        country_name = os.path.splitext(os.path.basename(self.country_geojson_filename))[0].lower()
        geo_json_geometry = geemap.geojson_to_ee(gdf.__geo_interface__)
        gmted = ee.Image("USGS/GMTED2010_FULL")
        resolution = 250
        tile_split = 8
        gmted_clipped = gmted.clip(geo_json_geometry)
        country_input_dir = self.data_in_directory
        os.makedirs(country_input_dir, exist_ok=True)
        tiles = self.split_bbox(gdf.total_bounds, n=tile_split)

        for idx, tile in enumerate(tiles):
            coords = [[[p[0], p[1]] for p in tile.exterior.coords]]
            region = ee.Geometry.Polygon(coords)
            output_file = os.path.join(country_input_dir, f'{country_name}_tile_{idx+1}_gmted_{resolution}m.tif')
            try:
                geemap.ee_export_image(gmted_clipped, filename=output_file, scale=resolution, region=region, file_per_band=False)
                print(f"Successfully downloaded {output_file}")
            except Exception as e:
                print(f"Error downloading {output_file}: {e}")

    def process_files(self, data_in_directory):
        print(f"Processing files in directory: {data_in_directory}")
        iso_files = {}

        for filename in os.listdir(data_in_directory):
            if filename.endswith(".tif") and not filename.endswith(".aux.xml"):
                parts = filename.split('_')
                iso_code = parts[0]
                resolution = parts[-1].split('.')[0]

                if iso_code not in iso_files:
                    iso_files[iso_code] = {resolution: []}

                iso_files[iso_code][resolution].append(os.path.join(data_in_directory, filename))

        for iso_code, resolutions in iso_files.items():
            for resolution, files in resolutions.items():
                if files:
                    output_file = os.path.join(self.data_out_directory, f"{iso_code}_gmted_{resolution}.tif")
                    print(f"Merging files for {iso_code} at {resolution} resolution into {output_file}")
                    self.merge_files(files, output_file)

    @staticmethod
    def merge_files(input_files, output_file):
        print(f"Merging {len(input_files)} files into {output_file}")
        vrt_options = gdal.BuildVRTOptions(resolution='highest', separate=False)
        vrt = gdal.BuildVRT('/vsimem/temp.vrt', input_files, options=vrt_options)
        gdal.Translate(output_file, vrt)
        gdal.Unlink('/vsimem/temp.vrt')
        print(f"Finished merging files into {output_file}")

    @staticmethod
    def split_bbox(bbox, n):
        minx, miny, maxx, maxy = bbox
        dx = (maxx - minx) / n
        dy = (maxy - miny) / n
        bboxes = []
        for i in range(n):
            for j in range(n):
                bboxes.append(box(minx + i * dx, miny + j * dy, minx + (i + 1) * dx, miny + (j + 1) * dy))
        return bboxes
