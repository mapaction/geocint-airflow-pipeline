import ee
import os
import geopandas as gpd
import geemap
from shapely.geometry import box
from osgeo import gdal
from google.oauth2 import service_account

class SRTMDownloader:
    def __init__(self, country_geojson_filename, data_in_directory, data_out_directory, use_30m=False, is_hsh=True):
        self.project_id = 'ma-ediakatos'
        self.country_geojson_filename = country_geojson_filename
        self.data_out_directory = data_out_directory  #os.path.join(data_out_directory, '211_elev')
        self.use_30m = use_30m
        self.data_in_directory = data_in_directory #os.path.join(data_in_directory, 'strm_30' if use_30m else 'srtm_90')
        #ee.Authenticate()
        #ee.Initialize(project=self.project_id)
        # Path to the service account JSON key file
        service_account_file = "/opt/airflow/dags/static_data/credentials.json"
        self.is_hsh = is_hsh
        self.data_type = 'hsh' if self.is_hsh else 'dtm'
        # Load service account credentials from the JSON key file
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=['https://www.googleapis.com/auth/cloud-platform']
        )

        # Set the environment variable
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file

        # Authenticate and initialize Earth Engine with the credentials
        ee.Initialize(credentials)


    def download_srtm(self):
        gdf = gpd.read_file(self.country_geojson_filename)
        country_name = os.path.splitext(os.path.basename(self.country_geojson_filename))[0].lower()
        geo_json_geometry = geemap.geojson_to_ee(gdf.__geo_interface__)
        srtm = ee.Image('USGS/SRTMGL1_003') if self.use_30m else ee.Image('CGIAR/SRTM90_V4')
        resolution = 30 if self.use_30m else 90
        tile_split = 32 if self.use_30m else 16
        srtm_clipped = srtm.clip(geo_json_geometry)
        srtm_hillside = ee.Terrain.slope(srtm)
        country_input_dir = self.data_in_directory
        os.makedirs(country_input_dir, exist_ok=True)
        tiles = self.split_bbox(gdf.total_bounds, n=tile_split)

        for idx, tile in enumerate(tiles):
            coords = [[[p[0], p[1]] for p in tile.exterior.coords]]
            region = ee.Geometry.Polygon(coords)
            output_file = os.path.join(country_input_dir, f'{country_name}_tile_{idx+1}_elev_{self.data_type}_ras_s0_srtm_pp_{resolution}m.tif')
            try:
                if self.is_hsh:
                    geemap.ee_export_image(srtm_hillside, filename=output_file, scale=resolution, region=region, file_per_band=False)
                    print(f"Successfully downloaded {output_file}")
                else:
                    geemap.ee_export_image(srtm_clipped, filename=output_file, scale=resolution, region=region, file_per_band=False)
                    print(f"Successfully downloaded {output_file}")
            except Exception as e:
                print(f"Error downloading {output_file}: {e}")
    # split this to two functions one for 30 and one for 90
    def process_files(self, data_in_directory):
        print(f"Processing files in directory: {data_in_directory}")
        iso_files = {}

        for filename in os.listdir(data_in_directory):
            if filename.endswith(".tif") and not filename.endswith(".aux.xml"):
                parts = filename.split('_')
                iso_code = parts[0]
                resolution = parts[-1].split('.')[0]

                if iso_code not in iso_files:
                    iso_files[iso_code] = {'30m': [], '90m': []}

                iso_files[iso_code][resolution].append(os.path.join(data_in_directory, filename))

        for iso_code, resolutions in iso_files.items():
            for resolution, files in resolutions.items():
                if files:
                    output_file = os.path.join(self.data_out_directory, f"{iso_code}_elev_{self.data_type}_ras_s0_srtm_pp_{resolution}.tif")
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