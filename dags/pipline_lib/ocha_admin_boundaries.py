import glob
import json
import shutil

from .utils import shepefile_to_geojson


def ocha_admin_boundaries(country_code, data_in_directory, data_out_dir):
    """ Downloads the hdx admin boundaries. Based on download_hdx_admin_boundaries.sh.

    First, downloads the data list from https://data.humdata.org/api/3/action/package_show?id=cod-ab-$country_code
    Then iterates through object downloading and un-compressing each file.
    """
    import io
    import requests
    import os
    import zipfile

    from .utils import add_string_to_filename

    save_dir = f"{data_in_directory}/ocha_admin_boundaries"
    os.makedirs(save_dir, exist_ok=True)

    datalist_url = f"https://data.humdata.org/api/3/action/package_show?id=cod-ab-{country_code}"
    datalist_json = requests.get(datalist_url).json()
    download_urls: list[str] = [result['download_url'] for result in
                                datalist_json['result']['resources']]
    print(download_urls)
    # TODO: can speedup with asyncio/threading if needed
    for url in download_urls:
        save_location = f"{save_dir}/{url.split('/')[-1]}"
        response = requests.get(url)
        if url.endswith('.zip'):
            save_location = save_location.replace(".zip", "")
            z = zipfile.ZipFile(io.BytesIO(response.content))
            z.extractall(save_location)
        else:
            with open(save_location, 'wb') as f:
                f.write(response.content)
    print("final files", os.listdir(data_in_directory))
    # TODO: next, continue with the script with ogr2ogr stuff.

    # Return early if no spatial boundaries (e.g. Afghanistan)
    if not any([url.endswith(".zip") for url in download_urls]):
        return

    os.makedirs(f"{data_out_dir}/country_extractions/{country_code}/202_admn/",
                exist_ok=True)

    with open("dags/static_data/admin_level_display_names.json") as f:
        admin_level_display_data = json.load(f)

    # Process shapefiles
    for boundary_level in (0, 1, 2, 3):
        admin_boundary_level = f"adm{boundary_level}"
        print(f"#### Getting files for admin boundary level {admin_boundary_level}")
        for filename in glob.glob(f"{save_dir}/**/*{admin_boundary_level}*"):
            print("glob_filename: ", filename)
            display_name = admin_level_display_data.get(country_code, {}).get(f"{admin_boundary_level}",
                                                                              "") or f"adminboundary{boundary_level}"
            output_filename = add_string_to_filename(data_out_dir, display_name)
            shutil.copy2(filename, output_filename)

            if filename.endswith(".shp"):
                geojson_file_name = f"{output_filename}_{admin_boundary_level}.json"
                print("Generating", geojson_file_name)
                shepefile_to_geojson(filename, geojson_file_name)
