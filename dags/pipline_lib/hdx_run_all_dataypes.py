import argparse
import os
from pipline_lib.hdx_country_data_download import download_country_data

def download_all_datatypes (country, country_code, data_types_file, input_folder, output_folder):
    """
    Reads data types from a file, downloads corresponding data for the country.
    """

    data_types_path = os.path.join(output_folder, 'hdx', data_types_file)

    if not os.path.exists(data_types_path):
        print(f"Error: File '{data_types_file}' not found for country {country}.")
        return

    with open(data_types_path, "r") as file:
        for data_type in file:
            data_type = data_type.strip()  # Remove newline character

            dir_name = "_".join(data_type.split(" "))
            dir_name = "_".join(dir_name.split("-"))

            # Construct destination folder
            dest_folder = os.path.join(output_folder, 'hdx', dir_name)
            os.makedirs(dest_folder, exist_ok=True)

            # Execute the download function
            try:
                download_country_data(country, data_type, dest_folder)
            except Exception as e:
                print(f"Error downloading {data_type} for {country}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download HDX data for a country based on data types in a file.")
    parser.add_argument("country", help="Name of the country (e.g., Mali)")
    parser.add_argument("country_code", help="iso3 code of the country (e.g., mli)")
    parser.add_argument(
        "--data_types_file", 
        default="hdx_datatypes.txt", 
        help="Name of the file containing data types (default: hdx_datatypes.txt)"
    )
    parser.add_argument(
        "--input_folder",
        default="/home/gis/air-cint/geocint-airflow-pipeline/data/input",
        help="Base folder for data and data type files (default: /home/gis/air-cint/geocint-airflow-pipeline/data/input/hdx)",
    )
    parser.add_argument(
        "--output_folder",
        default="/home/gis/air-cint/geocint-airflow-pipeline/data/output/country_extractions",
        help="Base folder for data and data type files (default: /home/gis/air-cint/geocint-airflow-pipeline/data/input/hdx)",
    )
    args = parser.parse_args()

    download_all_datatypes(args.country, args.country_code ,args.data_types_file, args.input_folder, args.output_folder)