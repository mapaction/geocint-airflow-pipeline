import json
import requests
import os

def download_hdx_admin_pop(country_code: str, data_out_dir: str) -> None:
    """Downloads population data from HDX for the given country code and saves it into a structured directory."""

    with open("dags/static_data/hdx_admin_pop_urls.json") as file:
        country_data_all_countries = json.load(file)
        country_data = next((x for x in country_data_all_countries if x['country_code'] == country_code), None)

        if not country_data:
            raise ValueError(f"Country code '{country_code}' not found in the data file.")

        for item in country_data['val']:
            download_url = item['download_url']
            filename = item['ma_file_name'] + ".csv"  # Ensure .csv extension
            
            layer_dir = os.path.join(data_out_dir, "223_popu")
            os.makedirs(layer_dir, exist_ok=True)  # Create level directory if it doesn't exist
            
            full_filename = os.path.join(layer_dir, filename)
            
            # Download and save the file
            response = requests.get(download_url)
            response.raise_for_status()  # Raise an error for bad responses
            with open(full_filename, 'wb') as file:
                file.write(response.content)

            print(f"Downloaded and saved: {full_filename}")
