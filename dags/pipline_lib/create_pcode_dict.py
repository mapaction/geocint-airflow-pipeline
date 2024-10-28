import geopandas as gpd
import pycountry
import os
import json
# Initialize the final dictionary
all_pcodes = {}

def get_iso3(country_name):
    try:
        country = pycountry.countries.get(name=country_name)
        return country.alpha_3
    except AttributeError:
        print(f"Warning: Could not find ISO3 code for {country_name}")
        return None

def get_admin_columns(admin_gdf, adm_level):
    expected_admin_name_columns = [
        f'ADM{adm_level}_EN',
        f'ADM{adm_level}_NAME',
        f'ADM{adm_level}_DESCR',
        f'NAME_{adm_level}',
        'NAME_EN',
        'NAME',
        'ADMIN_NAME',
    ]
    expected_admin_type_columns = [
        f'ADM{adm_level}_TYPE',
        f'ADM{adm_level}_NAME',
        f'ADM{adm_level}_DESCR',
        f'NAME_{adm_level}',
        'NAME_EN',
        'NAME',
        'ADMIN_NAME',
    ]
    expected_pcode_columns = [
        f'ADM{adm_level}_PCODE',
        f'ADM{adm_level}_CODE',
        'P_CODE',
        'PCODE',
        'CODE',
    ]
    admin_name_columns = []
    pcode_column = None

    # Check for admin name columns (uppercase first, then lowercase)
    for col in expected_admin_name_columns:
        if col in admin_gdf.columns:
            admin_name_columns.append(col)
    if not admin_name_columns:  # If no uppercase columns found
        for col in [col.lower() for col in expected_admin_name_columns]:
            if col in admin_gdf.columns:
                admin_name_columns.append(col)

    # Check for pcode columns (uppercase first, then lowercase)
    for col in expected_pcode_columns:
        if col in admin_gdf.columns:
            pcode_column = col
            break
    if pcode_column is None:  # If no uppercase columns found
        for col in [col.lower() for col in expected_pcode_columns]:
            if col in admin_gdf.columns:
                pcode_column = col
                break

    return admin_name_columns, pcode_column


def shapefile_processing(shapefile_path, dir_name, admin_level):
    global all_pcodes
    try:
        shapefile = gpd.read_file(shapefile_path)

        admin_name_columns, pcode_column = get_admin_columns(shapefile, admin_level)

        if pcode_column is None:
            print(f"Warning: Could not find a suitable pcode column in {shapefile_path}")
            return

        pcode_dict = {}
        for index, row in shapefile.iterrows():
            try:
                pcode = row[pcode_column]
                
                if pcode is not None:
                    # Get the ISO2 code for the country (using the ISO3 code from dir_name)
                    iso3 = get_iso3(dir_name)
                    country = pycountry.countries.get(alpha_3=iso3)
                    iso2 = country.alpha_2 if country else None 

                    # Construct the expected column name for the country's language using ISO2
                    country_code_column = f"ADM{admin_level}_{iso2.upper()}" if iso2 else None
                    
                    # Prioritize 'ADM{admin_level}_EN' if present, otherwise use the country-specific column
                    if f'ADM{admin_level}_EN' in admin_name_columns:
                        name = row[f'ADM{admin_level}_EN']
                    elif iso2 and country_code_column in admin_name_columns:
                        name = row[country_code_column]
                    else:
                        # Fallback to the first available name column if neither _EN nor country-specific is found
                        name = row[admin_name_columns[0]] if admin_name_columns else None  

                    # Get admin type, checking for both uppercase and lowercase column names
                    admin_type_col = f"ADM{admin_level}_TYPE"
                    if admin_type_col in shapefile.columns:
                        admin_type = row[admin_type_col]
                    elif admin_type_col.lower() in shapefile.columns:
                        admin_type = row[admin_type_col.lower()]
                    else:
                        admin_type = ""

                    # Handle missing ADM0_EN, checking for both uppercase and lowercase column names
                    country_name_col = "ADM0_EN"
                    if country_name_col in shapefile.columns:
                        country_name = row[country_name_col]
                    elif country_name_col.lower() in shapefile.columns:
                        country_name = row[country_name_col.lower()]
                    else:
                        print(f"Warning: 'ADM0_EN' not found in {shapefile_path}. Using ISO3 directory name as country name.")
                        country_name = dir_name

                    pcode_dict[pcode] = {
                        'names': [name.lower()] if name else [],  # Store the selected name
                        'admin_lvl': admin_level,
                        'country_iso': dir_name,
                        'country_name': country_name,
                        'admin_type': admin_type
                    }
                else:
                    print(f"Warning: pcode is None for a row in {shapefile_path}")

            except KeyError as e:
                print(f"Error processing {shapefile_path}: Missing column - {e}\n")
                break

        print(f"Shapefile: {shapefile_path}")
        print(f"Pcodes found: {pcode_dict}\n")
        all_pcodes.update(pcode_dict)

    except Exception as e:
        print(f"Error processing {shapefile_path}: {e}\n")


def generate_pcode_json_file(shapefile_dir, output_dir):
    for root, dirs, files in os.walk(shapefile_dir):
        print(f"Checking directory: {root}")
        print(f"Items found: {files + dirs}")

        for file in files:
            if file.endswith('.shp'):
                for admin_level in range(4):
                    if f"adm{admin_level}" in file.lower():
                        shapefile_path = os.path.join(root, file)

                        # Determine country_iso based on directory structure
                        if os.path.basename(os.path.dirname(root)) == shapefile_dir:  # Directly under shapefile_dir
                            country_iso = os.path.basename(root)
                        else:  # Nested deeper
                            country_iso = os.path.basename(os.path.dirname(os.path.dirname(root)))

                        shapefile_processing(shapefile_path, country_iso, admin_level)
                        break

    # Save only the pcodes dictionary to JSON file
    output_file_pcodes = os.path.join(output_dir, 'pcodes.json')
    with open(output_file_pcodes, 'w') as f:
        json.dump(all_pcodes, f, indent=4)

    print(f"Processing complete. Pcodes saved to {output_file_pcodes}")

# Example usage (replace with your actual directory paths)
# shapefile_dir = 'path/to/your/shapefiles'
# output_dir = 'path/to/your/output/directory'
# generate_pcode_json_file(shapefile_dir, output_dir)