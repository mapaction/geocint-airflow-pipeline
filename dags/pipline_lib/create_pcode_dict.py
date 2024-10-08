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

def shapefile_processing(shapefile_path, dir_name, admin_level):
    global all_pcodes
    try:
        shapefile = gpd.read_file(shapefile_path)

        pcode_dict = {}
        for index, row in shapefile.iterrows():
            try: 
                # Construct column names
                pcode_col = f'ADM{admin_level}_PCODE' 
                admin_name_col = f'ADM{admin_level}_EN'
                admin_type_col = f"ADM{admin_level}_TYPE"
                country_name_col = "ADM0_EN"
                pcode = None

                # Simplified admin type check
                if admin_level == 0: 
                    admin_type = 'Country'
                else:
                    if admin_type_col in shapefile.columns:
                        admin_type = row[admin_type_col]
                        if admin_type is None:
                            admin_type = ""
                    else:
                        admin_type = ""

                if pcode_col in shapefile.columns:
                    pcode = row[pcode_col]

                    # Check if admin_name_col exists, if not, try _PT
                    if admin_name_col in shapefile.columns:
                        admin_level_name = row[admin_name_col]
                    elif f'ADM{admin_level}_PT' in shapefile.columns:
                        admin_name_col = f'ADM{admin_level}_PT'
                        admin_level_name = row[admin_name_col]
                    else: 
                        # If neither _EN nor _PT exists, set admin_level_name to empty
                        admin_level_name = ""

                    # Handle missing ADM0_EN
                    if country_name_col in shapefile.columns:
                        country_name = row[country_name_col]
                    else:
                        print(f"Warning: 'ADM0_EN' not found in {shapefile_path}. Using ISO3 directory name as country name.")
                        country_name = dir_name

                else:
                    # Try alternative columns
                    if 'pcode' in shapefile.columns:
                        pcode = row['pcode']
                        
                        # Check if admin_name_col.lower() exists, otherwise set to empty
                        if admin_name_col.lower() in shapefile.columns:
                            admin_level_name = row[admin_name_col.lower()]
                        else:
                            admin_level_name = ""

                        # Handle missing ADM0_EN in alternatives
                        if country_name_col.lower() in shapefile.columns:
                            country_name = row[country_name_col.lower()]
                        else:
                            print(f"Warning: 'ADM0_EN' not found in {shapefile_path}. Using ISO3 directory name as country name.")
                            country_name = dir_name
                    # Add more elif conditions for other alternatives if needed

                if pcode is not None:
                    pcode_dict[pcode] = {
                        'name': admin_level_name,
                        'admin_lvl': admin_level,
                        'country_iso': dir_name,
                        'country_name': country_name,
                        'admin_type': admin_type
                    }
                else:
                    print(f"Warning: Could not find a suitable pcode column in {shapefile_path}")

            except KeyError as e:
                print(f"Error processing {shapefile_path}: Missing column - {e}\n")
                # You can add further debugging actions here, like inspecting the shapefile
                # Example: print(shapefile.head()) to see the first few rows
                # Example: print(shapefile.columns) to see all column names
                # n for nextline
                # c for continue
                # CTRL + Z to exit 
                # import pdb; pdb.set_trace()
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

        for item in files + dirs:
            item_path = os.path.join(root, item)
            if os.path.isdir(item_path):
                print(f"Found subdirectory: {item_path}\n")
                for sub_root, sub_dirs, sub_files in os.walk(item_path):
                    for file in sub_files:
                        if file.endswith('.shp'):
                            for admin_level in range(4):
                                if f"adm{admin_level}" in file.lower():
                                    shapefile_path = os.path.join(sub_root, file)
                                    shapefile_processing(shapefile_path, item, admin_level)
                                    break
            elif item.endswith('.shp'):
                for admin_level in range(4):
                    if f"adm{admin_level}" in item.lower():
                        shapefile_path = os.path.join(root, item)
                        shapefile_processing(shapefile_path, os.path.basename(root), admin_level)
                        break

    # Save the final dictionary to pcodes.json in the output directory
    output_file = os.path.join(output_dir, 'pcodes.json')  # Construct output path
    with open(output_file, 'w') as f:
        json.dump(all_pcodes, f, indent=4)

    print(f"Processing complete. Pcodes saved to {output_file}")