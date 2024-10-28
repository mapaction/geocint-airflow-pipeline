import argparse
import os
import json
import shutil

def process_pop_sadd(iso3_code, data_dir, metadata_path, output_dir):
    """Renames CSV files in a directory based on metadata and a standardized format."""
    with open(metadata_path, "r") as f:
        metadata = json.load(f)
        full_source = metadata.get("Source", "unknown_source")  

    # Attempt to extract abbreviation (and convert to lowercase)
    abbreviation_parts = [word[0].lower() for word in full_source.split() if word[0].isupper()]  
    source = "".join(abbreviation_parts) if abbreviation_parts else full_source.split()[0].lower()
    time_period = metadata.get('Time Period of the Dataset [?]', '')
    date_range = time_period.split('-')
    # Extract the end year, searching in earlier parts if necessary
    end_date_parts = date_range[-1].split()
    end_year = 'Not Found'
    for part in reversed(end_date_parts):
        if part.isdigit():
            end_year = part
            break

    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            scale = filename.split("_")[2]  

            new_filename = f"{iso3_code}_popu_pop_tab_{scale}_{source.replace(' ', '_')}_{end_year}.csv"
            old_path = os.path.join(data_dir, filename)
            new_path = os.path.join(output_dir, new_filename)

            os.rename(old_path, new_path)
            print(f"Renamed: {filename} -> {new_filename}")

    # Copy metadata file to output directory
    try:
        output_metadata_path = os.path.join(output_dir, os.path.basename(metadata_path))  # Use original metadata filename
        shutil.copy2(metadata_path, output_metadata_path)  # Copy with metadata
        print(f"Copied metadata to {output_metadata_path}")
    except (IOError, os.error) as e:
        print(f"Error copying metadata: {e}")

if __name__ == "__main__":
    # Argument parsing for renaming functionality
    parser = argparse.ArgumentParser(description="Rename CSV files based on metadata.")
    parser.add_argument("iso3_code", help="The ISO3 code of the country (e.g., mli)")
    parser.add_argument("data_dir", help="Path to the directory containing the CSV and metadata files")
    parser.add_argument("metadata_path", help="Full path to the metadata JSON file")
    parser.add_argument("output_dir", help="Path to the output directory where renamed files will be saved")
    args = parser.parse_args()

    process_pop_sadd(args.iso3_code, args.data_dir, args.metadata_path, args.output_dir)
