import zipfile
import os
import sys


def extract_data(zip_file, extract_dir, rename=None):
  """Extracts all files from a zip file to a specified directory with optional renaming of the first part (before extension).

  Args:
      zip_file: The path to the zip file.
      extract_dir: The directory where the extracted files will be saved.
      rename: A string to use as the new prefix for all extracted filenames (optional).

  Raises:
      OSError: If the zip file does not exist or there's an error creating the extraction directory.
  """
  
  # Check if zip file exists
  if not os.path.exists(zip_file):
    raise OSError(f"Error: Zip file '{zip_file}' does not exist.")

  # Create the extraction directory if it doesn't exist
  os.makedirs(extract_dir, exist_ok=True)

  # Open the zip file
  with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    extracted_files = []  # Keep track of extracted filenames (for unique renaming)

    # Extract all files from the zip
    for zip_info in zip_ref.infolist():
        # Get the original filename
        original_filename = os.path.basename(zip_info.filename)

        # Separate the filename and extension
        base, ext = os.path.splitext(original_filename)

        # Construct the new filename
        if rename:
            new_filename = f"{rename}{ext}"  # Use rename as prefix + original base + extension
        else:
            new_filename = original_filename  # No rename, keep original filename

        extracted_files.append(new_filename)

        zip_info.filename = new_filename
        print(f"Extracted file: {new_filename}")
        
        # Extract the file using zip_ref
        zip_ref.extract(zip_info, extract_dir)
        


if __name__ == "__main__":
  # Check for required arguments (at least 2)
  if len(sys.argv) < 3:
    print("Usage: python extraction.py <zip_file> <extract_dir> [rename]")
    sys.exit(1)

  # Get arguments from sys.argv
  zip_file = sys.argv[1]
  extract_dir = sys.argv[2]
  rename = None  # Optional rename

  # Check for optional rename argument
  if len(sys.argv) == 4:
    rename = sys.argv[3]

  extract_data(zip_file, extract_dir, rename)
  print("Extraction completed. Check logs for details.")
