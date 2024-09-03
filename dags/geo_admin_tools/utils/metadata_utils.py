import json
import numpy as np
from datetime import datetime
import os

metadata = {}

def update_metadata(iso_code, level, filepath):
    """Update metadata dictionary with processed file information."""
    if iso_code not in metadata:
        metadata[iso_code] = []

    # Convert NumPy types to native Python types
    if isinstance(level, (np.integer, int)):
        level = int(level)
    
    metadata[iso_code].append({
        "level": level,
        "filepath": filepath,
        "date_processed": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

def capture_metadata(base_download_directory):
    """Save metadata to a JSON file."""
    metadata_filepath = os.path.join(base_download_directory, "metadata.json")
    with open(metadata_filepath, 'w') as file:
        json.dump(metadata, file, indent=4)
    print(f"Metadata saved to {metadata_filepath}")
