import os

def ensure_unique_column_names(gdf):
    """
    Ensure unique column names by truncating if necessary (for shapefile limits).
    """
    new_columns = {}
    for col in gdf.columns:
        new_col = col[:10]
        counter = 1
        while new_col in new_columns.values():
            new_col = f"{col[:9]}{counter}"
            counter += 1
        new_columns[col] = new_col
    gdf.rename(columns=new_columns, inplace=True)
    return gdf

def unique_column_names(gdf):
        truncated_columns = {}
        final_columns = {}
        unique_suffixes = {}

        # Step 1: Truncate names
        for col in gdf.columns:
            truncated = col[:10]
            if truncated not in truncated_columns:
                truncated_columns[truncated] = 1
            else:
                truncated_columns[truncated] += 1
            final_columns[col] = truncated

        # Step 2: Resolve duplicates by adding a unique suffix
        for original, truncated in final_columns.items():
            if truncated_columns[truncated] > 1:
                if truncated not in unique_suffixes:
                    unique_suffixes[truncated] = 1
                else:
                    unique_suffixes[truncated] += 1
                suffix = unique_suffixes[truncated]
                suffix_length = len(str(suffix))
                truncated_with_suffix = truncated[:10-suffix_length] + str(suffix)
                final_columns[original] = truncated_with_suffix

        gdf.rename(columns=final_columns, inplace=True)
        return gdf

def save_data(gdf, output_filename):
    """
    Save GeoDataFrame to a shapefile.
    """
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    try:
        gdf.to_file(output_filename, driver='ESRI Shapefile')
        print(f"Data successfully saved to {output_filename}")
    except Exception as e:
        print(f"An error occurred while saving the GeoDataFrame: {e}")
