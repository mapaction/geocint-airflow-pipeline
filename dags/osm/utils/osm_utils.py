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
