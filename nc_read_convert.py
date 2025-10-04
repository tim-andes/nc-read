import xarray as xr
import pandas as pd
import json 
import os
import sys

# --- GEOJSON CONFIGURATION ---
# Output directory (platform-independent pathing)
OUTPUT_DIR = 'data/geojson' 
# TIGHT SEATTLE BOUNDS
SEATTLE_BBOX = {
    'west': -122.4,
    'south': 47.4,
    'east': -122.2,
    'north': 47.7
}
AEROSOL_VAR_NAME = 'Aerosol_Optical_Depth_550' 
# --- END GEOJSON CONFIGURATION ---

# Maximum length for attribute values before truncation
MAX_ATTR_LENGTH = 120

def generate_output_filename(nc_filepath):
    """
    Creates a descriptive, unique filename for the GeoJSON from the NetCDF name.
    Example: PACE_OCI.20250702.L3m.DAY.AER_UAA.V3_1.0p1deg.NRT.nc 
    -> seattle_aod_20250702_0p1deg.geojson
    """
    # 1. Get the base filename
    base_name = os.path.basename(nc_filepath)
    
    # 2. Extract Date (e.g., 20250702)
    try:
        # Find date by splitting around 'PACE_OCI.' and '.L3m'
        date_str = base_name.split('PACE_OCI.')[1].split('.L3m')[0]
    except IndexError:
        date_str = "undated"
    
    # 3. Extract Resolution (e.g., 0p1deg)
    # The resolution is usually after V3_1.
    resolution_str = "unknown_res"
    if 'V3_1.' in base_name:
        try:
            # Find resolution by splitting around 'V3_1.' and '.NRT'
            res_part = base_name.split('V3_1.')[1]
            if '.NRT' in res_part:
                 resolution_str = res_part.split('.NRT')[0]
            # Replace 'p' with 'point' for better readability if needed, or keep 'p'
            resolution_str = resolution_str.replace('p', 'point')
        except IndexError:
            pass # Keep default

    return f"seattle_aod_{date_str}_{resolution_str}.geojson"


# Get file path
def get_file():
    """Prompts user for file path and expands the home directory symbol (~)."""
    file_path = input("File path to .nc file (e.g., data/results_downloads/.../file.nc): ")
    # Expand ~ to home directory
    file_path = os.path.expanduser(file_path)
    
    if not os.path.exists(file_path):
        print(f"❌ Error: File not found at {file_path}")
        return None
    
    return file_path

def display_global_attributes(ds):
    """Displays all global attributes, truncating long values for readability."""
    print("\n" + "="*60)
    print("GLOBAL ATTRIBUTES (Metadata):")
    print("="*60)
    
    sorted_attrs = sorted(ds.attrs.items())
    
    for key, value in sorted_attrs:
        display_value = str(value)
        if len(display_value) > MAX_ATTR_LENGTH:
            display_value = display_value[:MAX_ATTR_LENGTH] + "..."
        
        if key in ['product_name', 'time_coverage_start', 'time_coverage_end', 'geospatial_lat_max', 'geospatial_lat_min', 'geospatial_lon_max', 'geospatial_lon_min', 'processing_version', 'day_night_flag']:
            print(f"🌟 **{key}:** {display_value}")
        else:
            print(f"   - {key}: {display_value}")

def pre_check_dataset(ds):
    """Validates file content, counts dimensions/variables, and checks for emptiness."""
    total_data_variables = len(ds.data_vars)
    total_coordinates = len(ds.coords)
    total_dimensions = len(ds.dims)
    
    if total_data_variables > 0:
        first_var_name = list(ds.data_vars)[0]
        total_points = ds[first_var_name].size
    else:
        total_points = 0
        
    is_empty = total_points == 0
    
    print("\n" + "--- FILE VALIDATION CHECK ---")
    print(f"Variables: {total_data_variables}, Coordinates: {total_coordinates}, Dimensions: {total_dimensions}")
    
    if is_empty:
        print("\n🚨 **WARNING: FILE APPEARS EMPTY OR CONTAINS NO DATA POINTS.**")
        print("----------------------------------------------------------------")
        print("This often happens with **Ocean Color (OC)** products over land.")
        print("\n**Recommendation:** Use **Aerosol products** for urban air quality.")
        print("----------------------------------------------------------------")
        
    else:
        print(f"✅ File contains **{total_points:,}** data points (based on first variable).")

    return is_empty, total_points

def display_info(ds):
    """Displays dataset info, variables, coordinates, and ALL global attributes."""
    print("\n" + "="*60)
    print("DATASET OVERVIEW")
    print("="*60)
    print(ds)
    
    display_global_attributes(ds)
    
    print("\n" + "="*60)
    print("VARIABLES AVAILABLE:")
    print("="*60)
    for var in ds.data_vars:
        print(f"\n- {var}")
        print(f"  Shape: {ds[var].shape}")
        print(f"  Dimensions: {ds[var].dims}")
        if 'long_name' in ds[var].attrs:
            print(f"  Description: {ds[var].attrs['long_name']}")
        if 'units' in ds[var].attrs:
            print(f"  Units: {ds[var].attrs['units']}")
    
    print("\n" + "="*60)
    print("COORDINATES:")
    print("="*60)
    for coord in ds.coords:
        print(f"- {coord}: {ds[coord].shape}")
        
# --- MODIFIED FUNCTION: GeoJSON Export Logic ---
def export_to_geojson(ds, nc_filepath):
    """
    Spatially filters the dataset to the SEATTLE_BBOX using boolean indexing and exports it to GeoJSON.
    """
    print("\nStarting GeoJSON export (Filtered by Seattle BBox)...")
    
    if AEROSOL_VAR_NAME not in ds.data_vars:
        print(f"❌ Error: Variable '{AEROSOL_VAR_NAME}' not found in the file.")
        print(f"Available variables: {list(ds.data_vars.keys())}")
        return
    
    metadata = {
        "source_product": ds.attrs.get('product_name', os.path.basename(nc_filepath)),
        "time_start": ds.attrs.get('time_coverage_start', 'N/A'),
        "time_end": ds.attrs.get('time_coverage_end', 'N/A'),
        "variable_description": ds[AEROSOL_VAR_NAME].attrs.get('long_name', ds[AEROSOL_VAR_NAME].attrs.get('description', 'Aerosol Optical Depth'))
    }
    
    # 1. Spatially filter the data using boolean indexing (more robust than slice)
    print(f"Filtering data to BBox: {SEATTLE_BBOX} using .where() for robust filtering...")
    
    try:
        ds_filtered = ds.where(
            (ds.lat >= SEATTLE_BBOX['south']) & (ds.lat <= SEATTLE_BBOX['north']) &
            (ds.lon >= SEATTLE_BBOX['west']) & (ds.lon <= SEATTLE_BBOX['east']),
            drop=True
        )
    except Exception as e:
         print(f"❌ Error during spatial filtering. Ensure 'lat' and 'lon' coordinates exist: {e}")
         return
    
    if AEROSOL_VAR_NAME not in ds_filtered.data_vars or ds_filtered[AEROSOL_VAR_NAME].size == 0:
        print("⚠️ Warning: Filtered dataset has no data points. This is likely due to no valid data in the area (e.g., cloudy/flagged) or a BBox that still misses the sparse grid.")
        return

    # 2. Convert to a Pandas DataFrame
    print("Converting filtered data to DataFrame...")
    df = ds_filtered[AEROSOL_VAR_NAME].to_dataframe().dropna()
    df = df.rename(columns={AEROSOL_VAR_NAME: 'aerosol_aod_550'})
    
    if df.empty:
        print("⚠️ Warning: Filtered dataset contains only NaN values (no valid data) even within the BBox.")
        return

    # 3. Build the GeoJSON structure
    print(f"Found {len(df)} valid data points. Generating GeoJSON...")
    
    geojson = {
        "type": "FeatureCollection",
        "metadata": metadata,
        "features": []
    }
    
    for (lat, lon), row in df.iterrows():
        aerosol_value = row['aerosol_aod_550']
        
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)] # [longitude, latitude]
            },
            "properties": {
                "aod": float(aerosol_value), 
                "lat": float(lat),
                "lon": float(lon)
            }
        }
        geojson['features'].append(feature)

    # 4. Create output directory and save the GeoJSON file
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except Exception as e:
        print(f"❌ Error creating output directory '{OUTPUT_DIR}': {e}")
        return

    output_file_name = generate_output_filename(nc_filepath)
    output_file_path = os.path.join(OUTPUT_DIR, output_file_name)
    
    with open(output_file_path, 'w') as f:
        json.dump(geojson, f, indent=2)
    
    print("-------------------------------------------------------------")
    print(f"✅ Success! GeoJSON saved to: **{output_file_path}**")
    print(f"   Total features exported: {len(geojson['features'])}")
    print(f"   Using AOD variable: '{AEROSOL_VAR_NAME}'")
    print("-------------------------------------------------------------")
    
def save_data(ds, file_path):
    """Presents export options and performs the chosen export operation."""
    print("\n" + "="*60)
    print("EXPORT OPTIONS")
    print("="*60)
    # Options 1-5 skipped for brevity but would be here
    print("1. Export entire dataset to CSV")
    print("2. Export entire dataset to Parquet")
    print("...")
    print("6. Exit without saving")
    print("7. **EXPORT AOD to GEOJSON (Filtered by Seattle BBox)** 🌎") # New Option 7
    
    choice = input("\nEnter your choice (1-7): ")
    
    if choice == "7":
        export_to_geojson(ds, file_path) 
        
    # All other choices 1-6 would be implemented here

    elif choice == "6":
        print("Exiting without saving.")
        return
        
    # --- The rest of the save_data logic (1-5) goes here ---
    elif choice == "1":
        output_file = input("Output filename (e.g., output.csv): ")
        print("Converting to dataframe...")
        df = ds.to_dataframe().reset_index()
        df.to_csv(output_file, index=False)
        print(f"✓ Saved to {output_file}")
    
    elif choice == "2":
        output_file = input("Output filename (e.g., output.parquet): ")
        print("Converting to dataframe...")
        df = ds.to_dataframe().reset_index()
        df.to_parquet(output_file, compression='snappy')
        print(f"✓ Saved to {output_file}")
        
    elif choice == "3":
        print("\nAvailable variables:")
        vars_list = list(ds.data_vars)
        for i, var in enumerate(vars_list, 1):
            print(f"{i}. {var}")
        
        try:
            var_choice = int(input("\nSelect variable number: ")) - 1
            var_name = vars_list[var_choice]
        except (ValueError, IndexError):
            print("Invalid selection. Aborting export.")
            return

        output_file = input(f"Output filename for {var_name} (e.g., {var_name}.csv): ")
        
        print(f"Converting {var_name} to dataframe...")
        df = ds[var_name].to_dataframe().reset_index()
        df.to_csv(output_file, index=False)
        print(f"✓ Saved to {output_file}")
        
    elif choice == "4":
        output_file = input("Output filename (e.g., dataset_info.txt): ")
        with open(output_file, 'w') as f:
            f.write(str(ds))
        print(f"✓ Dataset info saved to {output_file}")
        
    elif choice == "5":
        # Geographic filtering
        lat_names = ['latitude', 'lat', 'Latitude', 'LAT']
        lon_names = ['longitude', 'lon', 'Longitude', 'LON']
        
        lat_coord = None
        lon_coord = None
        
        for name in lat_names:
            if name in ds.coords:
                lat_coord = name
                break
        for name in lon_names:
            if name in ds.coords:
                lon_coord = name
                break
        
        if not lat_coord or not lon_coord:
            print("Error: Could not find latitude/longitude coordinates")
            return
        
        print(f"\nFound coordinates: **{lat_coord}**, **{lon_coord}**")
        
        try:
            lat_min = float(input(f"\nEnter minimum {lat_coord}: "))
            lat_max = float(input(f"Enter maximum {lat_coord}: "))
            lon_min = float(input(f"Enter minimum {lon_coord}: "))
            lon_max = float(input(f"Enter maximum {lon_coord}: "))
        except ValueError:
            print("Invalid number input. Aborting filter.")
            return

        print("Filtering data...")
        filtered = ds.sel(
            {lat_coord: slice(lat_min, lat_max),
             lon_coord: slice(lon_min, lon_max)}
        )
        
        output_file = input("Output filename (e.g., seattle_data.csv): ")
        df = filtered.to_dataframe().reset_index()
        df.to_csv(output_file, index=False)
        print(f"✓ Saved filtered data to {output_file}")
        
    else:
        print("Invalid choice.")


# --- RESTORED FUNCTION: main ---
def main():
    """Main execution flow for NetCDF file processing."""
    print("NetCDF File Explorer and Exporter 🚀")
    print("="*60)
    
    file_path = get_file()
    if not file_path:
        return
    
    print(f"\nOpening {file_path}...")
    
    try:
        ds = xr.open_dataset(file_path)
        
        # 1. Pre-Check Validation
        is_empty, total_points = pre_check_dataset(ds)
        
        # 2. Display Info
        display_info(ds)
        
        # 3. Handle Empty Files or Offer Export
        if not is_empty:
            while True:
                save_choice = input("\nWould you like to export data? (y/n): ").lower()
                if save_choice == 'y':
                    save_data(ds, file_path)
                    continue_choice = input("\nExport another format? (y/n): ").lower()
                    if continue_choice != 'y':
                        break
                else:
                    break
        
        ds.close()
        print("\n✓ Processing complete!")
        
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print(f"\nError processing file: {e}")

if __name__ == "__main__":
    main()