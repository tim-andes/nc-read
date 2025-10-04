import xarray as xr
import pandas as pd
import json 
import os
import sys

# --- GEOJSON CONFIGURATION ---
OUTPUT_DIR = 'data/geojson' 
SEATTLE_BBOX = {
    'west': -122.4,
    'south': 47.4,
    'east': -122.2,
    'north': 47.7
}

# Updated AOD variable candidates (same as checker)
AOD_VARIABLE_CANDIDATES = [
    # PACE OCI
    'Aerosol_Optical_Depth_550',
    
    # VIIRS (NOAA-20, NOAA-21, Suomi NPP)
    'COMBINE_AOD_550_AVG',
    'DT_AOD_550_AVG',
    'DB_AOD_550_AVG',
    'DT_DB_AOD_550_AVG',
    'DB_DT_AOD_550_AVG',
    
    # MODIS/Other
    'Optical_Depth_Land_And_Ocean',
    'AOD_550nm_Combined_Mean',
    'Aerosol_Optical_Depth_550nm_Mean',
    'AOD_550nm',
]
# --- END GEOJSON CONFIGURATION ---

MAX_ATTR_LENGTH = 120

def generate_output_filename(nc_filepath, satellite_type=""):
    """Creates a descriptive filename for the GeoJSON from the NetCDF name."""
    base_name = os.path.basename(nc_filepath)
    
    # Try to extract date
    date_str = "undated"
    
    # PACE format: PACE_OCI.20250702.L3m...
    if 'PACE_OCI.' in base_name:
        try:
            date_str = base_name.split('PACE_OCI.')[1].split('.')[0]
            satellite_type = "PACE"
        except IndexError:
            pass
    
    # VIIRS format: AER_DBDT_D10KM_L3_VIIRS_NOAA20.2025152...
    elif 'VIIRS' in base_name:
        try:
            # Extract Julian date (e.g., 2025152)
            parts = base_name.split('.')
            for part in parts:
                if part.startswith('2025') and len(part) == 7:
                    date_str = part
                    break
            
            if 'NOAA20' in base_name:
                satellite_type = "NOAA20"
            elif 'NOAA21' in base_name or 'JPSS2' in base_name:
                satellite_type = "NOAA21"
            elif 'NPP' in base_name:
                satellite_type = "NPP"
            else:
                satellite_type = "VIIRS"
        except:
            pass
    
    # Extract resolution if available
    resolution_str = ""
    if '0p1deg' in base_name or '0.1deg' in base_name:
        resolution_str = "_0p1deg"
    elif '1deg' in base_name:
        resolution_str = "_1deg"
    elif 'D10KM' in base_name:
        resolution_str = "_10km"
    
    return f"seattle_aod_{satellite_type}_{date_str}{resolution_str}.geojson"

def get_file():
    """Prompts user for file path and expands the home directory symbol (~)."""
    file_path = input("File path to .nc file (e.g., data/results_downloads/.../file.nc): ")
    file_path = os.path.expanduser(file_path)
    
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
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
            print(f"** {key}: {display_value}")
        else:
            print(f"   - {key}: {display_value}")

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
        print("\nWARNING: FILE APPEARS EMPTY OR CONTAINS NO DATA POINTS.")
        print("----------------------------------------------------------------")
        print("This often happens with Ocean Color (OC) products over land.")
        print("\nRecommendation: Use Aerosol products for urban air quality.")
        print("----------------------------------------------------------------")
    else:
        print(f"File contains {total_points:,} data points (based on first variable).")

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
        print(f"  Shape: {ds[var].shape}")
        print(f"  Dimensions: {ds[var].dims}")
        if 'long_name' in ds[var].attrs:
            print(f"  Description: {ds[var].attrs['long_name']}")
        if 'units' in ds[var].attrs:
            print(f"  Units: {ds[var].attrs['units']}")
    
    print("\n" + "="*60)
    print("COORDINATES:")
    print("="*60)
    for coord in ds.coords:
        print(f"- {coord}: {ds[coord].shape}")

def find_aod_variable(ds):
    """Dynamically find the AOD variable in the dataset."""
    for candidate in AOD_VARIABLE_CANDIDATES:
        if candidate in ds.data_vars:
            return candidate
    return None

def find_coordinates(ds):
    """Find latitude and longitude coordinates (handles different naming)."""
    lat_coord = None
    lon_coord = None
    
    for lat_name in ['lat', 'latitude', 'Latitude', 'LAT']:
        if lat_name in ds.coords:
            lat_coord = lat_name
            break
    
    for lon_name in ['lon', 'longitude', 'Longitude', 'LON']:
        if lon_name in ds.coords:
            lon_coord = lon_name
            break
    
    return lat_coord, lon_coord

def export_to_geojson(ds, nc_filepath):
    """
    Spatially filters the dataset to SEATTLE_BBOX and exports to GeoJSON.
    Works with both PACE and VIIRS data.
    """
    print("\nStarting GeoJSON export (Filtered by Seattle BBox)...")
    
    # Find AOD variable
    aod_variable = find_aod_variable(ds)
    if not aod_variable:
        print(f"Error: No AOD variable found in file.")
        print(f"Looked for: {AOD_VARIABLE_CANDIDATES}")
        print(f"Available variables: {list(ds.data_vars.keys())}")
        return
    
    print(f"Using AOD variable: {aod_variable}")
    
    # Find coordinates
    lat_coord, lon_coord = find_coordinates(ds)
    if not lat_coord or not lon_coord:
        print(f"Error: Could not find latitude/longitude coordinates")
        print(f"Available coordinates: {list(ds.coords.keys())}")
        return
    
    print(f"Using coordinates: {lat_coord}, {lon_coord}")
    
    metadata = {
        "source_product": ds.attrs.get('product_name', os.path.basename(nc_filepath)),
        "time_start": ds.attrs.get('time_coverage_start', ds.attrs.get('RangeBeginningDate', 'N/A')),
        "time_end": ds.attrs.get('time_coverage_end', ds.attrs.get('RangeEndingDate', 'N/A')),
        "variable_description": ds[aod_variable].attrs.get('long_name', 'Aerosol Optical Depth'),
        "variable_name": aod_variable
    }
    
    # Spatially filter the data
    print(f"Filtering data to BBox: {SEATTLE_BBOX}...")
    
    try:
        ds_filtered = ds.where(
            (ds[lat_coord] >= SEATTLE_BBOX['south']) & (ds[lat_coord] <= SEATTLE_BBOX['north']) &
            (ds[lon_coord] >= SEATTLE_BBOX['west']) & (ds[lon_coord] <= SEATTLE_BBOX['east']),
            drop=True
        )
    except Exception as e:
        print(f"Error during spatial filtering: {e}")
        return
    
    if aod_variable not in ds_filtered.data_vars or ds_filtered[aod_variable].size == 0:
        print("Warning: Filtered dataset has no data points in Seattle BBox.")
        return

    # Convert to DataFrame
    print("Converting filtered data to DataFrame...")
    df = ds_filtered[aod_variable].to_dataframe().dropna()
    df = df.rename(columns={aod_variable: 'aerosol_aod_550'})
    
    if df.empty:
        print("Warning: Filtered dataset contains only NaN values (no valid data).")
        return

    # Build GeoJSON
    print(f"Found {len(df)} valid data points. Generating GeoJSON...")
    
    geojson = {
        "type": "FeatureCollection",
        "metadata": metadata,
        "features": []
    }
    
    # Handle multi-index (lat, lon) or single index
    if isinstance(df.index, pd.MultiIndex):
        for idx, row in df.iterrows():
            # Extract lat/lon from multi-index
            if lat_coord in df.index.names and lon_coord in df.index.names:
                lat = idx[df.index.names.index(lat_coord)]
                lon = idx[df.index.names.index(lon_coord)]
            else:
                continue
            
            aerosol_value = row['aerosol_aod_550']
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)]
                },
                "properties": {
                    "aod": float(aerosol_value),
                    "lat": float(lat),
                    "lon": float(lon)
                }
            }
            geojson['features'].append(feature)
    else:
        # Single index - lat/lon might be columns
        for idx, row in df.iterrows():
            aerosol_value = row['aerosol_aod_550']
            lat = row.get(lat_coord, row.get('lat', None))
            lon = row.get(lon_coord, row.get('lon', None))
            
            if lat is None or lon is None:
                continue
            
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)]
                },
                "properties": {
                    "aod": float(aerosol_value),
                    "lat": float(lat),
                    "lon": float(lon)
                }
            }
            geojson['features'].append(feature)

    # Save GeoJSON
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except Exception as e:
        print(f"Error creating output directory '{OUTPUT_DIR}': {e}")
        return

    output_file_name = generate_output_filename(nc_filepath)
    output_file_path = os.path.join(OUTPUT_DIR, output_file_name)
    
    with open(output_file_path, 'w') as f:
        json.dump(geojson, f, indent=2)
    
    print("-------------------------------------------------------------")
    print(f"Success! GeoJSON saved to: {output_file_path}")
    print(f"   Total features exported: {len(geojson['features'])}")
    print(f"   Using AOD variable: {aod_variable}")
    print("-------------------------------------------------------------")

def save_data(ds, file_path):
    """Presents export options and performs the chosen export operation."""
    print("\n" + "="*60)
    print("EXPORT OPTIONS")
    print("="*60)
    print("1. Export entire dataset to CSV")
    print("2. Export entire dataset to Parquet")
    print("3. Export specific variable to CSV")
    print("4. Export dataset info to text file")
    print("5. Filter by geographic area (lat/lon) then export")
    print("6. Exit without saving")
    print("7. EXPORT AOD to GEOJSON (Filtered by Seattle BBox)")
    
    choice = input("\nEnter your choice (1-7): ")
    
    if choice == "7":
        export_to_geojson(ds, file_path)
    elif choice == "6":
        print("Exiting without saving.")
        return
    elif choice == "1":
        output_file = input("Output filename (e.g., output.csv): ")
        print("Converting to dataframe...")
        df = ds.to_dataframe().reset_index()
        df.to_csv(output_file, index=False)
        print(f"Saved to {output_file}")
    elif choice == "2":
        output_file = input("Output filename (e.g., output.parquet): ")
        print("Converting to dataframe...")
        df = ds.to_dataframe().reset_index()
        df.to_parquet(output_file, compression='snappy')
        print(f"Saved to {output_file}")
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
        print(f"Saved to {output_file}")
    elif choice == "4":
        output_file = input("Output filename (e.g., dataset_info.txt): ")
        with open(output_file, 'w') as f:
            f.write(str(ds))
        print(f"Dataset info saved to {output_file}")
    elif choice == "5":
        lat_coord, lon_coord = find_coordinates(ds)
        
        if not lat_coord or not lon_coord:
            print("Error: Could not find latitude/longitude coordinates")
            return
        
        print(f"\nFound coordinates: {lat_coord}, {lon_coord}")
        
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
        print(f"Saved filtered data to {output_file}")
    else:
        print("Invalid choice.")

def main():
    """Main execution flow for NetCDF file processing."""
    print("NetCDF File Explorer and Exporter")
    print("="*60)
    
    file_path = get_file()
    if not file_path:
        return
    
    print(f"\nOpening {file_path}...")
    
    try:
        ds = xr.open_dataset(file_path, decode_times=False)
        
        is_empty, total_points = pre_check_dataset(ds)
        display_info(ds)
        
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
        print("\nProcessing complete!")
        
    except FileNotFoundError:
        print("File not found.")
    except Exception as e:
        print(f"\nError processing file: {e}")

if __name__ == "__main__":
    main()