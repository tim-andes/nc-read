import xarray as xr
import os
import numpy as np 

# --- CONSTANTS FOR SEATTLE METRO CHECK ---
# The tight BBox for Seattle Metro Area
SEATTLE_BBOX = {
    'west': -122.4,
    'south': 47.4,
    'east': -122.2,
    'north': 47.7
}

# Updated list with VIIRS variable names
AOD_VARIABLE_CANDIDATES = [
    # PACE OCI
    'Aerosol_Optical_Depth_550',
    
    # VIIRS (NOAA-20, NOAA-21, Suomi NPP) - Added
    'COMBINE_AOD_550_AVG',           # Combined DT+DB (BEST for general use)
    'DT_AOD_550_AVG',                # Dark Target
    'DB_AOD_550_AVG',                # Deep Blue
    'DT_DB_AOD_550_AVG',             # DT primary, DB secondary
    'DB_DT_AOD_550_AVG',             # DB primary, DT secondary
    
    # MODIS/Other
    'Optical_Depth_Land_And_Ocean',
    'AOD_550nm_Combined_Mean',
    'Aerosol_Optical_Depth_550nm_Mean',
    'AOD_550nm',
    'Deep_Blue_Aerosol_Optical_Depth_550_Land_Mean',
    'Dark_Target_Aerosol_Optical_Depth_550_Ocean_Mean',
]
# --- END CONSTANTS ---


def check_file_for_data(file_path):
    """
    Opens a NetCDF file, dynamically finds the AOD variable, filters it to 
    the tight Seattle BBox, and checks if it contains any non-NaN data points.
    """
    file_name = os.path.basename(file_path)
    
    try:
        # We use decode_times=False to handle potential time dimension errors gracefully
        with xr.open_dataset(file_path, decode_times=False) as ds:
            
            # 1. Dynamically find the required AOD variable
            aod_variable = None
            for candidate in AOD_VARIABLE_CANDIDATES:
                if candidate in ds.data_vars:
                    aod_variable = candidate
                    break
            
            if not aod_variable:
                print(f"   - FAIL: Missing required AOD variable. Checked for: {AOD_VARIABLE_CANDIDATES}")
                return False

            # 2. Identify coordinate names (handle both lowercase and uppercase)
            lat_coord = None
            lon_coord = None
            
            # Check for latitude coordinate
            for lat_name in ['lat', 'latitude', 'Latitude', 'LAT']:
                if lat_name in ds.coords:
                    lat_coord = lat_name
                    break
            
            # Check for longitude coordinate
            for lon_name in ['lon', 'longitude', 'Longitude', 'LON']:
                if lon_name in ds.coords:
                    lon_coord = lon_name
                    break
            
            if not lat_coord or not lon_coord:
                print(f"   - FAIL: Could not find latitude/longitude coordinates")
                print(f"      Available coords: {list(ds.coords.keys())}")
                return False

            # 3. Spatially filter data to the tight Seattle BBox
            # Use .where() with method='nearest' if grid is not perfectly aligned, 
            # but .where() is safer for filtering L3 global grids.
            ds_filtered = ds.where(
                (ds[lat_coord] >= SEATTLE_BBOX['south']) & (ds[lat_coord] <= SEATTLE_BBOX['north']) &
                (ds[lon_coord] >= SEATTLE_BBOX['west']) & (ds[lon_coord] <= SEATTLE_BBOX['east']),
                drop=True
            )

            # 4. Check for the existence of the AOD data variable after filtering
            if aod_variable not in ds_filtered.data_vars or ds_filtered[aod_variable].size == 0:
                print(f"   - FAIL: AOD variable {aod_variable} was dropped after BBox filter (no data in BBox).")
                return False

            aod_data = ds_filtered[aod_variable]
            
            # 5. Use robust counting (xarray's .count() ignores NaNs)
            valid_count = aod_data.count().item()
            
            if valid_count > 0:
                print(f"   - SUCCESS: Found {valid_count} non-NaN AOD points in Seattle BBox (Variable: {aod_variable}).")
                return True
            else:
                print("   - FAIL: BBox filtered but all data points were NaN (likely cloud/quality masked).")
                return False

    except FileNotFoundError:
        print(f"   Error: File not found at {file_path}")
        return False
    except ValueError as e:
        # This catches the 'zero-size array to reduction operation maximum' error robustly
        if "zero-size array to reduction operation" in str(e):
             print(f"   Warning: BBox filter resulted in zero valid data points for {aod_variable}. This is usually not a code error, but a lack of non-NaN data in the area.")
             return False
        # Catch other ValueErrors
        print(f"   Warning: Error reading {file_name}: {e}")
        return False
    except Exception as e:
        # Catch errors like file corruption or unsupported format
        print(f"   Warning: Error reading {file_name}: {e}")
        return False

def find_valid_seattle_files(directory="./data/results_downloads"):
    """
    Scans a directory for NetCDF (.nc) files and checks if they contain 
    valid (non-NaN) data points for the tight Seattle BBox.
    """
    target_dir = os.path.abspath(os.path.normpath(directory))
    
    print(f"Starting scan for valid Seattle Metro AOD data in: {target_dir}")
    print(f"   Target BBox: Lat {SEATTLE_BBOX['south']}-{SEATTLE_BBOX['north']}, Lon {SEATTLE_BBOX['west']}-{SEATTLE_BBOX['east']}")
    print("=" * 60)
    
    if not os.path.isdir(target_dir):
        print(f"Directory not found: {target_dir}. Please create it or check the path.")
        return

    valid_files = []
    
    for root, _, files in os.walk(target_dir):
        for file_name in files:
            if file_name.endswith(('.nc', '.nc4')):
                file_path = os.path.join(root, file_name)
                
                # Print which file is currently being checked
                print(f"\nChecking file: {os.path.relpath(file_path, start=target_dir).replace(os.sep, '/')}")
                
                if check_file_for_data(file_path):
                    valid_files.append(file_path)

    # Post-scan summary
    print("\n" + "=" * 60)
    print(f"Scan Complete. Total files with valid Seattle AOD data found: {len(valid_files)}")
    print("=" * 60)
    
    if valid_files:
        print("List of Valid Data Files (Clear Day Over Seattle):")
        for f in valid_files:
            # Display relative path
            display_path = os.path.relpath(f, start=target_dir).replace(os.sep, '/')
            print(f"- {display_path}")
    else:
        print("No NetCDF files were found with valid, non-cloud-masked AOD data for the tight Seattle Metro BBox.")

if __name__ == "__main__":
    find_valid_seattle_files()