import xarray as xr
import os
import pandas as pd

# --- CONSTANTS FOR SEATTLE METRO CHECK ---
# The tight BBox for Seattle Metro Area
SEATTLE_BBOX = {
    'west': -122.4,
    'south': 47.4,
    'east': -122.2,
    'north': 47.7
}
AEROSOL_VAR_NAME = 'Aerosol_Optical_Depth_550'
# --- END CONSTANTS ---


def check_file_for_data(file_path):
    """
    Opens a NetCDF file, filters it to the tight Seattle BBox, and checks 
    if the Aerosol_Optical_Depth_550 variable contains any non-NaN data points.

    Args:
        file_path (str): The full path to the NetCDF file.

    Returns:
        bool: True if valid (non-NaN) data points are found in the Seattle BBox, False otherwise.
    """
    try:
        # Note: os.path.basename is used for display purposes, which is fine on any OS.
        with xr.open_dataset(file_path, decode_times=False) as ds:
            
            # 1. Check if the required variable exists
            if AEROSOL_VAR_NAME not in ds.data_vars:
                print(f"   - FAIL: Missing variable {AEROSOL_VAR_NAME}")
                return False

            # 2. Filter data to the tight Seattle BBox using robust boolean indexing (.where)
            ds_filtered = ds.where(
                (ds.lat >= SEATTLE_BBOX['south']) & (ds.lat <= SEATTLE_BBOX['north']) &
                (ds.lon >= SEATTLE_BBOX['west']) & (ds.lon <= SEATTLE_BBOX['east']),
                drop=True
            )

            # 3. Check for valid (non-NaN) data points
            df_valid = ds_filtered[AEROSOL_VAR_NAME].to_dataframe().dropna()
            
            if not df_valid.empty:
                valid_count = len(df_valid)
                print(f"   - SUCCESS: Found {valid_count} non-NaN AOD points in Seattle BBox.")
                return True
            else:
                print("   - FAIL: BBox filtered but all data points were NaN (likely cloud/quality masked).")
                return False

    except FileNotFoundError:
        print(f"❌ Error: File not found at {file_path}")
        return False
    except Exception as e:
        # Catch errors like file corruption or unsupported format
        print(f"⚠️ Error reading {os.path.basename(file_path)}: {e}")
        return False

def find_valid_seattle_files(directory="./data"):
    """
    Scans a directory for NetCDF (.nc) files and checks if they contain 
    valid (non-NaN) data points for the tight Seattle BBox.
    """
    target_dir = os.path.abspath(os.path.normpath(directory))
    
    print(f"🔍 Starting scan for valid Seattle Metro AOD data in: {target_dir}")
    print(f"   Target BBox: Lat {SEATTLE_BBOX['south']}-{SEATTLE_BBOX['north']}, Lon {SEATTLE_BBOX['west']}-{SEATTLE_BBOX['east']}")
    print("=" * 60)
    
    if not os.path.isdir(target_dir):
        print(f"❌ Directory not found: {target_dir}. Please create it or check the path.")
        return

    valid_files = []
    
    for root, _, files in os.walk(target_dir):
        for file_name in files:
            if file_name.endswith(('.nc', '.nc4')):
                file_path = os.path.join(root, file_name)
                
                if check_file_for_data(file_path):
                    valid_files.append(file_path)
                    # FIX HERE: Replace backslashes with forward slashes for display
                    display_path = os.path.relpath(file_path, start=target_dir).replace(os.sep, '/')
                    print(f"✅ VALID SEATTLE DATA: {display_path}")
                else:
                    pass 

    print("\n" + "=" * 60)
    print(f"📊 Scan Complete. Total files with **valid Seattle AOD data** found: {len(valid_files)}")
    print("=" * 60)
    
    if valid_files:
        print("List of Valid Data Files (Clear Day Over Seattle):")
        for f in valid_files:
            # FIX HERE: Replace backslashes with forward slashes for display
            display_path = os.path.relpath(f, start=target_dir).replace(os.sep, '/')
            print(f"- {display_path}")
    else:
        print("No NetCDF files were found with valid, non-cloud-masked AOD data for the tight Seattle Metro BBox.")

if __name__ == "__main__":
    find_valid_seattle_files(directory="./data/results_downloads")