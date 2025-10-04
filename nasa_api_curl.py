import requests
import json
import os
from datetime import datetime, timedelta
import xarray as xr
from tqdm import tqdm
from urllib.parse import urlparse, unquote

try:
    import earthaccess
    EARTHACCESS_AVAILABLE = True
except ImportError:
    EARTHACCESS_AVAILABLE = False
    print("Warning: earthaccess not installed. Download functionality will be limited.")
    print("Install with: pip install earthaccess")

# --- DOTENV IMPORT FOR CREDENTIAL LOADING ---
try:
    from dotenv import load_dotenv, find_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
# --------------------------------------------

# --- DIRECTORY STRUCTURE DEFINITIONS ---
BASE_DATA_DIR = './data'
RESULTS_DIR = os.path.join(BASE_DATA_DIR, 'results')
DOWNLOADS_BASE_DIR = os.path.join(BASE_DATA_DIR, 'results_downloads')
# ----------------------------------------

# Seattle bounding box coordinates
SEATTLE_BBOX = {
    'west': -123.0,
    'south': 47.0,
    'east': -122.0,
    'north': 48.0
}

# NASA CMR API endpoints
CMR_COLLECTIONS_URL = "https://cmr.earthdata.nasa.gov/search/collections.json"
CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"

def display_satellite_info():
    """Display information about what each satellite measures for urban health"""
    print("\n" + "="*80)
    print("SATELLITE DATA FOR URBAN HEALTH & POLLUTION MONITORING")
    print("="*80)
    print("""
🛰️  AEROSOL & AIR QUALITY DATA (what this script searches):

PACE / OCI (2024-present)
  • Particulate Matter (PM2.5, PM10) - respiratory health hazards
  • Aerosol types - identifies pollution sources (traffic, industry, fires)
  • Air Quality Index indicators
  • Smoke and haze detection
  ✓ Best for: Detailed pollution source identification

NOAA-21 VIIRS (2022-present)
  • Aerosol Optical Depth (AOD) - overall air pollution levels
  • Fine particulate matter estimates
  • Smoke detection from wildfires
  • Urban air quality monitoring
  ✓ Best for: Current operational air quality data

NOAA-20 & Suomi NPP VIIRS (2017 & 2011-present)
  • Similar to NOAA-21 but with longer historical records
  • Good for trend analysis over time
  ✓ Best for: Long-term pollution trends
""")
    print("="*80)

def search_aerosol_collections():
    """Search for NOAA-21 VIIRS and PACE OCI aerosol collections"""
    print("Searching for aerosol data collections...")
    
    all_collections = []
    
    # Search queries for different satellites
    search_queries = [
        ('PACE OCI aerosol', 'PACE / OCI'),
        ('NOAA-21 VIIRS aerosol', 'NOAA-21 VIIRS'),
        ('JPSS-2 VIIRS aerosol', 'NOAA-21 VIIRS (JPSS-2)'),
        ('NOAA-20 VIIRS aerosol', 'NOAA-20 VIIRS'),
        ('Suomi NPP VIIRS aerosol', 'Suomi NPP VIIRS')
    ]
    
    print("\nSearching multiple satellite platforms...")
    
    for keyword, platform_name in search_queries:
        params = {
            'keyword': keyword,
            'page_size': 50
        }
        
        try:
            response = requests.get(CMR_COLLECTIONS_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            collections = data.get('feed', {}).get('entry', [])
            
            if collections:
                print(f"  ✓ Found {len(collections)} collection(s) for {platform_name}")
                # Tag each collection with the platform for easier identification
                for col in collections:
                    col['_platform'] = platform_name
                all_collections.extend(collections)
            else:
                print(f"  - No collections for {platform_name}")
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error searching {platform_name}: {e}")
    
    # Remove duplicates based on short_name
    seen_names = set()
    unique_collections = []
    for col in all_collections:
        short_name = col.get('short_name')
        if short_name and short_name not in seen_names:
            seen_names.add(short_name)
            unique_collections.append(col)
    
    return unique_collections

def filter_collections(collections):
    """Filters collections to only include L3 products and relevant aerosol products."""
    print("Applying filters: L3 + Aerosol (AER/AOD/AE) - Ocean Color (OC/CHL/CARBON)")
    filtered = []
    for col in collections:
        short_name = col.get('short_name', '').upper()
        
        # 1. Filter for L3
        is_l3 = 'L3' in short_name
        
        # 2. Filter for "Good" aerosol product (AER/AOD/AE)
        is_aerosol = any(term in short_name for term in ['AER', 'AOD', 'AE'])
        
        # 3. Filter out "Bad" ocean color products (OC/CHL/CARBON)
        is_ocean_color = any(term in short_name for term in ['OC', 'CHL', 'CARBON'])
        
        if is_l3 and is_aerosol and not is_ocean_color:
            filtered.append(col)
            
    print(f"  ✓ Filtered down to {len(filtered)} relevant collections.")
    return filtered

def display_collections_paginated(collections, page_size=25):
    """Display collections with pagination."""
    total = len(collections)
    start = 0
    
    print("\n" + "#"*80)
    print("## ✅ FILTERED L3 AEROSOL COLLECTIONS")
    print("#"*80)
    print(f"Showing {total} collections. These are filtered for **Level 3 (L3)** and **Aerosol** data.")
    print("---")
    
    while start < total:
        end = min(start + page_size, total)
        
        print(f"\nShowing collections {start + 1}-{end} of {total}:")
        print("="*80)
        
        for i in range(start, end):
            collection = collections[i]
            title = collection.get('title', 'No title')
            short_name = collection.get('short_name', 'N/A')
            platform = collection.get('_platform', 'Unknown')
            summary = collection.get('summary', 'No description')[:100]
            
            print(f"\n{i + 1}. [{platform}] {title}")
            print(f"  Short Name: **{short_name}**")
            print(f"  Description: {summary}...")
        
        start = end
        
        if start < total:
            choice = input(f"\nShow next {min(page_size, total - start)} collections? (y/n): ").lower()
            if choice != 'y':
                break
        
    return

def search_granules(short_name, start_date=None, end_date=None):
    """Search for data granules for a specific collection over Seattle"""
    
    # Default to last 7 days if no dates provided
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=7)
    
    # Format dates for CMR API
    temporal = f"{start_date.strftime('%Y-%m-%dT00:00:00Z')},{end_date.strftime('%Y-%m-%dT23:59:59Z')}"
    
    bbox_str = f"{SEATTLE_BBOX['west']},{SEATTLE_BBOX['south']},{SEATTLE_BBOX['east']},{SEATTLE_BBOX['north']}"
    
    params = {
        'short_name': short_name,
        'bounding_box': bbox_str,
        'temporal': temporal,
        'page_size': 100
    }
    
    print(f"\nSearching for granules over Seattle...")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Bounding box: {bbox_str}")
    
    try:
        response = requests.get(CMR_GRANULES_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        granules = data.get('feed', {}).get('entry', [])
        
        print(f"\nFound {len(granules)} granules")
        return granules
    
    except requests.exceptions.RequestException as e:
        print(f"Error searching granules: {e}")
        return []

def display_granule_info(granules):
    """Display information about found granules"""
    if not granules:
        print("\nNo granules found for this search.")
        return []
    
    print("\n" + "="*80)
    print("AVAILABLE DATA GRANULES:")
    print("="*80)
    
    granule_list = []
    
    for i, granule in enumerate(granules, 1):
        title = granule.get('title', 'No title')
        time_start = granule.get('time_start', 'N/A')
        granule_id = granule.get('id', 'N/A')
        
        # Get file size if available
        size_mb = None
        for link in granule.get('links', []):
            if 'inherited' in link and link['inherited'] == False:
                size_mb = link.get('length', 0) / (1024 * 1024) # Convert to MB
                break
        
        # Get download links
        links = granule.get('links', [])
        download_links = [link['href'] for link in links if link.get('rel') == 'http://esipfed.org/ns/fedsearch/1.1/data#']
        
        granule_info = {
            'index': i,
            'title': title,
            'id': granule_id,
            'time': time_start,
            'size_mb': size_mb,
            'download_url': download_links[0] if download_links else None
        }
        granule_list.append(granule_info)
        
        print(f"\n{i}. {title}")
        print(f"  Time: {time_start}")
        if size_mb:
            print(f"  Size: {size_mb:.2f} MB")
        if download_links:
            print(f"  Download URL: {download_links[0][:80]}...")
    
    return granule_list

def get_granule_id_from_url(file_path):
    """Extracts the granule ID from the downloaded file path for logging."""
    return os.path.basename(file_path)

def check_file_contents(file_path):
    """Check if NetCDF file has actual data (not a placeholder)"""
    file_id = get_granule_id_from_url(file_path)
    print(f"\nChecking file: **{file_id}**")
    
    try:
        # Use decode_times=False to avoid time parsing errors in large datasets
        ds = xr.open_dataset(file_path, decode_times=False)
        
        num_vars = len(ds.data_vars)
        
        # Check for the Ocean Color signature: no data variables over land
        if num_vars == 0:
            print("  ❌ WARNING: File has **zero** data variables. This confirms it's a **placeholder**.")
            print("  Reason: Likely an **Ocean Color (OC)** product filtered over land.")
            ds.close()
            return False
            
        print(f"  ✅ Variables found: {num_vars}")
        
        # Check if the primary variable is empty (e.g., all NaNs or size 1)
        is_valid = False
        for var_name in ds.data_vars:
            # Skip coordinate variables if they sneak into data_vars
            if var_name in ds.coords:
                continue
            
            # Check if the variable size is greater than 1 (to filter out scalar variables)
            if ds[var_name].size > 1:
                is_valid = True
                print(f"  - Primary variable **{var_name}** with shape {ds[var_name].shape} found.")
                break # Found a valid variable, no need to check others

        ds.close()
        return is_valid
        
    except Exception as e:
        print(f"  ❌ ERROR: Could not read file content. Check file integrity.")
        print(f"  Error details: {e}")
        return False

def download_granules(granule_ids, output_dir):
    """
    Download granules, check their contents, and save to a timestamped directory.
    
    Args:
        granule_ids (list): List of CMR concept IDs for the granules to download.
        output_dir (str): The full path to the timestamped directory to save files in.
        
    Returns:
        list: A list of file paths for valid (non-placeholder) downloaded files.
    """
    if not EARTHACCESS_AVAILABLE:
        print("\nError: earthaccess library not installed")
        return []
    
    # Create the timestamped output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\nDownloading {len(granule_ids)} granule(s) to **{output_dir}**...")
    
    try:
        granule_results = earthaccess.search_data(
            concept_id=granule_ids
        )
        
        if not granule_results:
            print("✗ No granules found. They may not be available for download.")
            return []
        
        print(f"Found {len(granule_results)} granule(s) available for download")
        print("\nPress Ctrl+C at any time to stop downloading and check files downloaded so far.\n")
        
        # Download files with progress bar
        files = []
        try:
            with tqdm(total=len(granule_results), desc="Downloading", unit="file") as pbar:
                for granule in granule_results:
                    try:
                        downloaded = earthaccess.download(
                            granule,
                            local_path=output_dir
                        )
                        if downloaded:
                            files.extend(downloaded if isinstance(downloaded, list) else [downloaded])
                        pbar.update(1)
                    except Exception as e:
                        pbar.write(f"✗ Error downloading {granule.get('producer_granule_id', 'unknown')}: {e}")
                        pbar.update(1)
        except KeyboardInterrupt:
            print("\n\n⚠️  Download cancelled by user!")
            print(f"Downloaded {len(files)} file(s) before cancellation.")
        
        print(f"\n✓ Downloaded {len(files)} file(s) total")
        
        # Check each file
        valid_files = []
        empty_files = []
        
        print("\n" + "="*80)
        print("CHECKING FILE CONTENTS (Data Validation):")
        print("="*80)
        
        for file_path in files:
            if check_file_contents(file_path):
                valid_files.append(file_path)
            else:
                empty_files.append(file_path)
        
        # Summary
        print("\n" + "="*80)
        print("DOWNLOAD SUMMARY:")
        print("="*80)
        print(f"Valid files with data: {len(valid_files)}")
        print(f"Empty/placeholder files: {len(empty_files)}")
        
        if valid_files:
            print(f"\n✓ Valid files saved to: {output_dir}")
        
        if empty_files:
            print("\n✗ Empty files (you may want to delete these):")
            for f in empty_files:
                print(f"  - {f}")
            print("\n**Action:** If you see many empty files, try selecting a different **Aerosol** collection.")
        
        return valid_files
        
    except Exception as e:
        print(f"\n✗ Download error: {e}")
        return []
        
def login_earthaccess():
    """
    Login to NASA Earthdata, attempting to load credentials from .env first, 
    then prompting the user if necessary.
    """
    if not EARTHACCESS_AVAILABLE:
        print("\nError: earthaccess library not installed")
        return False
    
    # --- START OF DOTENV LOGIC ---
    if DOTENV_AVAILABLE:
        # Load environment variables from .env file
        dotenv_path = find_dotenv('.env') 
        if dotenv_path:
            print(f"\nAttempting to load credentials from .env file: {dotenv_path}")
            load_dotenv(dotenv_path)
            
            # Check if env vars were set
            if os.environ.get('EARTHDATA_USERNAME') and os.environ.get('EARTHDATA_PASSWORD'):
                print("✓ Credentials successfully loaded from .env. Attempting silent login.")
            else:
                print("⚠️ .env file found, but required variables (EARTHDATA_USERNAME, EARTHDATA_PASSWORD) not set. Falling back to prompt.")
        else:
            print("\n.env file not found. Falling back to saved credentials or manual prompt.")
    else:
        print("\nNote: Install 'pip install python-dotenv' to enable automatic credential loading from a .env file.")
    # --- END OF DOTENV LOGIC ---

    max_attempts = 3
    attempt = 0
    
    while attempt < max_attempts:
        try:
            attempt += 1
            
            if attempt == 1:
                # earthaccess.login() will check the environment first
                print("\nLogging in to NASA Earthdata...")
                # Only show prompt message if credentials weren't already loaded
                if not (os.environ.get('EARTHDATA_USERNAME') and os.environ.get('EARTHDATA_PASSWORD')):
                    print("(If this is the first time or if .env failed, it will prompt for username/password)")
            else:
                print(f"\n✗ Login failed. Attempt {attempt} of {max_attempts}")
                print("Please check your credentials and try again.")
            
            auth = earthaccess.login()
            
            if auth.authenticated:
                print("✓ Successfully authenticated!")
                return True
            else:
                print("✗ Authentication failed")
                if attempt < max_attempts:
                    retry = input("\nWould you like to try again? (y/n): ").lower()
                    if retry != 'y':
                        return False
                
        except Exception as e:
            error_msg = str(e)
            print(f"✗ Login error: {error_msg}")
            
            if "invalid_credentials" in error_msg.lower() or "authentication" in error_msg.lower():
                if attempt < max_attempts:
                    print("\nTip: Make sure you're using your Earthdata Login credentials from:")
                    print("https://urs.earthdata.nasa.gov/")
                    retry = input("\nWould you like to try again? (y/n): ").lower()
                    if retry != 'y':
                        return False
                else:
                    print(f"\nMaximum login attempts ({max_attempts}) reached.")
                    return False
            else:
                print(f"\nUnexpected error during login.")
                return False
    
    print(f"\nMaximum login attempts ({max_attempts}) reached.")
    print("Please verify your credentials at: https://urs.earthdata.nasa.gov/")
    return False

def parse_granule_selection(input_str, max_index):
    """
    Parses a string of indices and ranges (e.g., '1,3,5-7') into a set of unique integers.
    """
    selected_indices = set()
    parts = [part.strip() for part in input_str.split(',')]
    
    for part in parts:
        if '-' in part:
            # Handle range (e.g., 5-7)
            try:
                start, end = map(int, part.split('-'))
                if start > end:
                    start, end = end, start # Handle backward ranges like 7-5
                
                # Check for bounds and add indices (1-based)
                for i in range(start, end + 1):
                    if 1 <= i <= max_index:
                        selected_indices.add(i)
            except ValueError:
                # Ignore invalid range parts
                continue
        else:
            # Handle single index (e.g., 3)
            try:
                index = int(part)
                # Check for bounds and add index (1-based)
                if 1 <= index <= max_index:
                    selected_indices.add(index)
            except ValueError:
                # Ignore invalid single index parts
                continue
                
    # Return as a sorted list
    return sorted(list(selected_indices))

def get_valid_main_choice(prompt, max_options):
    """
    Gets a valid integer choice from the user, or 'q'. Loops until valid.
    Returns: integer (choice) or string ('q')
    """
    while True:
        choice = input(prompt).strip()
        if choice.lower() == 'q':
            return 'q'
        
        try:
            idx = int(choice) - 1 # Convert to 0-based index
            if 0 <= idx < max_options:
                return int(choice)
            else:
                print(f"Invalid selection. Please enter a number between 1 and {max_options}, or 'q' to quit.")
        except ValueError:
            print("Invalid input. Please enter a valid number or 'q'.")

def get_valid_download_choice():
    """
    Gets a valid choice (1, 2, or 3) for the download option. Loops until valid.
    Returns: string ('1', '2', or '3')
    """
    while True:
        print("\nOptions:")
        print("1. Download all granules")
        print("2. Download specific granules (e.g., 1,3,5-10)")
        print("3. Skip download")
        
        choice = input("\nEnter choice (1-3): ").strip()
        if choice in ['1', '2', '3']:
            return choice
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

def main():
    print("="*80)
    print("SATELLITE AEROSOL DATA FINDER FOR SEATTLE")
    print("Platforms: PACE OCI, NOAA-21, NOAA-20, Suomi NPP VIIRS")
    print("="*80)
    
    # Show info about satellite data types
    display_satellite_info()
    
    input("\nPress Enter to search for aerosol/pollution data...")
    
    # Search for collections
    collections = search_aerosol_collections()
    
    if not collections:
        print("\nNo collections found. Try searching manually at:")
        print("https://search.earthdata.nasa.gov/search?q=NOAA-21%20VIIRS%20aerosol")
        return
        
    # Apply Filtering
    collections = filter_collections(collections)
    
    if not collections:
        print("\n" + "="*80)
        print("No valid L3 Aerosol collections found after filtering. Please check the search terms.")
        print("Exiting.")
        print("="*80)
        return

    granules = []
    
    # --- Main loop for collection selection and granule search with validation ---
    while True:
        # Display collections with pagination and guidance
        display_collections_paginated(collections, page_size=25)
        
        # Get validated choice
        choice = get_valid_main_choice(
            f"\nEnter **Aerosol L3** collection number (1-{len(collections)}) to search for data (or 'q' to quit): ", 
            len(collections)
        )
        
        if choice == 'q':
            break 
        
        try:
            # choice is guaranteed to be a valid integer index (1-based)
            idx = choice - 1
            selected = collections[idx]
            short_name = selected.get('short_name')
            
            print(f"\nSelected: {selected.get('title')}")
            
            # Get date range (No validation loop here, simple input for now)
            print("\nEnter date range (leave blank for last 7 days):")
            start_input = input("Start date (YYYY-MM-DD): ").strip()
            end_input = input("End date (YYYY-MM-DD): ").strip()
            
            start_date = None
            end_date = None
            
            if start_input:
                try:
                    start_date = datetime.strptime(start_input, '%Y-%m-%d')
                except ValueError:
                    print("Invalid date format. Using default range.")
            if end_input:
                try:
                    end_date = datetime.strptime(end_input, '%Y-%m-%d')
                except ValueError:
                    print("Invalid date format. Using default range.")

            # Search for granules
            granules = search_granules(short_name, start_date, end_date)
            
            if not granules:
                print("\n" + "="*80)
                print("⚠️ No granules found for the selected collection and date range.")
                print("Returning to collection selection to try a different collection or date range.")
                print("="*80)
                continue # Loop continues
            
            # Granules found, break the loop
            break 
            
        except Exception as e:
            print(f"An unexpected error occurred: {e}. Returning to collection selection.")
            continue
            
    # Check if the loop was broken by the user quitting
    if choice == 'q' or not granules:
        print("\nSearch operation ended.")
        return 

    # --- SUCCESSFUL GRANULE LOGIC STARTS HERE ---
    
    # Display results
    granule_list = display_granule_info(granules)
    
    # Save results to data/results
    if granules:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(RESULTS_DIR, f"aerosol_granules_{timestamp}.json")
        
        with open(output_file, 'w') as f:
            json.dump(granules, f, indent=2)
        print(f"\n✓ Full results saved to: **{output_file}**")
    
    # Download option
    if granule_list and EARTHACCESS_AVAILABLE:
        print("\n" + "="*80)
        download_choice = input("\nWould you like to download files? (y/n): ").lower()
        
        if download_choice == 'y':
            if not login_earthaccess():
                print("\nCannot download without authentication.")
                return
            
            # Get validated download choice (1, 2, or 3)
            choice = get_valid_download_choice()
            
            selected_granules_ids = []
            
            if choice == '1':
                selected_granules_ids = [g['id'] for g in granule_list]
            elif choice == '2':
                # --- START OF RANGE/LIST PROCESSING WITH VALIDATION ---
                max_granules = len(granule_list)
                print(f"Total granules available: {max_granules}")
                
                while True:
                    indices_input = input("Enter granule numbers (comma-separated or range, e.g., 1,3,5-10): ").strip()
                    selected_indices_1based = parse_granule_selection(indices_input, max_granules)
                    
                    if not selected_indices_1based:
                        print(f"Invalid or empty selection. Please enter valid numbers/ranges between 1 and {max_granules}.")
                        continue
                    else:
                        break
                
                # Convert 1-based indices back to 0-based and fetch the corresponding IDs
                for i in selected_indices_1based:
                    selected_granules_ids.append(granule_list[i-1]['id'])
                # --- END OF RANGE/LIST PROCESSING ---
            elif choice == '3':
                print("Skipping download.")
                return
            
            if selected_granules_ids:
                # Create timestamped downloads directory
                download_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                downloads_target_dir = os.path.join(DOWNLOADS_BASE_DIR, download_timestamp)
                
                print(f"\nDownloads will be saved to: **{downloads_target_dir}**")

                valid_files = download_granules(selected_granules_ids, downloads_target_dir)
                
                if valid_files:
                    print(f"\n✓ Successfully downloaded {len(valid_files)} valid file(s)!")
                    print(f"\nYou can now use the NetCDF reader script to explore files in: **{downloads_target_dir}**")
    
    elif not EARTHACCESS_AVAILABLE:
        print("\n" + "="*80)
        print("DOWNLOAD INSTRUCTIONS:")
        print("="*80)
        print("""
earthaccess not installed. To enable downloading:

1. Install earthaccess:
    pip install earthaccess

2. Create a NASA Earthdata account:
    https://urs.earthdata.nasa.gov/users/new

3. Run this script again to download files automatically.
""")

if __name__ == "__main__":
    main()