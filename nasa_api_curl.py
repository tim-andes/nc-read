import requests
import json
import os
from datetime import datetime, timedelta
import xarray as xr

try:
    import earthaccess
    EARTHACCESS_AVAILABLE = True
except ImportError:
    EARTHACCESS_AVAILABLE = False
    print("Warning: earthaccess not installed. Download functionality will be limited.")
    print("Install with: pip install earthaccess")

# Seattle bounding box coordinates
SEATTLE_BBOX = {
    'west': -122.4,
    'south': 47.4,
    'east': -122.2,
    'north': 47.7
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

🌡️  THERMAL/HEAT DATA (for urban heat islands - NOT in this script):

For asphalt heat maps, you need THERMAL INFRARED data:

Landsat 8/9 - Thermal Infrared Sensor (TIRS)
  • Land Surface Temperature (LST)
  • Urban heat island mapping
  • Hot spots from roads, parking lots, roofs
  • 100m resolution - good for neighborhoods
  
ECOSTRESS (on ISS)
  • Very high resolution thermal (70m)
  • Surface temperature of buildings, streets
  • Urban heat stress indicators
  
MODIS Terra/Aqua - Thermal bands
  • Daily global temperature
  • 1km resolution - city-wide patterns
  
ASTER
  • 90m thermal resolution
  • Good for detailed urban thermal mapping

📊 RECOMMENDED DATA PIPELINE FOR URBAN HEALTH:

1. AIR POLLUTION: Use this script → PACE OCI or NOAA-21 VIIRS aerosol data
2. HEAT MAPS: Need separate search for Landsat or ECOSTRESS thermal data
3. COMBINATION: Overlay pollution + heat for comprehensive health risk maps
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
    
    print(f"\n{'='*80}")
    print(f"Found {len(unique_collections)} unique aerosol collections")
    print("="*80)
    
    return unique_collections

def display_collections_paginated(collections, page_size=25):
    """Display collections with pagination"""
    total = len(collections)
    start = 0
    
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
            print(f"   Short Name: {short_name}")
            print(f"   Description: {summary}...")
        
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
                size_mb = link.get('length', 0) / (1024 * 1024)  # Convert to MB
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
        print(f"   Time: {time_start}")
        print(f"   Granule ID: {granule_id}")
        if size_mb:
            print(f"   Size: {size_mb:.2f} MB")
        if download_links:
            print(f"   Download URL: {download_links[0][:80]}...")
    
    return granule_list

def check_file_contents(file_path):
    """Check if NetCDF file has actual data (not a placeholder)"""
    print(f"\nChecking file contents: {os.path.basename(file_path)}")
    
    try:
        ds = xr.open_dataset(file_path)
        
        num_vars = len(ds.data_vars)
        num_coords = len(ds.coords)
        
        print(f"  ✓ Variables found: {num_vars}")
        print(f"  ✓ Coordinates found: {num_coords}")
        
        if num_vars == 0:
            print(f"  ✗ WARNING: File has no data variables (likely placeholder/empty)")
            ds.close()
            return False
        
        # Show first few variables
        if num_vars > 0:
            print(f"  ✓ Sample variables:")
            for i, var in enumerate(list(ds.data_vars)[:5]):
                print(f"    - {var}: {ds[var].shape}")
        
        ds.close()
        return True
        
    except Exception as e:
        print(f"  ✗ Error reading file: {e}")
        return False

def login_earthaccess():
    """Login to NASA Earthdata"""
    if not EARTHACCESS_AVAILABLE:
        print("\nError: earthaccess library not installed")
        print("Install with: pip install earthaccess")
        return False
    
    try:
        print("\nLogging in to NASA Earthdata...")
        print("(First time will prompt for username/password)")
        auth = earthaccess.login()
        
        if auth.authenticated:
            print("✓ Successfully authenticated!")
            return True
        else:
            print("✗ Authentication failed")
            return False
            
    except Exception as e:
        print(f"✗ Login error: {e}")
        return False

def download_granules(granule_ids, output_dir='./data'):
    """Download granules and check their contents"""
    if not EARTHACCESS_AVAILABLE:
        print("\nError: earthaccess library not installed")
        return []
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\nDownloading {len(granule_ids)} granule(s) to {output_dir}...")
    print("Searching for granule data...")
    
    try:
        # First, search for the granules to get the actual granule objects
        granule_results = earthaccess.search_data(
            concept_id=granule_ids
        )
        
        if not granule_results:
            print("✗ No granules found. They may not be available for download.")
            return []
        
        print(f"Found {len(granule_results)} granule(s) available for download")
        
        # Download files
        files = earthaccess.download(
            granule_results,
            local_path=output_dir
        )
        
        print(f"\n✓ Downloaded {len(files)} file(s)")
        
        # Check each file
        valid_files = []
        empty_files = []
        
        print("\n" + "="*80)
        print("CHECKING FILE CONTENTS:")
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
            print("\n✓ Valid files:")
            for f in valid_files:
                print(f"  - {f}")
        
        if empty_files:
            print("\n✗ Empty files (you may want to delete these):")
            for f in empty_files:
                print(f"  - {f}")
        
        return valid_files
        
    except Exception as e:
        print(f"\n✗ Download error: {e}")
        return []

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
    
    # Display collections with pagination
    display_collections_paginated(collections, page_size=25)
    
    # Let user select a collection
    print("\n" + "="*80)
    choice = input("\nEnter collection number to search for data (or 'q' to quit): ")
    
    if choice.lower() == 'q':
        return
    
    try:
        idx = int(choice) - 1
        if idx < 0 or idx >= len(collections):
            print("Invalid selection")
            return
        
        selected = collections[idx]
        short_name = selected.get('short_name')
        
        print(f"\nSelected: {selected.get('title')}")
        
        # Get date range
        print("\nEnter date range (leave blank for last 7 days):")
        start_input = input("Start date (YYYY-MM-DD): ").strip()
        end_input = input("End date (YYYY-MM-DD): ").strip()
        
        start_date = None
        end_date = None
        
        if start_input:
            start_date = datetime.strptime(start_input, '%Y-%m-%d')
        if end_input:
            end_date = datetime.strptime(end_input, '%Y-%m-%d')
        
        # Search for granules
        granules = search_granules(short_name, start_date, end_date)
        
        # Display results
        granule_list = display_granule_info(granules)
        
        # Save results to JSON
        if granules:
            output_file = f"aerosol_granules_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_file, 'w') as f:
                json.dump(granules, f, indent=2)
            print(f"\n✓ Full results saved to: {output_file}")
        
        # Download option
        if granule_list and EARTHACCESS_AVAILABLE:
            print("\n" + "="*80)
            download_choice = input("\nWould you like to download files? (y/n): ").lower()
            
            if download_choice == 'y':
                # Login first
                if not login_earthaccess():
                    print("\nCannot download without authentication.")
                    return
                
                # Select granules to download
                print("\nOptions:")
                print("1. Download all granules")
                print("2. Download specific granules")
                print("3. Skip download")
                
                choice = input("\nEnter choice (1-3): ").strip()
                
                selected_granules = []
                
                if choice == '1':
                    selected_granules = [g['id'] for g in granule_list]
                elif choice == '2':
                    indices = input("Enter granule numbers (comma-separated, e.g., 1,3,5): ")
                    try:
                        selected_indices = [int(i.strip()) for i in indices.split(',')]
                        selected_granules = [granule_list[i-1]['id'] for i in selected_indices 
                                           if 0 < i <= len(granule_list)]
                    except (ValueError, IndexError):
                        print("Invalid selection")
                        return
                elif choice == '3':
                    print("Skipping download.")
                    return
                
                if selected_granules:
                    output_dir = input("\nOutput directory (default: ./data): ").strip() or './data'
                    valid_files = download_granules(selected_granules, output_dir)
                    
                    if valid_files:
                        print(f"\n✓ Successfully downloaded {len(valid_files)} valid file(s)!")
                        print(f"\nYou can now use the NetCDF reader script to explore these files.")
        
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
        
    except ValueError:
        print("Invalid input")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()