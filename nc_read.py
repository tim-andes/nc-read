import xarray as xr
import pandas as pd
import os

# Get file path
def get_file():
    file_path = input("File path to .nc file (e.g., ~/Downloads/file.nc): ")
    # Expand ~ to home directory
    file_path = os.path.expanduser(file_path)
    
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None
    
    return file_path

# Display dataset info
def display_info(ds):
    print("\n" + "="*60)
    print("DATASET OVERVIEW")
    print("="*60)
    print(ds)
    
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

# Extract and save data
def save_data(ds):
    print("\n" + "="*60)
    print("EXPORT OPTIONS")
    print("="*60)
    print("1. Export entire dataset to CSV")
    print("2. Export entire dataset to Parquet")
    print("3. Export specific variable to CSV")
    print("4. Export dataset info to text file")
    print("5. Filter by geographic area (lat/lon) then export")
    print("6. Exit without saving")
    
    choice = input("\nEnter your choice (1-6): ")
    
    if choice == "1":
        output_file = input("Output filename (e.g., output.csv): ")
        print("Converting to dataframe...")
        df = ds.to_dataframe().reset_index()
        df.to_csv(output_file, index=False)
        print(f"✓ Saved to {output_file}")
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
        
    elif choice == "2":
        output_file = input("Output filename (e.g., output.parquet): ")
        print("Converting to dataframe...")
        df = ds.to_dataframe().reset_index()
        df.to_parquet(output_file, compression='snappy')
        print(f"✓ Saved to {output_file}")
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
        
    elif choice == "3":
        print("\nAvailable variables:")
        vars_list = list(ds.data_vars)
        for i, var in enumerate(vars_list, 1):
            print(f"{i}. {var}")
        
        var_choice = int(input("\nSelect variable number: ")) - 1
        var_name = vars_list[var_choice]
        output_file = input(f"Output filename for {var_name} (e.g., {var_name}.csv): ")
        
        print(f"Converting {var_name} to dataframe...")
        df = ds[var_name].to_dataframe().reset_index()
        df.to_csv(output_file, index=False)
        print(f"✓ Saved to {output_file}")
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
        
    elif choice == "4":
        output_file = input("Output filename (e.g., dataset_info.txt): ")
        with open(output_file, 'w') as f:
            f.write(str(ds))
        print(f"✓ Dataset info saved to {output_file}")
        
    elif choice == "5":
        # Try to identify lat/lon coordinate names
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
            print(f"Available coordinates: {list(ds.coords)}")
            return
        
        print(f"\nFound coordinates: {lat_coord}, {lon_coord}")
        print(f"{lat_coord} range: {float(ds[lat_coord].min())} to {float(ds[lat_coord].max())}")
        print(f"{lon_coord} range: {float(ds[lon_coord].min())} to {float(ds[lon_coord].max())}")
        print("\nFor Seattle, WA try: lat 47.4 to 47.7, lon -122.4 to -122.2")
        
        lat_min = float(input(f"\nEnter minimum {lat_coord}: "))
        lat_max = float(input(f"Enter maximum {lat_coord}: "))
        lon_min = float(input(f"Enter minimum {lon_coord}: "))
        lon_max = float(input(f"Enter maximum {lon_coord}: "))
        
        print("Filtering data...")
        filtered = ds.sel(
            {lat_coord: slice(lat_min, lat_max),
             lon_coord: slice(lon_min, lon_max)}
        )
        
        print(f"Filtered dataset size: {filtered.dims}")
        
        output_file = input("Output filename (e.g., seattle_data.csv): ")
        df = filtered.to_dataframe().reset_index()
        df.to_csv(output_file, index=False)
        print(f"✓ Saved filtered data to {output_file}")
        print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
        
    elif choice == "6":
        print("Exiting without saving.")
        return
    else:
        print("Invalid choice.")

# Main function
def main():
    print("NetCDF File Explorer and Exporter")
    print("="*60)
    
    file_path = get_file()
    if not file_path:
        return
    
    print(f"\nOpening {file_path}...")
    
    try:
        ds = xr.open_dataset(file_path)
        display_info(ds)
        
        while True:
            save_choice = input("\nWould you like to export data? (y/n): ").lower()
            if save_choice == 'y':
                save_data(ds)
                continue_choice = input("\nExport another format? (y/n): ").lower()
                if continue_choice != 'y':
                    break
            else:
                break
        
        ds.close()
        print("\n✓ Done!")
        
    except Exception as e:
        print(f"\nError opening file: {e}")

if __name__ == "__main__":
    main()