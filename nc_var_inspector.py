import xarray as xr
import os
import sys

def inspect_netcdf(file_path):
    """Inspect all variables, coordinates, and attributes in a NetCDF file"""
    
    print("="*80)
    print(f"INSPECTING: {os.path.basename(file_path)}")
    print("="*80)
    
    try:
        ds = xr.open_dataset(file_path)
        
        # Basic info
        print(f"\n📁 File: {file_path}")
        print(f"   Data Variables: {len(ds.data_vars)}")
        print(f"   Coordinates: {len(ds.coords)}")
        print(f"   Dimensions: {len(ds.dims)}")
        
        # Dimensions
        print("\n" + "="*80)
        print("DIMENSIONS:")
        print("="*80)
        for dim, size in ds.dims.items():
            print(f"  {dim}: {size}")
        
        # Coordinates
        print("\n" + "="*80)
        print("COORDINATES:")
        print("="*80)
        if len(ds.coords) > 0:
            for coord in ds.coords:
                coord_data = ds.coords[coord]
                print(f"\n  {coord}:")
                print(f"    Shape: {coord_data.shape}")
                print(f"    Data type: {coord_data.dtype}")
                if coord_data.size > 0 and coord_data.size <= 10:
                    print(f"    Values: {coord_data.values}")
                elif coord_data.size > 0:
                    print(f"    Range: {float(coord_data.min())} to {float(coord_data.max())}")
        else:
            print("  No coordinates found")
        
        # Data Variables
        print("\n" + "="*80)
        print("DATA VARIABLES:")
        print("="*80)
        if len(ds.data_vars) > 0:
            for var in ds.data_vars:
                var_data = ds[var]
                print(f"\n  ✓ {var}")
                print(f"    Shape: {var_data.shape}")
                print(f"    Dimensions: {var_data.dims}")
                print(f"    Data type: {var_data.dtype}")
                
                # Attributes
                if var_data.attrs:
                    print(f"    Attributes:")
                    for attr_key, attr_val in var_data.attrs.items():
                        attr_str = str(attr_val)
                        if len(attr_str) > 60:
                            attr_str = attr_str[:60] + "..."
                        print(f"      - {attr_key}: {attr_str}")
                
                # Sample data if small enough
                if var_data.size <= 20:
                    print(f"    Data: {var_data.values}")
                elif var_data.size > 0:
                    try:
                        # Count non-NaN values
                        import numpy as np
                        if np.issubdtype(var_data.dtype, np.floating):
                            non_nan = np.count_nonzero(~np.isnan(var_data.values))
                            total = var_data.size
                            print(f"    Non-NaN values: {non_nan}/{total} ({100*non_nan/total:.1f}%)")
                            if non_nan > 0:
                                print(f"    Range: {float(np.nanmin(var_data.values))} to {float(np.nanmax(var_data.values))}")
                    except:
                        pass
        else:
            print("  ⚠️  NO DATA VARIABLES FOUND")
        
        # Global Attributes
        print("\n" + "="*80)
        print("GLOBAL ATTRIBUTES:")
        print("="*80)
        if ds.attrs:
            for key, value in ds.attrs.items():
                value_str = str(value)
                if len(value_str) > 80:
                    value_str = value_str[:80] + "..."
                print(f"  {key}: {value_str}")
        else:
            print("  No global attributes")
        
        # Search for AOD-related variables
        print("\n" + "="*80)
        print("SEARCHING FOR AOD/AEROSOL VARIABLES:")
        print("="*80)
        aod_keywords = ['aod', 'aerosol', 'optical', 'depth', 'aot']
        found_aod = []
        
        for var in ds.data_vars:
            var_lower = var.lower()
            if any(keyword in var_lower for keyword in aod_keywords):
                found_aod.append(var)
        
        if found_aod:
            print("  ✓ Potential AOD variables found:")
            for var in found_aod:
                print(f"    - {var}")
        else:
            print("  ⚠️  No obvious AOD variables found")
            print("  💡 Check the full variable list above")
        
        ds.close()
        print("\n" + "="*80)
        return True
        
    except Exception as e:
        print(f"\n❌ Error reading file: {e}")
        return False

def main():
    print("NetCDF Variable Inspector")
    print("="*80)
    
    # Get file path
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("\nEnter path to .nc file: ").strip()
    
    # Remove quotes if present
    file_path = file_path.strip('"').strip("'")
    
    # Expand home directory
    file_path = os.path.expanduser(file_path)
    
    if not os.path.exists(file_path):
        print(f"\n❌ Error: File not found at {file_path}")
        return
    
    # Inspect the file
    success = inspect_netcdf(file_path)
    
    if success:
        print("\n✅ Inspection complete!")
    
    print("\n💡 TIP: Look for variables containing 'AOD', 'Aerosol', or 'Optical_Depth'")
    print("   These are typically the air quality measurements you need.")

if __name__ == "__main__":
    main()