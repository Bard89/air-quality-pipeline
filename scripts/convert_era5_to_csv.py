#!/usr/bin/env python3
"""
Convert ERA5 NetCDF files to CSV format
"""

import xarray as xr
import pandas as pd
import os
import argparse
from pathlib import Path
import glob

def convert_netcdf_to_csv(nc_file, output_dir):
    """Convert a single NetCDF file to CSV"""
    
    print(f"Processing: {nc_file}")
    
    # Open NetCDF
    ds = xr.open_dataset(nc_file)
    
    # Get the PBL variable name (could be 'blh' or 'boundary_layer_height')
    pbl_vars = [var for var in ds.data_vars if 'boundary' in var.lower() or var == 'blh']
    
    if not pbl_vars:
        print(f"Warning: No PBL variable found in {nc_file}")
        print(f"Available variables: {list(ds.data_vars)}")
        return
    
    pbl_var = pbl_vars[0]
    print(f"Using variable: {pbl_var}")
    
    # Convert to DataFrame - ensure all dimensions are included
    df = ds[pbl_var].to_dataframe().reset_index()
    
    # Debug: print available columns
    print(f"Available columns after conversion: {list(df.columns)}")
    
    # Rename columns
    column_mapping = {
        'time': 'timestamp',
        'valid_time': 'timestamp',  # ERA5 uses valid_time
        'latitude': 'latitude',
        'longitude': 'longitude',
        pbl_var: 'pbl_height_m'
    }
    
    # Some files might use different dimension names
    if 'lat' in df.columns:
        column_mapping['lat'] = 'latitude'
    if 'lon' in df.columns:
        column_mapping['lon'] = 'longitude'
    
    df = df.rename(columns=column_mapping)
    
    # If timestamp is still missing, it might be in the index
    if 'timestamp' not in df.columns and 'time' in df.index.names:
        df = df.reset_index()
        df = df.rename(columns={'time': 'timestamp'})
    
    # Select only needed columns
    keep_cols = ['timestamp', 'latitude', 'longitude', 'pbl_height_m']
    available_cols = [col for col in keep_cols if col in df.columns]
    
    if 'timestamp' not in available_cols:
        print("Warning: timestamp column not found!")
        print(f"Available columns: {list(df.columns)}")
    
    df = df[available_cols]
    
    # Create output filename
    base_name = Path(nc_file).stem
    output_file = os.path.join(output_dir, f"{base_name}.csv")
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Saved: {output_file}")
    print(f"Records: {len(df):,}")
    
    # Close dataset
    ds.close()
    
    return output_file


def main():
    parser = argparse.ArgumentParser(description='Convert ERA5 NetCDF files to CSV')
    parser.add_argument('input', help='Input NetCDF file or directory')
    parser.add_argument('--output-dir', default='data/era5/processed', 
                       help='Output directory for CSV files')
    parser.add_argument('--pattern', default='*.nc', 
                       help='File pattern for directory input (default: *.nc)')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Get list of files to process
    if os.path.isfile(args.input):
        files = [args.input]
    elif os.path.isdir(args.input):
        files = glob.glob(os.path.join(args.input, args.pattern))
        files.sort()
    else:
        print(f"Error: {args.input} not found")
        return
    
    if not files:
        print("No files found to process")
        return
    
    print(f"Found {len(files)} files to process")
    
    # Process each file
    for nc_file in files:
        try:
            convert_netcdf_to_csv(nc_file, args.output_dir)
        except Exception as e:
            print(f"Error processing {nc_file}: {e}")
            continue
    
    print("\nConversion complete!")
    print(f"CSV files saved to: {args.output_dir}")


if __name__ == '__main__':
    main()