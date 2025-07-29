#!/usr/bin/env python3
"""
Download historical ERA5 data directly from CDS
"""

import cdsapi
import os
from datetime import datetime, timedelta
import argparse
import calendar

def download_era5_pbl(year, month, output_dir):
    """Download ERA5 PBL height for a specific month"""
    
    # Initialize CDS client
    c = cdsapi.Client()
    
    # Define output filename
    output_file = os.path.join(output_dir, f'era5_pbl_{year}_{month:02d}.nc')
    
    if os.path.exists(output_file):
        print(f"File already exists: {output_file}")
        return
    
    print(f"Downloading ERA5 PBL for {year}-{month:02d}...")
    
    # Get the correct number of days for this month
    _, num_days = calendar.monthrange(year, month)
    
    # Request parameters
    request = {
        'product_type': 'reanalysis',
        'format': 'netcdf',
        'variable': 'boundary_layer_height',
        'year': str(year),
        'month': f'{month:02d}',
        'day': [f'{d:02d}' for d in range(1, num_days + 1)],  # Correct days for month
        'time': [f'{h:02d}:00' for h in range(24)],  # All hours
        'area': [46, 122, 24, 146],  # Japan: North, West, South, East
    }
    
    try:
        # Download data
        c.retrieve(
            'reanalysis-era5-single-levels',
            request,
            output_file
        )
        print(f"Successfully downloaded: {output_file}")
        
    except Exception as e:
        print(f"Error downloading {year}-{month:02d}: {e}")
        if os.path.exists(output_file):
            os.remove(output_file)


def main():
    parser = argparse.ArgumentParser(description='Download historical ERA5 PBL data')
    parser.add_argument('--start-year', type=int, required=True, help='Start year')
    parser.add_argument('--end-year', type=int, required=True, help='End year')
    parser.add_argument('--start-month', type=int, default=1, help='Start month (default: 1)')
    parser.add_argument('--end-month', type=int, default=12, help='End month (default: 12)')
    parser.add_argument('--output-dir', default='data/era5/raw', help='Output directory')
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Create .cdsapirc if it doesn't exist
    cdsapirc_path = os.path.expanduser('~/.cdsapirc')
    if not os.path.exists(cdsapirc_path):
        print("\nERROR: ~/.cdsapirc not found!")
        print("\nCreate ~/.cdsapirc with:")
        print("url: https://cds.climate.copernicus.eu/api/v2")
        print("key: YOUR_UID:YOUR_API_KEY")
        print("\nGet your key from: https://cds.climate.copernicus.eu/api-how-to")
        return
    
    # Download data for each month
    for year in range(args.start_year, args.end_year + 1):
        start_m = args.start_month if year == args.start_year else 1
        end_m = args.end_month if year == args.end_year else 12
        
        for month in range(start_m, end_m + 1):
            download_era5_pbl(year, month, args.output_dir)
            
    print("\nDownload complete!")
    print(f"Files saved to: {args.output_dir}")
    print("\nTo process the NetCDF files:")
    print("1. Install: pip install xarray netcdf4")
    print("2. Use xarray to read and convert to CSV")


if __name__ == '__main__':
    main()