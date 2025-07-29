#!/usr/bin/env python3

import asyncio
import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import pandas as pd
import logging

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from src.plugins.era5.datasource import ERA5DataSource
from src.core.data_storage import DataStorage
from src.domain.models import ParameterType


async def download_era5_pbl(
    country_code: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: Optional[str] = None
):
    load_dotenv()
    
    cds_api_key = os.getenv('CDS_API_KEY')
    if not cds_api_key:
        print("\nWARNING: CDS_API_KEY not found in environment variables")
        print("ERA5 will run in demo mode with synthetic data")
        print("\nTo use real ERA5 data:")
        print("1. Register at https://cds.climate.copernicus.eu/")
        print("2. Get your API key from https://cds.climate.copernicus.eu/api-how-to")
        print("3. Add CDS_API_KEY to your .env file")
        print("\nContinuing with demo data...\n")
        
    try:
        datasource = ERA5DataSource(cds_api_key=cds_api_key)
        storage = DataStorage()
        
        print(f"\n=== ERA5 PBL Height Download ===")
        print(f"Country: {country_code}")
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        print(f"Mode: {'Real data' if cds_api_key else 'Demo data'}")
        
        # Get locations for country
        locations = await datasource.get_locations(country=country_code, limit=5)
        if not locations:
            print(f"Country {country_code} not supported")
            return None
            
        print(f"Found {len(locations)} grid points")
        
        # Collect all PBL data
        all_data = []
        
        for location in locations:
            print(f"\nProcessing {location.name}...")
            
            # Get PBL height sensor
            sensors = await datasource.get_sensors(
                location, 
                parameters=[ParameterType.BOUNDARY_LAYER_HEIGHT]
            )
            
            if not sensors:
                print("No PBL height sensor found")
                continue
                
            sensor = sensors[0]
            
            # Get measurements
            async for measurements in datasource.get_measurements(
                sensor,
                start_date=start_date,
                end_date=end_date
            ):
                for m in measurements:
                    data_row = {
                        'timestamp': m.timestamp,
                        'latitude': float(location.coordinates.latitude),
                        'longitude': float(location.coordinates.longitude),
                        'pbl_height_m': float(m.value),
                        'quality': m.quality_flag,
                        'location_id': location.id
                    }
                    all_data.append(data_row)
                    
        await datasource.close()
        
        if not all_data:
            print("\nNo PBL height data found")
            return None
            
        # Create DataFrame
        df = pd.DataFrame(all_data)
        df = df.sort_values(['timestamp', 'latitude', 'longitude'])
        
        # Save to CSV
        if output_dir:
            output_path = Path(output_dir)
        else:
            output_path = storage.get_processed_dir('era5')
            
        output_path.mkdir(parents=True, exist_ok=True)
        
        filename = f"era5_pbl_{country_code.lower()}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
        filepath = output_path / filename
        
        df.to_csv(filepath, index=False)
        
        print(f"\n{'='*60}")
        print("DOWNLOAD COMPLETE")
        print(f"Total records: {len(df)}")
        print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"PBL height range: {df['pbl_height_m'].min():.1f}m - {df['pbl_height_m'].max():.1f}m")
        print(f"Average PBL height: {df['pbl_height_m'].mean():.1f}m")
        print(f"Saved to: {filepath}")
        print(f"{'='*60}")
        
        # Print diurnal cycle
        df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        hourly_avg = df.groupby('hour')['pbl_height_m'].mean()
        
        print("\nDiurnal PBL cycle:")
        for hour, height in hourly_avg.items():
            print(f"  {hour:02d}:00 - {height:6.1f}m")
            
        return filepath
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Download ERA5 Planetary Boundary Layer Height data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --country JP --start 2024-01-01 --end 2024-01-07
  %(prog)s --country JP --start 2024-03-01 --end 2024-03-31  # Spring season
  
Note: Real data requires CDS API key. Without it, demo data will be generated.
        """
    )
    
    parser.add_argument('--country', '-c', type=str, required=True,
                       help='Country code (currently only JP supported)')
    parser.add_argument('--start', '-s', type=parse_date, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=parse_date, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', '-o', type=str,
                       help='Output directory (default: data/era5/processed/)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.verbose:
        logging.basicConfig(level=logging.INFO, 
                          format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING)
    
    if args.country.upper() != 'JP':
        print("ERROR: Currently only Japan (JP) is supported for ERA5 data")
        return
        
    if args.start > args.end:
        print("ERROR: Start date must be before end date")
        return
        
    # Run the download
    asyncio.run(download_era5_pbl(
        args.country.upper(),
        args.start,
        args.end,
        args.output_dir
    ))


if __name__ == "__main__":
    main()