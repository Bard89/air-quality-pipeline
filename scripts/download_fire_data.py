#!/usr/bin/env python3

import asyncio
import argparse
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import pandas as pd

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from src.plugins.firms.datasource import FIRMSDataSource
from src.core.data_storage import DataStorage


async def download_fire_data(
    country_code: str,
    days_back: Optional[int] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    output_dir: Optional[str] = None
):
    # Load environment variables
    load_dotenv()
    
    # Check for API key
    api_key = os.getenv('FIRMS_API_KEY')
    if not api_key:
        print("ERROR: FIRMS_API_KEY not found in environment variables")
        print("Please set FIRMS_API_KEY in your .env file")
        print("Register at: https://firms.modaps.eosdis.nasa.gov/api/")
        return None
        
    try:
        # Initialize datasource
        datasource = FIRMSDataSource(api_key)
        storage = DataStorage()
        
        # Determine date range
        if start_date and end_date:
            # Use provided dates
            date_start = start_date
            date_end = end_date
        elif days_back:
            # Use days back from today
            date_end = datetime.now(timezone.utc)
            date_start = date_end - timedelta(days=days_back)
        else:
            # Default to last 7 days
            date_end = datetime.now(timezone.utc)
            date_start = date_end - timedelta(days=7)
            
        print(f"\n=== NASA FIRMS Fire Data Download ===")
        print(f"Country: {country_code}")
        print(f"Date range: {date_start.date()} to {date_end.date()}")
        print(f"Days: {(date_end - date_start).days + 1}")
        
        # Check if this is historical data
        is_historical = (datetime.now(timezone.utc) - date_start).days > 60
        if is_historical:
            print("\nWARNING: The FIRMS API only provides data for the last ~2 months.")
            print("For older data, please use the FIRMS Archive Download tool:")
            print("https://firms.modaps.eosdis.nasa.gov/download/")
            print("\nAttempting to fetch data anyway...")
        
        # Get location for country
        locations = await datasource.get_locations(country=country_code)
        if not locations:
            print(f"Country {country_code} not supported")
            return None
            
        location = locations[0]
        print(f"Location: {location.name}")
        
        # Get sensors
        sensors = await datasource.get_sensors(location)
        
        # Collect all fire data
        all_data = []
        
        for sensor in sensors:
            print(f"\nFetching {sensor.parameter.value} data...")
            
            async for measurements in datasource.get_measurements(
                sensor,
                start_date=date_start,
                end_date=date_end
            ):
                for m in measurements:
                    data_row = {
                        'timestamp': m.timestamp,
                        'country': country_code,
                        'parameter': sensor.parameter.value,
                        'value': float(m.value),
                        'unit': sensor.unit.value,
                        'latitude': m.metadata['fire_location']['lat'],
                        'longitude': m.metadata['fire_location']['lon'],
                        'satellite': m.metadata['satellite'],
                        'fire_id': m.metadata['fire_id'],
                        'intensity_class': m.metadata['intensity_class'],
                        'scan_area_km2': m.metadata.get('scan_area_km2')
                    }
                    all_data.append(data_row)
                    
        await datasource.close()
        
        if not all_data:
            print("\nNo fire detections found in the specified period")
            return None
            
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        # Remove duplicates based on fire_id
        df = df.drop_duplicates(subset=['fire_id', 'parameter'])
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        # Save to CSV
        if output_dir:
            output_path = Path(output_dir)
        else:
            output_path = storage.get_processed_dir('firms')
            
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create filename based on date range
        filename = f"firms_{country_code.lower()}_fires_{date_start.strftime('%Y%m%d')}_{date_end.strftime('%Y%m%d')}.csv"
        filepath = output_path / filename
        
        df.to_csv(filepath, index=False)
        
        print(f"\n{'='*60}")
        print("DOWNLOAD COMPLETE")
        print(f"Total fire detections: {len(df[df['parameter'] == 'fire_radiative_power'])}")
        print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"Saved to: {filepath}")
        print(f"{'='*60}")
        
        # Print summary statistics
        frp_data = df[df['parameter'] == 'fire_radiative_power']
        if not frp_data.empty:
            print("\nFire Statistics:")
            print(f"Average FRP: {frp_data['value'].mean():.1f} MW")
            print(f"Max FRP: {frp_data['value'].max():.1f} MW")
            print(f"Total fires by intensity:")
            intensity_counts = frp_data['intensity_class'].value_counts()
            for intensity, count in intensity_counts.items():
                print(f"  - {intensity}: {count}")
                
        return filepath
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


def parse_date(date_str):
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        # Make timezone aware
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Download NASA FIRMS fire detection data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --country JP --days 7
  %(prog)s --country IN --start 2024-01-01 --end 2024-01-31  # Historical data
  %(prog)s --country TH --start 2023-03-01 --end 2023-03-31  # Burning season
  %(prog)s --check-status  # Check API status
  %(prog)s --list-countries
        """
    )
    
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    parser.add_argument('--country', '-c', type=str,
                       help='Country code (e.g., JP, IN, TH)')
    parser.add_argument('--days', '-d', type=int,
                       help='Number of days to look back from today')
    parser.add_argument('--start', '-s', type=parse_date,
                       help='Start date (YYYY-MM-DD) for historical data')
    parser.add_argument('--end', '-e', type=parse_date,
                       help='End date (YYYY-MM-DD) for historical data')
    parser.add_argument('--output-dir', '-o', type=str,
                       help='Output directory (default: data/firms/processed/)')
    parser.add_argument('--list-countries', action='store_true',
                       help='List supported countries and exit')
    parser.add_argument('--check-status', action='store_true',
                       help='Check API key status and exit')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.verbose:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING)
    
    if args.check_status:
        # Just check API status
        load_dotenv()
        api_key = os.getenv('FIRMS_API_KEY')
        if not api_key:
            print("ERROR: FIRMS_API_KEY not found in environment variables")
            return
            
        async def check_status():
            from src.plugins.firms.api_client import FIRMSAPIClient
            client = FIRMSAPIClient(api_key)
            status = await client.check_api_status()
            await client.close()
            
            print("\nFIRMS API Status:")
            print(f"API Key: {api_key[:10]}...")
            if "error" not in status:
                print(f"Transaction Limit: {status.get('transaction_limit', 'N/A')}")
                print(f"Current Transactions: {status.get('current_transactions', 'N/A')}")
                print(f"Time Window: {status.get('transaction_time_limit', 'N/A')}")
            else:
                print(f"Error: {status['error']}")
                
        asyncio.run(check_status())
        return
        
    if args.list_countries:
        print("\nSupported countries for FIRMS fire detection:")
        countries = [
            ('JP', 'Japan'),
            ('KR', 'South Korea'),
            ('CN', 'China'),
            ('IN', 'India'),
            ('TH', 'Thailand'),
            ('ID', 'Indonesia'),
            ('MY', 'Malaysia'),
            ('VN', 'Vietnam'),
        ]
        for code, name in countries:
            print(f"  {code}: {name}")
        return
        
    if not args.country:
        parser.error("--country is required")
        
    # Validate date arguments
    if args.start and args.end:
        if args.days:
            parser.error("Cannot use --days with --start/--end")
        if args.start > args.end:
            parser.error("Start date must be before end date")
    elif args.start or args.end:
        parser.error("Both --start and --end are required when using date range")
        
    # Run the download
    asyncio.run(download_fire_data(
        args.country.upper(),
        days_back=args.days,
        start_date=args.start,
        end_date=args.end,
        output_dir=args.output_dir
    ))


if __name__ == "__main__":
    main()