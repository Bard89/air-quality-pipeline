#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

from src.openaq.client import OpenAQClient
from src.openaq.location_finder import LocationFinder
from src.openaq.data_downloader import DataDownloader
from src.openaq.incremental_downloader import IncrementalDownloader
from src.core.data_storage import DataStorage
from src.utils.data_analyzer import analyze_dataset


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


def get_country_id(client, country_code):
    countries = client.get_countries()
    for country in countries.get('results', []):
        if country['code'].upper() == country_code.upper():
            return country['id'], country['name']
    return None, None


def main():
    parser = argparse.ArgumentParser(
        description='Download air quality data by country',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --country US --start 2024-01-01 --end 2024-12-31
  %(prog)s --country IN --start 2024-06-01 --end 2024-06-30 --parameters pm25,pm10
  %(prog)s --country JP --days 30  # Last 30 days
  %(prog)s --list-countries
        """
    )
    
    parser.add_argument('--country', '-c', type=str, 
                       help='Country code (e.g., US, IN, JP, TH)')
    parser.add_argument('--start', '-s', type=parse_date,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=parse_date,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--days', '-d', type=int,
                       help='Alternative to start/end: download last N days')
    parser.add_argument('--parameters', '-p', type=str,
                       help='Comma-separated parameters (default: all)')
    parser.add_argument('--limit-sensors', '-l', type=int,
                       help='Limit number of sensors per parameter')
    parser.add_argument('--list-countries', action='store_true',
                       help='List available countries and exit')
    parser.add_argument('--analyze', '-a', action='store_true', default=True,
                       help='Analyze data after download (default: True)')
    parser.add_argument('--country-wide', action='store_true',
                       help='Download ALL data from country (day-by-day, most efficient for full country)')
    parser.add_argument('--max-locations', type=int,
                       help='Maximum number of locations to download (for --country-wide mode)')
    
    args = parser.parse_args()
    
    load_dotenv()
    api_key = os.getenv('OPENAQ_API_KEY')
    if not api_key:
        print("Error: OPENAQ_API_KEY not found in .env file")
        sys.exit(1)
    
    storage = DataStorage()
    client = OpenAQClient(api_key, storage)
    
    if args.list_countries:
        print("Fetching available countries...")
        countries = client.get_countries()
        print("\nAvailable countries:")
        for c in sorted(countries['results'], key=lambda x: x.get('name', '')):
            print(f"  {c['code']:3} - {c['name']:30} (ID: {c['id']})")
        return
    
    if not args.country:
        parser.error("--country is required")
    
    # Determine date range
    if args.days:
        from datetime import timedelta
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=args.days)
    elif args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        parser.error("Specify either --start/--end or --days")
    
    # Get country info
    country_id, country_name = get_country_id(client, args.country)
    if not country_id:
        print(f"Error: Country '{args.country}' not found")
        print("Use --list-countries to see available countries")
        sys.exit(1)
    
    print(f"\n=== Air Quality Data Download ===")
    print(f"Country: {country_name} ({args.country.upper()})")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Parameters: {args.parameters or 'all'}")
    
    # Country-wide mode: most efficient for full country downloads
    if args.country_wide:
        print(f"\nMode: INCREMENTAL COUNTRY-WIDE")
        print("Data saves after each location - safe to interrupt!")
        
        params = None
        if args.parameters:
            params = [p.strip().lower() for p in args.parameters.split(',')]
            
        downloader = IncrementalDownloader(client)
        output_path = downloader.download_country_incremental(
            args.country.upper(), country_id, start_date, end_date, 
            params, max_locations=args.max_locations, resume=True
        )
        
        if args.analyze and output_path:
            print("\nANALYZING DATA...")
            analyze_dataset(output_path)
        
        return
    
    # Find locations
    finder = LocationFinder(client)
    print(f"\nFinding locations in {country_name}...")
    
    locations = finder.find_locations_in_country(
        args.country.upper(), 
        {args.country.upper(): {'id': country_id}}
    )
    print(f"Found {len(locations)} locations")
    
    # Extract all sensors
    all_sensors = []
    for location in locations:
        sensors = finder.extract_sensor_info(location)
        all_sensors.extend(sensors)
    
    # Filter by parameters if specified
    if args.parameters:
        param_list = [p.strip().lower() for p in args.parameters.split(',')]
        all_sensors = [s for s in all_sensors if s['parameter'] in param_list]
    
    # Filter active sensors
    if args.parameters:
        active_sensors = []
        param_list = [p.strip().lower() for p in args.parameters.split(',')]
        for param in param_list:
            param_sensors = finder.find_active_sensors(
                locations, 
                parameter=param,
                min_date=start_date.strftime('%Y-%m-%d')
            )
            active_sensors.extend(param_sensors)
    else:
        active_sensors = finder.find_active_sensors(
            locations, 
            parameter=None,
            min_date=start_date.strftime('%Y-%m-%d')
        )
    
    print(f"\nTotal sensors: {len(all_sensors)}")
    print(f"Active sensors: {len(active_sensors)}")
    
    # Count by parameter
    param_counts = {}
    for s in active_sensors:
        p = s['parameter']
        param_counts[p] = param_counts.get(p, 0) + 1
    
    print("\nActive sensors by parameter:")
    for p, count in sorted(param_counts.items()):
        print(f"  {p}: {count}")
    
    if not active_sensors:
        print("\nNo active sensors found!")
        sys.exit(1)
    
    # Apply sensor limit if specified
    if args.limit_sensors:
        limited_sensors = []
        for param in param_counts:
            param_sensors = [s for s in active_sensors if s['parameter'] == param]
            limited_sensors.extend(param_sensors[:args.limit_sensors])
        active_sensors = limited_sensors
        print(f"\nLimited to {len(active_sensors)} sensors")
    
    # Warn about large downloads
    days = (end_date - start_date).days
    estimated_requests = len(active_sensors) * ((days + 89) // 90)  # 90-day chunks
    estimated_time = estimated_requests * 1.05 / 60  # 1.05s per request
    
    if estimated_time > 60:
        print(f"\n⚠️  WARNING: This download will make ~{estimated_requests:,} API requests")
        print(f"Estimated time: {estimated_time:.0f} minutes")
        print("\nConsider using filters to reduce data:")
        print("  --parameters pm25,pm10  # Download only specific pollutants")
        print("  --limit-sensors 10      # Limit sensors per parameter")
        print("  --days 30               # Download recent data only")
        print("  --country-wide          # Use incremental mode for full country")
        
        response = input("\nContinue? (y/N): ")
        if response.lower() != 'y':
            print("Download cancelled.")
            sys.exit(0)
    
    # Download data
    downloader = DataDownloader(client)
    print(f"\nStarting download from {len(active_sensors)} sensors...")
    
    df = downloader.download_multiple_sensors(active_sensors, start_date, end_date)
    
    if df.empty:
        print("No data downloaded!")
        sys.exit(1)
    
    # Save data
    filename = f"{args.country.lower()}_airquality_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    output_path = storage.get_processed_dir('openaq') / filename
    df.to_csv(output_path, index=False)
    
    print(f"\n{'='*60}")
    print(f"DOWNLOAD COMPLETE")
    print(f"Saved {len(df):,} measurements to:")
    print(f"{output_path}")
    print(f"{'='*60}")
    
    # Analyze if requested
    if args.analyze:
        print("\nANALYZING DATA...")
        analyze_dataset(str(output_path))


if __name__ == "__main__":
    main()