#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
from tqdm import tqdm

from src.plugins.jartic.datasource import JARTICDataSource
from src.core.data_storage import DataStorage
from src.utils.data_analyzer import analyze_dataset


async def download_jartic_data(args):
    storage = DataStorage()
    
    # Create datasource (JARTIC doesn't need API keys)
    datasource = JARTICDataSource(
        api_keys=[],  # No API keys needed for JARTIC
        cache_dir=Path("data/jartic/cache"),
        cleanup_after_parse=not args.keep_cache
    )
    
    async with datasource:
        if args.list_locations:
            print("Fetching JARTIC traffic monitoring locations...")
            locations = await datasource.find_locations(
                country_code="JP",
                limit=args.limit
            )
            
            print(f"\nFound {len(locations)} locations:")
            for loc in locations[:10]:  # Show first 10
                print(f"  {loc.id} - {loc.name}")
                print(f"    Coordinates: {loc.coordinates.latitude}, {loc.coordinates.longitude}")
                print(f"    City: {loc.city or 'Unknown'}")
                print(f"    Metadata: {loc.metadata}")
                print()
            
            if len(locations) > 10:
                print(f"  ... and {len(locations) - 10} more locations")
            return
        
        if args.list_archives:
            print("Checking available JARTIC archives...")
            downloader = datasource.downloader
            archives = await downloader.get_archive_index()
            
            print(f"\nAvailable archives ({len(archives)} total):")
            for archive in archives[:12]:  # Show last year
                print(f"  {archive['year']}-{archive['month']:02d} - {archive['text']}")
            
            if len(archives) > 12:
                print(f"  ... and {len(archives) - 12} more archives")
            return
        
        # Download measurements
        if not args.location_id:
            print("\n🚗 JARTIC Traffic Data Download")
            print("="*60)
            print(f"Period: {args.start.strftime('%Y-%m-%d')} to {args.end.strftime('%Y-%m-%d')}")
            
            # Calculate which archives we need
            months_needed = []
            current = datetime(args.start.year, args.start.month, 1)
            end = datetime(args.end.year, args.end.month, 1)
            while current <= end:
                months_needed.append((current.year, current.month))
                if current.month == 12:
                    current = datetime(current.year + 1, 1, 1)
                else:
                    current = datetime(current.year, current.month + 1, 1)
            
            print(f"Archives needed: {len(months_needed)}")
            for year, month in months_needed:
                print(f"  • {year}-{month:02d}")
            print()
            
            print("📍 Finding locations...")
            locations = await datasource.find_locations(country_code="JP")
            print(f"   Found {len(locations)} locations")
        else:
            # Create a dummy location for specific ID
            from src.domain.models import Location, Coordinates
            from decimal import Decimal
            locations = [Location(
                id=args.location_id,
                name=f"Location {args.location_id}",
                coordinates=Coordinates(
                    latitude=Decimal("35.6762"),
                    longitude=Decimal("139.6503")
                ),
                country="JP"
            )]
        
        # Prepare output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"jp_traffic_{timestamp}.csv"
        output_path = storage.get_processed_dir('jartic') / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download data
        total_measurements = 0
        with open(output_path, 'w') as f:
            # Write header
            f.write("timestamp,location_id,location_name,latitude,longitude,parameter,value,unit\n")
            
            # Add progress bar for locations
            locations_to_process = min(len(locations), args.max_locations) if args.max_locations else len(locations)
            location_progress = tqdm(
                total=locations_to_process,
                desc="📍 Locations",
                unit="loc",
                position=0,
                leave=True
            )
            
            for loc_idx, location in enumerate(locations):
                if args.max_locations and loc_idx >= args.max_locations:
                    break
                
                location_progress.set_postfix_str(f"Current: {location.name[:30]}")
                
                sensors = await datasource.get_sensors(location)
                
                # Only show sensor progress for sensors we actually process
                sensors_with_data = []
                for sensor in sensors:
                    sensors_with_data.append(sensor)
                
                for sensor_idx, sensor in enumerate(sensors_with_data):
                    sensor_desc = f"{sensor.parameter.value}"
                    
                    measurement_count = 0
                    measurement_progress = tqdm(
                        desc=sensor_desc,
                        unit=" meas",
                        position=1,
                        leave=False,
                        bar_format='{desc}: {n_fmt} measurements [{elapsed}]'
                    )
                    
                    async for measurement in datasource.stream_measurements(
                        sensor,
                        start_date=args.start,
                        end_date=args.end
                    ):
                        f.write(f"{measurement.timestamp.isoformat()},"
                               f"{location.id},"
                               f"{location.name},"
                               f"{location.coordinates.latitude},"
                               f"{location.coordinates.longitude},"
                               f"{sensor.parameter.value},"
                               f"{measurement.value},"
                               f"{sensor.unit.value}\n")
                        
                        measurement_count += 1
                        total_measurements += 1
                        measurement_progress.update(1)
                        
                        if measurement_count % 10000 == 0:
                            f.flush()
                    
                    measurement_progress.close()
                    if measurement_count > 0:
                        logger.info(f"{sensor.parameter.value}: {measurement_count:,} measurements")
                
                location_progress.update(1)
            
            location_progress.close()
        
        print(f"\n{'='*60}")
        print("DOWNLOAD COMPLETE")
        print(f"Total measurements: {total_measurements:,}")
        print(f"Saved to: {output_path}")
        print(f"{'='*60}")
        
        if args.analyze and total_measurements > 0:
            print("\nANALYZING DATA...")
            analyze_dataset(str(output_path))


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Download JARTIC traffic data for Japan',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --list-archives                    # Show available archives
  %(prog)s --list-locations                   # Show traffic monitoring locations
  %(prog)s --start 2024-01-01 --end 2024-01-31  # Download January 2024 data
  %(prog)s --location-id 001 --start 2024-01-01 --end 2024-01-31  # Specific location
  %(prog)s --max-locations 10 --start 2024-01-01 --end 2024-01-31  # First 10 locations
        """
    )
    
    # List options
    parser.add_argument('--list-archives', action='store_true',
                       help='List available JARTIC archives')
    parser.add_argument('--list-locations', action='store_true',
                       help='List traffic monitoring locations')
    parser.add_argument('--limit', type=int, default=100,
                       help='Limit for location listing (default: 100)')
    
    # Download options
    parser.add_argument('--start', '-s', type=parse_date,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=parse_date,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--location-id', type=str,
                       help='Download data for specific location ID')
    parser.add_argument('--max-locations', type=int,
                       help='Maximum number of locations to download')
    
    # Other options
    parser.add_argument('--analyze', '-a', action='store_true', default=True,
                       help='Analyze data after download (default: True)')
    parser.add_argument('--keep-cache', action='store_true',
                       help='Keep downloaded archive files in cache')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not (args.list_archives or args.list_locations or (args.start and args.end)):
        parser.error("Specify either --list-archives, --list-locations, or --start/--end dates")
    
    # Run async download
    asyncio.run(download_jartic_data(args))


if __name__ == "__main__":
    main()