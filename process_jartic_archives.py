#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import time

from src.plugins.jartic.datasource import JARTICDataSource
from src.core.data_storage import DataStorage
from src.utils.data_analyzer import analyze_dataset


async def process_single_archive(archive_path: Path, locations, start_date: datetime, end_date: datetime, 
                               output_path: Path, archive_index: int, total_archives: int):
    """Process a single archive and return measurements"""
    measurements = []
    month_str = archive_path.stem.replace('jartic_typeB_', '').replace('_', '-')
    
    datasource = JARTICDataSource(
        api_keys=[],
        cache_dir=archive_path.parent,
        cleanup_after_parse=False
    )
    
    async with datasource:
        # Get all sensors for all locations
        location_count = 0
        for location in locations:
            location_count += 1
            sensors = await datasource.get_sensors(location)
            
            for sensor in sensors:
                try:
                    async for measurement in datasource.parser.parse_measurements(
                        archive_path,
                        sensor,
                        start_date,
                        end_date
                    ):
                        measurements.append({
                            'timestamp': measurement.timestamp.isoformat(),
                            'location_id': location.id,
                            'location_name': location.name,
                            'latitude': str(location.coordinates.latitude),
                            'longitude': str(location.coordinates.longitude),
                            'parameter': sensor.parameter.value,
                            'value': str(measurement.value),
                            'unit': sensor.unit.value
                        })
                        
                        # Yield batch every 1000 measurements
                        if len(measurements) >= 1000:
                            yield archive_index, month_str, measurements, location_count, len(locations)
                            measurements = []
                        
                except Exception as e:
                    pass  # Skip sensor errors
    
    # Yield remaining measurements
    if measurements:
        yield archive_index, month_str, measurements, location_count, len(locations)


def write_measurements_batch(output_path: Path, measurements: list, append: bool = True):
    """Write measurements to CSV file"""
    mode = 'a' if append else 'w'
    with open(output_path, mode) as f:
        if not append:
            f.write("timestamp,location_id,location_name,latitude,longitude,parameter,value,unit\n")
        
        for m in measurements:
            f.write(f"{m['timestamp']},{m['location_id']},{m['location_name']},"
                   f"{m['latitude']},{m['longitude']},{m['parameter']},"
                   f"{m['value']},{m['unit']}\n")


async def process_archives_parallel(archives: list, start_date: datetime, end_date: datetime, 
                                  max_workers: int, max_locations: int = None):
    """Process multiple archives in parallel"""
    
    # Get locations first
    storage = DataStorage()
    datasource = JARTICDataSource(
        api_keys=[],
        cache_dir=Path("data/jartic/cache"),
        cleanup_after_parse=False
    )
    
    print("📍 Finding locations...")
    async with datasource:
        locations = await datasource.find_locations(country_code="JP")
        if max_locations:
            locations = locations[:max_locations]
    print(f"   Found {len(locations)} locations")
    print()
    
    # Prepare output file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"jp_traffic_{timestamp}.csv"
    output_path = storage.get_processed_dir('jartic') / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write CSV header
    write_measurements_batch(output_path, [], append=False)
    
    print(f"🚗 JARTIC Archive Processor (Parallel Mode)")
    print("="*60)
    print(f"Archives to process: {len(archives)}")
    print(f"Parallel workers: {max_workers}")
    print(f"Locations: {len(locations)}")
    print(f"Output: {output_path}")
    print()
    
    # Progress tracking
    overall_progress = tqdm(
        total=len(archives),
        desc="📊 Overall Progress",
        unit="archive",
        position=0,
        bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
    )
    
    measurement_count = tqdm(
        desc="📈 Measurements",
        unit=" total",
        position=1,
        bar_format='{desc}: {n_fmt}{unit}'
    )
    
    current_archive = tqdm(
        desc="🔄 Processing",
        position=2,
        bar_format='{desc}: {postfix}'
    )
    
    total_measurements = 0
    processed_archives = 0
    
    # Process each archive
    for i, archive_path in enumerate(archives):
        try:
            month_str = archive_path.stem.replace('jartic_typeB_', '').replace('_', '-')
            current_archive.set_postfix_str(f"{month_str}")
            
            archive_measurements = 0
            async for archive_index, month_str, measurements, loc_count, total_locs in process_single_archive(
                archive_path, locations, start_date, end_date, 
                output_path, i, len(archives)
            ):
                # Update progress
                current_archive.set_postfix_str(f"{month_str} - Location {loc_count}/{total_locs}")
                
                # Write measurements to file
                if measurements:
                    write_measurements_batch(output_path, measurements)
                    archive_measurements += len(measurements)
                    total_measurements += len(measurements)
                    measurement_count.n = total_measurements
                    measurement_count.refresh()
            
            processed_archives += 1
            overall_progress.update(1)
            
            tqdm.write(f"✅ Processed {month_str}: {archive_measurements:,} measurements")
            
        except Exception as e:
            tqdm.write(f"❌ Failed to process archive: {str(e)}")
            overall_progress.update(1)
    
    # Close progress bars
    overall_progress.close()
    measurement_count.close()
    current_archive.close()
    
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print(f"Archives processed: {processed_archives}/{len(archives)}")
    print(f"Total measurements: {total_measurements:,}")
    print(f"Output file: {output_path}")
    if output_path.exists():
        print(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"{'='*60}")
    
    return output_path, total_measurements


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Process JARTIC traffic archives in parallel',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This script processes already downloaded JARTIC archives from the cache directory.
First use download_jartic_archives.py to download the archives, then use this script to process them.

Examples:
  %(prog)s --start 2024-01-01 --end 2024-12-31
  %(prog)s --start 2024-01-01 --end 2024-03-31 --workers 8 --max-locations 100
  %(prog)s --cache-dir /custom/cache/path --start 2024-01-01 --end 2024-01-31 --analyze
        """
    )
    
    parser.add_argument('--start', '-s', type=parse_date, required=True,
                       help='Start date for filtering data (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=parse_date, required=True,
                       help='End date for filtering data (YYYY-MM-DD)')
    parser.add_argument('--cache-dir', type=Path, default=Path("data/jartic/cache"),
                       help='Cache directory with archives (default: data/jartic/cache)')
    parser.add_argument('--workers', '-w', type=int, default=4,
                       help='Number of parallel workers (default: 4)')
    parser.add_argument('--max-locations', type=int,
                       help='Maximum number of locations to process')
    parser.add_argument('--analyze', '-a', action='store_true',
                       help='Analyze data after processing')
    
    args = parser.parse_args()
    
    # Find archives in cache directory
    if not args.cache_dir.exists():
        parser.error(f"Cache directory does not exist: {args.cache_dir}")
    
    archives = sorted(args.cache_dir.glob("jartic_typeB_*.zip"))
    if not archives:
        parser.error(f"No JARTIC archives found in {args.cache_dir}")
    
    # Filter archives based on date range
    filtered_archives = []
    for archive in archives:
        # Extract year and month from filename
        parts = archive.stem.split('_')
        if len(parts) >= 4:
            try:
                year = int(parts[2])
                month = int(parts[3])
                archive_date = datetime(year, month, 1, tzinfo=timezone.utc)
                
                # Check if archive month overlaps with requested date range
                if archive_date.year == args.start.year and archive_date.month >= args.start.month:
                    filtered_archives.append(archive)
                elif archive_date.year == args.end.year and archive_date.month <= args.end.month:
                    filtered_archives.append(archive)
                elif args.start.year < archive_date.year < args.end.year:
                    filtered_archives.append(archive)
            except (ValueError, IndexError):
                continue
    
    if not filtered_archives:
        parser.error(f"No archives found for date range {args.start.date()} to {args.end.date()}")
    
    print(f"Found {len(filtered_archives)} archives to process")
    
    # Process archives
    output_path, total_measurements = asyncio.run(
        process_archives_parallel(
            filtered_archives, 
            args.start, 
            args.end,
            args.workers,
            args.max_locations
        )
    )
    
    # Analyze if requested
    if args.analyze and total_measurements > 0:
        print("\nANALYZING DATA...")
        analyze_dataset(str(output_path))


if __name__ == "__main__":
    main()