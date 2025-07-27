#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import threading
import time

from src.plugins.jartic.datasource import JARTICDataSource
from src.core.data_storage import DataStorage
from src.utils.data_analyzer import analyze_dataset


class FastJARTICProcessor:
    def __init__(self, max_workers=8):
        self.max_workers = max_workers
        self.total_measurements = 0
        self.lock = threading.Lock()
        
    async def process_location_batch(self, locations_batch, archive_path, start_date, end_date):
        """Process a batch of locations"""
        measurements = []
        
        datasource = JARTICDataSource(
            api_keys=[],
            cache_dir=archive_path.parent,
            cleanup_after_parse=False
        )
        
        async with datasource:
            for location in locations_batch:
                sensors = await datasource.get_sensors(location)
                
                for sensor in sensors:
                    try:
                        async for measurement in datasource.parser.parse_measurements(
                            archive_path,
                            sensor,
                            start_date,
                            end_date
                        ):
                            measurements.append(
                                f"{measurement.timestamp.isoformat()},{location.id},{location.name},"
                                f"{location.coordinates.latitude},{location.coordinates.longitude},"
                                f"{sensor.parameter.value},{measurement.value},{sensor.unit.value}\n"
                            )
                            
                    except Exception:
                        pass  # Skip sensor errors
        
        return measurements
    
    async def process_archive_parallel(self, archive_path, locations, start_date, end_date, output_path):
        """Process archive with true parallel processing"""
        month_str = archive_path.stem.replace('jartic_typeB_', '').replace('_', '-')
        
        print(f"\n🔄 Processing {month_str} with {self.max_workers} parallel workers...")
        
        # Split locations into batches
        batch_size = max(1, len(locations) // self.max_workers)
        location_batches = [
            locations[i:i + batch_size] 
            for i in range(0, len(locations), batch_size)
        ]
        
        # Progress bar for batches
        batch_progress = tqdm(
            total=len(location_batches),
            desc=f"📦 Processing batches",
            unit="batch"
        )
        
        # Process batches in parallel
        tasks = []
        for batch in location_batches:
            task = self.process_location_batch(batch, archive_path, start_date, end_date)
            tasks.append(task)
        
        # Collect results as they complete
        with open(output_path, 'a') as f:
            for coro in asyncio.as_completed(tasks):
                measurements = await coro
                
                # Write batch to file
                for line in measurements:
                    f.write(line)
                
                # Update counters
                with self.lock:
                    self.total_measurements += len(measurements)
                
                batch_progress.update(1)
                
                # Flush periodically
                if self.total_measurements % 100000 == 0:
                    f.flush()
        
        batch_progress.close()
        print(f"✅ Completed {month_str}: {self.total_measurements:,} measurements")


async def main(args):
    """Main function"""
    storage = DataStorage()
    
    # Find archives
    if not args.cache_dir.exists():
        print(f"❌ Cache directory does not exist: {args.cache_dir}")
        sys.exit(1)
    
    archives = sorted(args.cache_dir.glob("jartic_typeB_*.zip"))
    if not archives:
        print(f"❌ No JARTIC archives found in {args.cache_dir}")
        sys.exit(1)
    
    # Filter archives based on date range
    filtered_archives = []
    for archive in archives:
        parts = archive.stem.split('_')
        if len(parts) >= 4:
            try:
                year = int(parts[2])
                month = int(parts[3])
                archive_date = datetime(year, month, 1, tzinfo=timezone.utc)
                
                if (archive_date.year == args.start.year and archive_date.month >= args.start.month) or \
                   (archive_date.year == args.end.year and archive_date.month <= args.end.month) or \
                   (args.start.year < archive_date.year < args.end.year):
                    filtered_archives.append(archive)
            except:
                continue
    
    if not filtered_archives:
        print(f"❌ No archives found for date range")
        sys.exit(1)
    
    print(f"\n🚗 JARTIC Fast Parallel Processor")
    print("="*60)
    print(f"Archives to process: {len(filtered_archives)}")
    print(f"Parallel workers: {args.workers}")
    
    # Get locations
    datasource = JARTICDataSource(
        api_keys=[],
        cache_dir=args.cache_dir,
        cleanup_after_parse=False
    )
    
    print("\n📍 Finding locations...")
    async with datasource:
        locations = await datasource.find_locations(country_code="JP")
        if args.max_locations:
            locations = locations[:args.max_locations]
    print(f"   Found {len(locations)} locations")
    
    # Prepare output
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"jp_traffic_fast_{timestamp}.csv"
    output_path = storage.get_processed_dir('jartic') / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write header
    with open(output_path, 'w') as f:
        f.write("timestamp,location_id,location_name,latitude,longitude,parameter,value,unit\n")
    
    # Process archives
    processor = FastJARTICProcessor(max_workers=args.workers)
    
    for archive in filtered_archives:
        await processor.process_archive_parallel(
            archive, locations, args.start, args.end, output_path
        )
    
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print(f"Total measurements: {processor.total_measurements:,}")
    print(f"Output file: {output_path}")
    if output_path.exists():
        print(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"{'='*60}")
    
    if args.analyze and processor.total_measurements > 0:
        print("\nANALYZING DATA...")
        analyze_dataset(str(output_path))


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fast parallel JARTIC traffic archive processor',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--start', '-s', type=parse_date, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=parse_date, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--cache-dir', type=Path, default=Path("data/jartic/cache"),
                       help='Cache directory (default: data/jartic/cache)')
    parser.add_argument('--workers', '-w', type=int, default=8,
                       help='Number of parallel workers (default: 8)')
    parser.add_argument('--max-locations', type=int,
                       help='Maximum number of locations')
    parser.add_argument('--analyze', '-a', action='store_true',
                       help='Analyze data after processing')
    
    args = parser.parse_args()
    asyncio.run(main(args))