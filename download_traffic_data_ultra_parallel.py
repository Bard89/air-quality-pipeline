#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
from collections import deque
import time
import multiprocessing as mp
from typing import List, Dict, Any

from src.plugins.jartic.datasource import JARTICDataSource
from src.core.data_storage import DataStorage
from src.utils.data_analyzer import analyze_dataset


class UltraParallelJARTICDownloader:
    def __init__(self, datasource, storage, max_parallel_downloads=2, max_parallel_processing=4):
        self.datasource = datasource
        self.storage = storage
        self.max_parallel_downloads = max_parallel_downloads
        self.max_parallel_processing = max_parallel_processing
        self.download_queue = deque()
        self.processing_queue = asyncio.Queue()
        self.completed_months = []
        self.total_months = 0
        self.total_measurements = 0
        self.lock = threading.Lock()
        self.measurement_lock = threading.Lock()
        
    async def download_worker(self, progress_bars):
        """Worker that downloads archives"""
        while True:
            with self.lock:
                if not self.download_queue:
                    break
                archive_info = self.download_queue.popleft()
                
            month_str = f"{archive_info['year']}-{archive_info['month']:02d}"
            
            # Update download progress bar
            progress_bars['download'].set_description(f"📥 Downloading {month_str}")
            
            try:
                # Check if already downloaded
                existing_path = self.datasource.cache_dir / f"jartic_typeB_{archive_info['year']}_{archive_info['month']:02d}.zip"
                if existing_path.exists():
                    progress_bars['status'].write(f"✓ Using cached {month_str}")
                    archive_path = existing_path
                else:
                    archive_path = await self.datasource.downloader.download_archive(
                        archive_info['year'],
                        archive_info['month']
                    )
                
                await self.processing_queue.put({
                    'archive_info': archive_info,
                    'archive_path': archive_path
                })
                progress_bars['download'].update(1)
                    
            except Exception as e:
                progress_bars['status'].write(f"❌ Failed to download {month_str}: {e}")
    
    async def process_location_batch(self, locations_batch, archive_path, sensor_params, 
                                   start_date, end_date, month_str):
        """Process a batch of locations in parallel"""
        measurements = []
        
        for location in locations_batch:
            sensors = await self.datasource.get_sensors(location)
            
            for sensor in sensors:
                if sensor.parameter.value not in sensor_params:
                    continue
                    
                try:
                    async for measurement in self.datasource.parser.parse_measurements(
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
                        
                        if len(measurements) >= 10000:
                            yield measurements
                            measurements = []
                            
                except Exception as e:
                    pass  # Skip errors for individual sensors
        
        if measurements:
            yield measurements
    
    async def process_worker(self, locations, start_date, end_date, output_path, progress_bars):
        """Worker that processes archives in parallel"""
        with open(output_path, 'a') as output_file:
            while True:
                try:
                    archive_data = await asyncio.wait_for(
                        self.processing_queue.get(), 
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    # Check if downloads are done
                    with self.lock:
                        if len(self.completed_months) >= self.total_months:
                            break
                    continue
                
                archive_info = archive_data['archive_info']
                archive_path = archive_data['archive_path']
                month_str = f"{archive_info['year']}-{archive_info['month']:02d}"
                
                progress_bars['process'].set_description(f"🔄 Processing {month_str}")
                
                # Split locations into batches for parallel processing
                batch_size = max(1, len(locations) // self.max_parallel_processing)
                location_batches = [
                    locations[i:i + batch_size] 
                    for i in range(0, len(locations), batch_size)
                ]
                
                month_measurements = 0
                
                # Process location batches in parallel
                tasks = []
                for batch in location_batches:
                    task = self.process_location_batch(
                        batch, archive_path, 
                        ['traffic_volume', 'vehicle_speed', 'occupancy_rate'],  # Process all sensor types
                        start_date, end_date, month_str
                    )
                    tasks.append(task)
                
                # Gather results from all batches
                async for task in asyncio.as_completed([
                    asyncio.create_task(self._gather_batch_results(task))
                    for task in tasks
                ]):
                    batch_results = await task
                    for measurements in batch_results:
                        # Write measurements to file
                        for m in measurements:
                            output_file.write(
                                f"{m['timestamp']},{m['location_id']},{m['location_name']},"
                                f"{m['latitude']},{m['longitude']},{m['parameter']},"
                                f"{m['value']},{m['unit']}\n"
                            )
                        
                        month_measurements += len(measurements)
                        with self.measurement_lock:
                            self.total_measurements += len(measurements)
                        
                        # Update progress
                        progress_bars['measurements'].set_description(
                            f"📈 {month_str}"
                        )
                        progress_bars['measurements'].set_postfix({
                            'total': f"{self.total_measurements:,}",
                            'month': f"{month_measurements:,}",
                            'rate': f"{month_measurements // max(1, (time.time() - archive_data.get('start_time', time.time()))):,}/s"
                        })
                        
                        output_file.flush()
                
                with self.lock:
                    self.completed_months.append(month_str)
                    progress_bars['process'].update(1)
                    progress_bars['overall'].update(1)
                    progress_bars['status'].write(
                        f"✅ Completed {month_str}: {month_measurements:,} measurements"
                    )
    
    async def _gather_batch_results(self, batch_generator):
        """Helper to gather results from async generator"""
        results = []
        async for result in batch_generator:
            results.append(result)
        return results
    
    async def download_jartic_data_parallel(self, args):
        """Main parallel download function"""
        # Prepare output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"jp_traffic_{timestamp}.csv"
        output_path = self.storage.get_processed_dir('jartic') / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Calculate archives needed
        months_needed = []
        current = datetime(args.start.year, args.start.month, 1)
        end = datetime(args.end.year, args.end.month, 1)
        while current <= end:
            months_needed.append({
                'year': current.year,
                'month': current.month,
                'start_time': time.time()
            })
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
        
        self.total_months = len(months_needed)
        self.download_queue.extend(months_needed)
        
        print(f"\n🚗 JARTIC Traffic Data Download (Ultra Parallel Mode)")
        print("="*60)
        print(f"Period: {args.start.strftime('%Y-%m-%d')} to {args.end.strftime('%Y-%m-%d')}")
        print(f"Archives needed: {self.total_months}")
        print(f"Parallel downloads: {self.max_parallel_downloads}")
        print(f"Parallel processing: {self.max_parallel_processing} batches")
        print()
        
        # Get locations once
        print("📍 Finding locations...")
        locations = await self.datasource.find_locations(country_code="JP")
        if args.max_locations:
            locations = locations[:args.max_locations]
        print(f"   Found {len(locations)} locations")
        print()
        
        # Create progress bars
        progress_bars = {
            'overall': tqdm(total=self.total_months, desc="📊 Overall Progress", 
                           unit="month", position=0, 
                           bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'),
            'download': tqdm(total=self.total_months, desc="📥 Downloads", 
                            unit="archive", position=1),
            'process': tqdm(total=self.total_months, desc="🔄 Processing", 
                           unit="month", position=2),
            'measurements': tqdm(desc="📈 Measurements", unit=" meas", position=3,
                               bar_format='{desc}: {postfix}'),
            'status': tqdm(bar_format='{desc}', position=4)
        }
        
        # Write header
        with open(output_path, 'w') as f:
            f.write("timestamp,location_id,location_name,latitude,longitude,"
                   "parameter,value,unit\n")
        
        # Start workers
        download_tasks = []
        for _ in range(self.max_parallel_downloads):
            task = asyncio.create_task(self.download_worker(progress_bars))
            download_tasks.append(task)
        
        # Start processing workers
        process_tasks = []
        for _ in range(min(2, self.max_parallel_processing)):  # Limit file writers
            task = asyncio.create_task(
                self.process_worker(locations, args.start, args.end, output_path, progress_bars)
            )
            process_tasks.append(task)
        
        # Wait for completion
        await asyncio.gather(*download_tasks)
        await asyncio.gather(*process_tasks)
        
        # Close progress bars
        for pb in progress_bars.values():
            pb.close()
        
        print(f"\n{'='*60}")
        print("DOWNLOAD COMPLETE")
        print(f"Total measurements: {self.total_measurements:,}")
        print(f"Saved to: {output_path}")
        print(f"{'='*60}")
        
        if args.analyze and self.total_measurements > 0:
            print("\nANALYZING DATA...")
            analyze_dataset(str(output_path))


async def download_jartic_data(args):
    """Wrapper function for parallel download"""
    storage = DataStorage()
    
    # Create datasource
    datasource = JARTICDataSource(
        api_keys=[],
        cache_dir=Path("data/jartic/cache"),
        cleanup_after_parse=not args.keep_cache
    )
    
    async with datasource:
        downloader = UltraParallelJARTICDownloader(
            datasource, storage, 
            max_parallel_downloads=args.parallel_downloads,
            max_parallel_processing=args.parallel_processing
        )
        await downloader.download_jartic_data_parallel(args)


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Download JARTIC traffic data for Japan (Ultra Parallel Version)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --start 2024-01-01 --end 2024-12-31 --keep-cache
  %(prog)s --start 2024-01-01 --end 2024-01-31 --parallel-downloads 3 --parallel-processing 8
        """
    )
    
    # Download options
    parser.add_argument('--start', '-s', type=parse_date, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=parse_date, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--max-locations', type=int,
                       help='Maximum number of locations to download')
    
    # Parallel options
    parser.add_argument('--parallel-downloads', type=int, default=2,
                       help='Number of parallel downloads (default: 2)')
    parser.add_argument('--parallel-processing', type=int, default=4,
                       help='Number of parallel processing threads (default: 4)')
    
    # Other options
    parser.add_argument('--analyze', '-a', action='store_true', default=True,
                       help='Analyze data after download (default: True)')
    parser.add_argument('--keep-cache', action='store_true',
                       help='Keep downloaded archive files in cache')
    
    args = parser.parse_args()
    
    # Run async download
    asyncio.run(download_jartic_data(args))


if __name__ == "__main__":
    main()