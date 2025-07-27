#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import threading
from collections import deque
import time

from src.plugins.jartic.datasource import JARTICDataSource
from src.core.data_storage import DataStorage
from src.utils.data_analyzer import analyze_dataset


class ParallelJARTICDownloader:
    def __init__(self, datasource, storage, max_parallel_downloads=2):
        self.datasource = datasource
        self.storage = storage
        self.max_parallel_downloads = max_parallel_downloads
        self.download_queue = deque()
        self.processing_queue = deque()
        self.completed_months = []
        self.total_months = 0
        self.total_measurements = 0
        self.lock = threading.Lock()
        
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
                
                with self.lock:
                    self.processing_queue.append({
                        'archive_info': archive_info,
                        'archive_path': archive_path
                    })
                progress_bars['download'].update(1)
                    
            except Exception as e:
                progress_bars['status'].write(f"❌ Failed to download {month_str}: {e}")
    
    async def process_measurements(self, archive_data, locations, start_date, end_date, 
                                   output_file, progress_bars):
        """Process measurements from a single archive"""
        archive_info = archive_data['archive_info']
        archive_path = archive_data['archive_path']
        month_str = f"{archive_info['year']}-{archive_info['month']:02d}"
        
        progress_bars['process'].set_description(f"🔄 Processing {month_str}")
        
        month_measurements = 0
        
        # Create location progress bar for this month
        location_bar = tqdm(
            total=len(locations),
            desc=f"   📍 {month_str} Locations",
            unit="loc",
            position=5,
            leave=False
        )
        
        for loc_idx, location in enumerate(locations):
            location_bar.set_postfix_str(f"Current: {location.name[:30]}")
            sensors = await self.datasource.get_sensors(location)
            
            for sensor in sensors:
                sensor_measurements = 0
                # Need to parse measurements from specific archive
                async for measurement in self.datasource.parser.parse_measurements(
                    archive_path,
                    sensor,
                    start_date,
                    end_date
                ):
                    output_file.write(f"{measurement.timestamp.isoformat()},"
                                     f"{location.id},"
                                     f"{location.name},"
                                     f"{location.coordinates.latitude},"
                                     f"{location.coordinates.longitude},"
                                     f"{sensor.parameter.value},"
                                     f"{measurement.value},"
                                     f"{sensor.unit.value}\n")
                    
                    sensor_measurements += 1
                    month_measurements += 1
                    self.total_measurements += 1
                    
                    if sensor_measurements % 1000 == 0:
                        progress_bars['measurements'].set_description(
                            f"📈 {month_str} - {location.name[:20]} - {sensor.parameter.value}"
                        )
                        progress_bars['measurements'].set_postfix({
                            'total': f"{self.total_measurements:,}",
                            'month': f"{month_measurements:,}",
                            'sensor': f"{sensor_measurements:,}"
                        })
                    
                    if month_measurements % 10000 == 0:
                        output_file.flush()
            
            location_bar.update(1)
        
        location_bar.close()
        
        with self.lock:
            self.completed_months.append(month_str)
            progress_bars['process'].update(1)
            progress_bars['status'].write(
                f"✅ Completed {month_str}: {month_measurements:,} measurements"
            )
    
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
                'month': current.month
            })
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)
        
        self.total_months = len(months_needed)
        self.download_queue.extend(months_needed)
        
        print(f"\n🚗 JARTIC Traffic Data Download (Parallel Mode)")
        print("="*60)
        print(f"Period: {args.start.strftime('%Y-%m-%d')} to {args.end.strftime('%Y-%m-%d')}")
        print(f"Archives needed: {self.total_months}")
        print(f"Parallel downloads: {self.max_parallel_downloads}")
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
                           bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'),
            'download': tqdm(total=self.total_months, desc="📥 Downloads", 
                            unit="archive", position=1),
            'process': tqdm(total=self.total_months, desc="🔄 Processing", 
                           unit="month", position=2),
            'measurements': tqdm(desc="📈 Measurements", unit=" meas", position=3,
                               bar_format='{desc}: {postfix}'),
            'status': tqdm(bar_format='{desc}', position=4)
        }
        
        # Start processing
        with open(output_path, 'w') as output_file:
            # Write header
            output_file.write("timestamp,location_id,location_name,latitude,longitude,"
                            "parameter,value,unit\n")
            
            # Start download workers
            download_tasks = []
            for _ in range(self.max_parallel_downloads):
                task = asyncio.create_task(self.download_worker(progress_bars))
                download_tasks.append(task)
            
            # Process archives as they complete downloading
            while len(self.completed_months) < self.total_months:
                if self.processing_queue:
                    archive_data = self.processing_queue.popleft()
                    await self.process_measurements(
                        archive_data, locations, args.start, args.end,
                        output_file, progress_bars
                    )
                    progress_bars['overall'].update(1)
                else:
                    # Wait for downloads to complete
                    await asyncio.sleep(1)
            
            # Wait for all downloads to complete
            await asyncio.gather(*download_tasks)
        
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
        if args.parallel:
            downloader = ParallelJARTICDownloader(
                datasource, storage, 
                max_parallel_downloads=args.parallel_downloads
            )
            await downloader.download_jartic_data_parallel(args)
        else:
            # Fall back to original sequential implementation
            print("Use --parallel flag for parallel downloads")


def parse_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD")


def main():
    parser = argparse.ArgumentParser(
        description='Download JARTIC traffic data for Japan (Parallel Version)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --start 2024-01-01 --end 2024-12-31 --parallel
  %(prog)s --start 2024-01-01 --end 2024-01-31 --parallel --parallel-downloads 3
  %(prog)s --start 2024-01-01 --end 2024-12-31 --parallel --keep-cache
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
    parser.add_argument('--parallel', action='store_true',
                       help='Enable parallel download and processing')
    parser.add_argument('--parallel-downloads', type=int, default=2,
                       help='Number of parallel downloads (default: 2)')
    
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