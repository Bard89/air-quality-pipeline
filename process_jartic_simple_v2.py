#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import zipfile
import csv
from tqdm import tqdm
import io

from src.core.data_storage import DataStorage


def count_total_measurements(archives: list, start_date: datetime, end_date: datetime) -> int:
    """Count total measurements to process across all archives"""
    print("Counting total measurements...")
    total_count = 0
    
    for archive in archives:
        with zipfile.ZipFile(archive, 'r') as main_zip:
            prefecture_files = [f for f in main_zip.namelist() if f.endswith('.zip') and not f.startswith('__MACOSX')]
            
            for pref_file in prefecture_files:
                try:
                    pref_data = main_zip.read(pref_file)
                    with zipfile.ZipFile(io.BytesIO(pref_data), 'r') as pref_zip:
                        csv_files = [f for f in pref_zip.namelist() if f.endswith('.csv')]
                        
                        for csv_file in csv_files:
                            csv_data = pref_zip.read(csv_file).decode('shift_jis', errors='ignore')
                            lines = csv_data.splitlines()
                            
                            # Quick count of valid lines in date range
                            for line in lines[1:]:  # Skip header
                                if ',' in line and len(line.split(',')) >= 10:
                                    # Quick date check on first column
                                    try:
                                        time_str = line.split(',')[0]
                                        if '2024/01' in time_str:  # Quick filter for January 2024
                                            total_count += 1
                                    except:
                                        continue
                except:
                    continue
    
    print(f"Total measurements to process: {total_count:,}")
    return total_count


def process_jartic_archive_with_total(archive_path: Path, output_path: Path, start_date: datetime, 
                                      end_date: datetime, progress_bar: tqdm):
    """Process archive with accurate progress tracking"""
    
    measurements_count = 0
    
    with zipfile.ZipFile(archive_path, 'r') as main_zip:
        prefecture_files = [f for f in main_zip.namelist() if f.endswith('.zip') and not f.startswith('__MACOSX')]
        
        with open(output_path, 'a', encoding='utf-8') as out_file:
            for pref_file in prefecture_files:
                try:
                    prefecture = pref_file.replace('.zip', '').split('/')[-1]
                    pref_data = main_zip.read(pref_file)
                    
                    with zipfile.ZipFile(io.BytesIO(pref_data), 'r') as pref_zip:
                        csv_files = [f for f in pref_zip.namelist() if f.endswith('.csv')]
                        
                        for csv_file in csv_files:
                            csv_data = pref_zip.read(csv_file).decode('shift_jis', errors='ignore')
                            
                            for line_idx, line in enumerate(csv_data.splitlines()):
                                if line_idx == 0:
                                    continue
                                    
                                cols = line.split(',')
                                if len(cols) < 10:
                                    continue
                                
                                try:
                                    time_str = cols[0]
                                    timestamp = datetime.strptime(time_str, '%Y/%m/%d %H:%M').replace(tzinfo=timezone.utc)
                                    
                                    if not (start_date <= timestamp <= end_date):
                                        continue
                                    
                                    location_code = cols[2]
                                    location_name = cols[3]
                                    traffic_volume = cols[7]
                                    
                                    if not traffic_volume or traffic_volume == '-':
                                        continue
                                    
                                    out_file.write(
                                        f"{timestamp.isoformat()},JARTIC_{location_code},{location_name},"
                                        f"0,0,traffic_volume,{traffic_volume},vehicles/5min,{prefecture}\n"
                                    )
                                    
                                    measurements_count += 1
                                    progress_bar.update(1)
                                    
                                    # Update file size every 100k measurements
                                    if measurements_count % 100000 == 0:
                                        if output_path.exists():
                                            size_mb = output_path.stat().st_size / (1024 * 1024)
                                            progress_bar.set_postfix_str(f"Output: {size_mb:.1f} MB")
                                        
                                except Exception:
                                    continue
                                    
                except Exception:
                    continue
    
    return measurements_count


def main():
    parser = argparse.ArgumentParser(
        description='JARTIC processor with accurate progress tracking'
    )
    
    parser.add_argument('--start', '-s', required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--cache-dir', type=Path, default=Path("data/jartic/cache"),
                       help='Cache directory with archives')
    parser.add_argument('--skip-count', action='store_true',
                       help='Skip counting phase (use if you know the total)')
    parser.add_argument('--estimated-total', type=int,
                       help='Estimated total if skipping count')
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    # Find archives
    archives = sorted(args.cache_dir.glob("jartic_typeB_*.zip"))
    if not archives:
        print(f"❌ No archives found in {args.cache_dir}")
        return
    
    # Filter archives by date
    filtered_archives = []
    for archive in archives:
        parts = archive.stem.split('_')
        if len(parts) >= 4:
            year = int(parts[2])
            month = int(parts[3])
            archive_date = datetime(year, month, 1, tzinfo=timezone.utc)
            
            if archive_date.year < start_date.year or \
               (archive_date.year == start_date.year and archive_date.month < start_date.month):
                continue
            if archive_date.year > end_date.year or \
               (archive_date.year == end_date.year and archive_date.month > end_date.month):
                continue
            
            filtered_archives.append(archive)
    
    print(f"\n🚗 JARTIC Processor with Accurate Progress")
    print("="*60)
    print(f"Archives found: {len(filtered_archives)}")
    print(f"Date range: {args.start} to {args.end}")
    
    # Count total measurements unless skipped
    if args.skip_count and args.estimated_total:
        total_to_process = args.estimated_total
        print(f"Using estimated total: {total_to_process:,}")
    else:
        total_to_process = count_total_measurements(filtered_archives, start_date, end_date)
    
    # Prepare output
    storage = DataStorage()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = storage.get_processed_dir('jartic') / f"jp_traffic_all_{timestamp}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write header
    with open(output_path, 'w') as f:
        f.write("timestamp,location_id,location_name,latitude,longitude,parameter,value,unit,prefecture\n")
    
    # Process with accurate progress bar
    bar_format = '{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}'
    
    with tqdm(total=total_to_process, desc="Processing", unit="", bar_format=bar_format) as pbar:
        total_measurements = 0
        
        for archive in filtered_archives:
            pbar.set_description(f"Processing {archive.name}")
            count = process_jartic_archive_with_total(archive, output_path, start_date, end_date, pbar)
            total_measurements += count
    
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print(f"Total measurements: {total_measurements:,}")
    print(f"Output file: {output_path}")
    if output_path.exists():
        print(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()