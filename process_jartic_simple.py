#!/usr/bin/env python3
import argparse
import asyncio
from datetime import datetime, timezone
from pathlib import Path
import zipfile
import csv
from tqdm import tqdm

from src.core.data_storage import DataStorage




def main():
    parser = argparse.ArgumentParser(
        description='Simple JARTIC processor - extracts all traffic data from archives'
    )
    
    parser.add_argument('--start', '-s', required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--cache-dir', type=Path, default=Path("data/jartic/cache"),
                       help='Cache directory with archives')
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    # Find archives
    archives = sorted(args.cache_dir.glob("jartic_typeB_*.zip"))
    if not archives:
        print(f"❌ No archives found in {args.cache_dir}")
        return
    
    print(f"\n🚗 Simple JARTIC Processor")
    print("="*60)
    print(f"Archives found: {len(archives)}")
    print(f"Date range: {args.start} to {args.end}")
    
    # Prepare output - include date range in filename
    storage = DataStorage()
    date_range = f"{start_date.strftime('%Y%m')}-{end_date.strftime('%Y%m')}"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = storage.get_processed_dir('jartic') / f"jp_traffic_{date_range}_{timestamp}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write header
    with open(output_path, 'w') as f:
        f.write("timestamp,location_id,location_name,latitude,longitude,parameter,value,unit,prefecture\n")
    
    # Estimate total measurements (rough estimate: ~100M per archive for full month)
    estimated_total = len(archives) * 100_000_000
    
    # Create custom progress bar format
    bar_format = '{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}] {postfix}'
    
    # Process each archive
    total_measurements = 0
    with tqdm(total=estimated_total, desc="Processing", unit="", bar_format=bar_format) as pbar:
        for archive in archives:
            # Check if archive is in date range
            parts = archive.stem.split('_')
            if len(parts) >= 4:
                year = int(parts[2])
                month = int(parts[3])
                archive_date = datetime(year, month, 1, tzinfo=timezone.utc)
                
                # Skip if outside date range
                if archive_date.year < start_date.year or \
                   (archive_date.year == start_date.year and archive_date.month < start_date.month):
                    continue
                if archive_date.year > end_date.year or \
                   (archive_date.year == end_date.year and archive_date.month > end_date.month):
                    continue
            
            # Update description
            pbar.set_description(f"Processing {archive.name}")
            
            # Create a sub-progress tracking function
            def update_progress():
                nonlocal total_measurements
                total_measurements += 1
                pbar.update(1)
                
                # Update file size every 10k measurements
                if total_measurements % 10000 == 0:
                    if output_path.exists():
                        size_mb = output_path.stat().st_size / (1024 * 1024)
                        pbar.set_postfix_str(f"Output: {size_mb:.1f} MB")
            
            # Process with custom progress updater
            measurements_in_archive = 0
            
            with zipfile.ZipFile(archive, 'r') as main_zip:
                prefecture_files = [f for f in main_zip.namelist() if f.endswith('.zip') and not f.startswith('__MACOSX')]
                
                with open(output_path, 'a', encoding='utf-8') as out_file:
                    for pref_file in prefecture_files:
                        try:
                            prefecture = pref_file.replace('.zip', '').split('/')[-1]
                            pref_data = main_zip.read(pref_file)
                            
                            import io
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
                                            
                                            measurements_in_archive += 1
                                            update_progress()
                                                
                                        except Exception:
                                            continue
                                            
                        except Exception:
                            continue
        
        # Update total to actual count
        pbar.total = total_measurements
        pbar.refresh()
    
    print(f"\n{'='*60}")
    print("PROCESSING COMPLETE")
    print(f"Total measurements: {total_measurements:,}")
    print(f"Output file: {output_path}")
    if output_path.exists():
        print(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()