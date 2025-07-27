#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
import csv
import io
from tqdm import tqdm

from src.core.data_storage import DataStorage


def process_csv_file(csv_path: Path, start_date: datetime, end_date: datetime, output_file):
    """Process a single CSV file and write matching records to output"""
    
    count = 0
    prefecture = csv_path.stem.split('_')[0]  # Get prefecture from filename
    
    # Try UTF-8 first (if files were converted), fallback to Shift-JIS
    encodings = ['utf-8', 'shift_jis']
    
    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                content = f.read()
                break
        except UnicodeDecodeError:
            continue
    else:
        # If all encodings fail, use shift_jis with errors='ignore'
        with open(csv_path, 'r', encoding='shift_jis', errors='ignore') as f:
            content = f.read()
    
    # Process the content
    with io.StringIO(content) as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        for row in reader:
            if len(row) < 10:
                continue
            
            try:
                # Parse timestamp
                time_str = row[0]
                timestamp = datetime.strptime(time_str, '%Y/%m/%d %H:%M').replace(tzinfo=timezone.utc)
                
                # Check date range
                if not (start_date <= timestamp <= end_date):
                    continue
                
                # Extract fields
                location_code = row[2]
                location_name = row[3]
                traffic_volume = row[7]
                
                # Skip empty values
                if not traffic_volume or traffic_volume == '-':
                    continue
                
                # Write to output
                output_file.write(
                    f"{timestamp.isoformat()},JARTIC_{location_code},{location_name},"
                    f"0,0,traffic_volume,{traffic_volume},vehicles/5min,{prefecture}\n"
                )
                count += 1
                
            except (ValueError, IndexError):
                continue
    
    return count


def main():
    parser = argparse.ArgumentParser(
        description='Process extracted JARTIC CSV files'
    )
    
    parser.add_argument('--start', '-s', required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--input-dir', '-i', type=Path, default=Path("data/jartic/extracted"),
                       help='Directory with extracted CSV files')
    parser.add_argument('--month', '-m',
                       help='Specific month directory to process (e.g., 2024_02)')
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    # Find CSV files to process
    if args.month:
        month_dir = args.input_dir / args.month
        if not month_dir.exists():
            print(f"❌ Month directory not found: {month_dir}")
            return
        csv_files = list(month_dir.glob('*.csv'))
    else:
        # Find all month directories in date range
        csv_files = []
        for month_dir in sorted(args.input_dir.iterdir()):
            if month_dir.is_dir() and month_dir.name.count('_') == 1:
                year, month = month_dir.name.split('_')
                try:
                    dir_date = datetime(int(year), int(month), 1, tzinfo=timezone.utc)
                    # Check if this month overlaps with our date range
                    # Create last day of the month for comparison
                    if dir_date.month == 12:
                        month_end = datetime(dir_date.year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
                    else:
                        month_end = datetime(dir_date.year, dir_date.month + 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
                    
                    # Check if month overlaps with date range
                    if not (month_end < start_date or dir_date > end_date):
                        csv_files.extend(month_dir.glob('*.csv'))
                except (ValueError, TypeError):
                    continue
    
    if not csv_files:
        print("❌ No CSV files found to process")
        return
    
    print(f"\n🚗 Processing Extracted JARTIC CSVs")
    print("="*60)
    print(f"CSV files to process: {len(csv_files)}")
    print(f"Date range: {args.start} to {args.end}")
    
    # Prepare output
    storage = DataStorage()
    date_range = f"{start_date.strftime('%Y%m')}-{end_date.strftime('%Y%m')}"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = storage.get_processed_dir('jartic') / f"jp_traffic_{date_range}_{timestamp}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Process files
    total_measurements = 0
    
    with open(output_path, 'w') as out_file:
        # Write header
        out_file.write("timestamp,location_id,location_name,latitude,longitude,parameter,value,unit,prefecture\n")
        
        # Process each CSV
        for csv_file in tqdm(csv_files, desc="Processing CSV files"):
            count = process_csv_file(csv_file, start_date, end_date, out_file)
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