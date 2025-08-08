#!/usr/bin/env python3

import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
from typing import Optional

sys.path.append(str(Path(__file__).parent.parent))

from scripts.processors import JARTICProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def process_jartic_full(start_date: datetime, end_date: datetime, max_rows: Optional[int] = None, 
                        n_workers: Optional[int] = None, input_file: Optional[str] = None):
    processor = JARTICProcessor(country='JP', max_rows=max_rows, n_workers=n_workers, input_file=input_file)
    print(f"Using parallel processing with {processor.n_workers} workers")
    
    output_suffix = f'_sample_{max_rows//1000000}M' if max_rows else '_full'
    output_path = Path(f'data/processed/jp_jartic_processed_{start_date.strftime("%Y%m%d")}_to_{end_date.strftime("%Y%m%d")}{output_suffix}.csv')
    
    print(f"Processing JARTIC data for {start_date.date()} to {end_date.date()}")
    if input_file:
        print(f"Input file: {input_file}")
    if max_rows:
        print(f"Row limit: {max_rows:,} rows")
    else:
        print(f"Processing entire file (no row limit)")
    print(f"Output will be saved to: {output_path}")
    
    if not max_rows:
        print("\n⚠️  WARNING: Full processing will take 30-60 minutes and process 315M+ rows")
        response = input("Continue? (y/n): ")
        if response.lower() != 'y':
            print("Aborted")
            return
    
    try:
        df = processor.process_date_range(start_date, end_date, output_path)
        
        if not df.empty:
            print(f"\nSuccess! Processed {len(df):,} hexagon-hour records")
            print(f"\nSummary:")
            print(f"- Output file: {output_path}")
            print(f"- File size: {output_path.stat().st_size / (1024**2):.1f} MB")
            print(f"- Unique hexagons: {df['h3_index_res8'].nunique():,}")
            print(f"- Unique timestamps: {df['timestamp'].nunique()}")
            print(f"- Unique prefectures: {df['prefecture'].nunique()}")
            print(f"- Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        else:
            print("No data was processed")
            
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(
        description='Process JARTIC traffic data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process entire file (default)
  %(prog)s --start 2023-01-01 --end 2023-01-31
  
  # Process specific input file
  %(prog)s --start 2023-01-01 --end 2023-01-31 --input-file /path/to/jartic_traffic_2023_01.csv
  
  # Process sample of 50M rows
  %(prog)s --start 2023-01-01 --end 2023-01-31 --max-rows 50000000
  
  # Process with specific number of workers
  %(prog)s --start 2023-01-01 --end 2023-01-31 --workers 4
        """
    )
    
    parser.add_argument('--start', '-s', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--max-rows', '-m', type=int, default=None,
                       help='Maximum rows to process (default: no limit, process entire file)')
    parser.add_argument('--workers', '-w', type=int, default=None,
                       help='Number of parallel workers (default: auto-detect)')
    parser.add_argument('--input-file', '-i', type=str, default=None,
                       help='Specific input CSV file to process (overrides date-based selection)')
    
    args = parser.parse_args()
    
    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d')
    
    process_jartic_full(start_date, end_date, args.max_rows, 
                       n_workers=args.workers, input_file=args.input_file)

if __name__ == "__main__":
    main()