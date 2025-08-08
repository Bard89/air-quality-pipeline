#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
import pandas as pd
from typing import Optional, List

sys.path.append(str(Path(__file__).parent.parent))

from scripts.processors import (
    OpenAQProcessor,
    OpenMeteoProcessor,
    NASAPowerProcessor,
    ERA5Processor,
    FIRMSProcessor,
    JARTICProcessor,
    TerrainProcessor
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROCESSOR_CLASSES = {
    'openaq': OpenAQProcessor,
    'openmeteo': OpenMeteoProcessor,
    'nasapower': NASAPowerProcessor,
    'era5': ERA5Processor,
    'firms': FIRMSProcessor,
    'jartic': JARTICProcessor,
    'terrain': TerrainProcessor
}

def process_source(
    source: str,
    country: str,
    start_date: datetime,
    end_date: datetime,
    output_dir: Optional[Path] = None
) -> Optional[Path]:
    if source not in PROCESSOR_CLASSES:
        logger.error(f"Unknown source: {source}")
        logger.info(f"Available sources: {', '.join(PROCESSOR_CLASSES.keys())}")
        return None
    
    processor_class = PROCESSOR_CLASSES[source]
    processor = processor_class(country=country)
    
    logger.info(f"{'='*60}")
    logger.info(f"Processing {source.upper()} data for {country}")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"{'='*60}")
    
    if output_dir is None:
        output_dir = Path('data') / 'processed'
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_filename = (f"{country.lower()}_{source}_processed_"
                      f"{start_date.strftime('%Y%m%d')}_to_"
                      f"{end_date.strftime('%Y%m%d')}.csv")
    output_path = output_dir / output_filename
    
    try:
        df = processor.process_date_range(start_date, end_date, output_path)
        
        if df.empty:
            logger.warning(f"No data processed for {source}")
            return None
        
        logger.info(f"Successfully processed {len(df)} records")
        logger.info(f"Output saved to: {output_path}")
        
        logger.info("\nData Summary:")
        logger.info(f"- Records: {len(df):,}")
        if 'timestamp' in df.columns:
            logger.info(f"- Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        if f'h3_index_res8' in df.columns:
            logger.info(f"- Unique hexagons: {df['h3_index_res8'].nunique():,}")
        
        numeric_cols = df.select_dtypes(include='number').columns
        if len(numeric_cols) > 0:
            logger.info("\nNumeric columns statistics:")
            for col in numeric_cols[:5]:
                if not col.startswith('h3_'):
                    logger.info(f"  {col}: mean={df[col].mean():.2f}, "
                              f"std={df[col].std():.2f}, "
                              f"nulls={df[col].isna().sum()}")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error processing {source}: {e}")
        import traceback
        traceback.print_exc()
        return None

def process_all_sources(
    country: str,
    start_date: datetime,
    end_date: datetime,
    sources: Optional[List[str]] = None,
    output_dir: Optional[Path] = None
) -> List[Path]:
    if sources is None:
        sources = list(PROCESSOR_CLASSES.keys())
    
    logger.info(f"\n{'='*80}")
    logger.info(f"PROCESSING ALL SOURCES FOR {country}")
    logger.info(f"Sources: {', '.join(sources)}")
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"{'='*80}\n")
    
    processed_files = []
    
    for source in sources:
        try:
            output_path = process_source(source, country, start_date, end_date, output_dir)
            if output_path:
                processed_files.append(output_path)
        except Exception as e:
            logger.error(f"Failed to process {source}: {e}")
            continue
    
    logger.info(f"\n{'='*80}")
    logger.info(f"PROCESSING COMPLETE")
    logger.info(f"Successfully processed {len(processed_files)}/{len(sources)} sources")
    logger.info(f"Output files:")
    for file_path in processed_files:
        logger.info(f"  - {file_path}")
    logger.info(f"{'='*80}\n")
    
    return processed_files

def main():
    parser = argparse.ArgumentParser(
        description='Process environmental data from various sources',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all sources for Japan for January 2023
  %(prog)s --country JP --start 2023-01-01 --end 2023-01-31
  
  # Process only OpenAQ and OpenMeteo data
  %(prog)s --country JP --start 2023-01-01 --end 2023-01-31 --sources openaq openmeteo
  
  # Process single source
  %(prog)s --country JP --start 2023-01-01 --end 2023-01-31 --sources openaq
        """
    )
    
    parser.add_argument('--country', '-c', type=str, required=True,
                       help='Country code (e.g., JP, IN, KR)')
    parser.add_argument('--start', '-s', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', '-e', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--sources', nargs='+', 
                       choices=list(PROCESSOR_CLASSES.keys()) + ['all'],
                       default=['all'],
                       help='Data sources to process')
    parser.add_argument('--output-dir', '-o', type=str,
                       help='Output directory for processed files')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d')
    
    if start_date > end_date:
        parser.error("Start date must be before end date")
    
    sources = args.sources
    if 'all' in sources:
        sources = list(PROCESSOR_CLASSES.keys())
    
    output_dir = Path(args.output_dir) if args.output_dir else None
    
    processed_files = process_all_sources(
        country=args.country.upper(),
        start_date=start_date,
        end_date=end_date,
        sources=sources,
        output_dir=output_dir
    )
    
    if not processed_files:
        logger.error("No files were successfully processed")
        sys.exit(1)
    
    logger.info(f"Processing complete. {len(processed_files)} files created.")

if __name__ == "__main__":
    main()